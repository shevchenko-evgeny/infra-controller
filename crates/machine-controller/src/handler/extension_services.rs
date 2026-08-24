/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Instance extension-service reconciliation and status handling.

use std::collections::{BTreeMap, HashMap, HashSet};

use carbide_uuid::extension_service::ExtensionServiceId;
use carbide_uuid::machine::MachineId;
use chrono::Utc;
use config_version::Versioned;
use db::extension_service as db_extension_service;
use eyre::eyre;
use itertools::Itertools;
use model::extension_service::{
    DPF_HELM_CHART_PLACEMENT_LABEL_VALUE, DpfHelmChartIdentity, ExtensionServiceType,
};
use model::instance::config::extension_services::InstanceExtensionServiceConfig;
use model::instance::snapshot::InstanceSnapshot;
use model::instance::status::SyncState;
use model::instance::status::extension_service::{
    ExtensionServiceDeploymentStatus, ExtensionServiceStatusObservation,
    InstanceExtensionServiceStatusObservation, InstanceExtensionServicesStatus,
};
use model::machine::ManagedHostStateSnapshot;
use sqlx::PgConnection;
use state_controller::state_handler::StateHandlerError;

use crate::dpf::DpfOperations;

/// Builds instance extension-service status from its two authoritative sources.
///
/// Kubernetes Pod services retain their agent-reported status. DPF Helm services
/// deliberately do not: DPF has no per-DPU, per-service workload observation.
/// Instead, after every relevant DPUDevice label patch has succeeded, this
/// reports the *placement contract* as `Running` or `Terminated`. It must not
/// be interpreted as Helm workload health.
pub(super) async fn get_extension_services_status(
    mh_snapshot: &ManagedHostStateSnapshot,
    instance: &InstanceSnapshot,
    db_pool: &sqlx::PgPool,
    dpf_sdk: Option<&dyn DpfOperations>,
) -> Result<InstanceExtensionServicesStatus, StateHandlerError> {
    let service_types = extension_service_types_for_instance(instance, db_pool).await?;
    let mut agent_config = instance.config.extension_services.clone();
    agent_config.service_configs.retain(|config| {
        service_types.get(&config.service_id) == Some(&ExtensionServiceType::KubernetesPod)
    });
    let dpf_service_configs = instance
        .config
        .extension_services
        .service_configs
        .iter()
        .filter(|config| {
            service_types.get(&config.service_id) == Some(&ExtensionServiceType::DpfHelmChart)
        })
        .collect_vec();

    let (_, device_to_id_map) = mh_snapshot
        .host_snapshot
        .get_dpu_device_and_id_mappings()
        .unwrap_or_else(|_| (HashMap::default(), HashMap::default()));

    let primary_dpu_machine_id = mh_snapshot.host_snapshot.primary_attached_dpu_machine_id();
    let used_dpus = instance
        .config
        .network
        .get_used_dpus(&device_to_id_map, primary_dpu_machine_id);

    let mut observations = instance.observations.extension_services.clone();

    if !dpf_service_configs.is_empty() {
        let dpf_sdk = match dpf_sdk {
            Some(dpf_sdk) => dpf_sdk,
            None => {
                // Do not leave a previous successful placement visible while
                // this controller cannot even attempt to verify or repair it.
                let target_dpu_ids = used_dpus.iter().copied().collect();
                let pending = persist_dpf_helm_chart_placement_observations(
                    mh_snapshot,
                    instance,
                    &dpf_service_configs,
                    &target_dpu_ids,
                    false,
                    ExtensionServiceDeploymentStatus::Pending,
                    db_pool,
                )
                .await?;
                for (machine_id, observation) in pending {
                    observations
                        .entry(machine_id)
                        .or_default()
                        .set_for_service_type(ExtensionServiceType::DpfHelmChart, observation);
                }
                return Err(StateHandlerError::GenericError(eyre!(
                    "DPF SDK is unavailable for DPF Helm chart placement reconciliation"
                )));
            }
        };
        let placement_observations = reconcile_dpf_helm_chart_placement(
            mh_snapshot,
            instance,
            &dpf_service_configs,
            false,
            dpf_sdk,
            db_pool,
        )
        .await?;
        for (machine_id, observation) in placement_observations {
            observations
                .entry(machine_id)
                .or_default()
                .set_for_service_type(ExtensionServiceType::DpfHelmChart, observation);
        }
    }

    let required_dpus: Vec<_> = instance
        .config
        .extension_services
        .service_configs
        .iter()
        .map(|config| {
            let dpu_ids = match service_types.get(&config.service_id) {
                Some(ExtensionServiceType::DpfHelmChart) if config.removed.is_some() => {
                    mh_snapshot.dpu_snapshots.iter().map(|dpu| dpu.id).collect()
                }
                _ => used_dpus.clone(),
            };
            (config.service_id, config.version, dpu_ids)
        })
        .collect();

    Ok(
        InstanceExtensionServicesStatus::from_config_and_service_type_observations(
            Versioned::new(
                &instance.config.extension_services,
                instance.extension_services_config_version,
            ),
            &service_types,
            &required_dpus,
            &observations,
        ),
    )
}

/// Looks up the persisted service type for every service referenced by an
/// instance configuration. Type is deliberately resolved from the database,
/// not from agent observations, so a DPF service can never accidentally enter
/// the legacy agent-status path.
pub(super) async fn extension_service_types_for_instance(
    instance: &InstanceSnapshot,
    db_pool: &sqlx::PgPool,
) -> Result<HashMap<ExtensionServiceId, ExtensionServiceType>, StateHandlerError> {
    let service_ids = instance
        .config
        .extension_services
        .service_configs
        .iter()
        .map(|config| config.service_id)
        .unique()
        .collect_vec();
    let services = {
        let mut connection = db_pool.acquire().await?;
        db_extension_service::find_by_ids(&mut connection, &service_ids, false, false).await?
    };
    let service_types: HashMap<_, _> = services
        .into_iter()
        .map(|service| (service.id, service.service_type))
        .collect();
    if service_ids
        .iter()
        .any(|service_id| !service_types.contains_key(service_id))
    {
        return Err(StateHandlerError::MissingData {
            object_id: instance.id.to_string(),
            missing: "extension service referenced by instance configuration",
        });
    }
    Ok(service_types)
}

/// Reconciles only NICo-owned DPF Helm placement labels for this instance.
///
/// Every physical DPU on the host receives a patch: currently targeted DPUs
/// get each active DPF service's generated label, while non-targeted DPUs and
/// services marked `removed` have that same label deleted.  Applying the
/// complete per-service delta to every physical DPU handles ordinary
/// attachment, detachment, and target-set changes without touching labels
/// owned by DPF or other controllers.
///
/// The caller resolves service types before entering this function, so no
/// database transaction is held across a DPF request. Readiness and deletion
/// callers retry an error; the Ready-state caller only logs it as drift.
pub(super) async fn reconcile_dpf_helm_chart_placement(
    mh_snapshot: &ManagedHostStateSnapshot,
    instance: &InstanceSnapshot,
    dpf_service_configs: &[&InstanceExtensionServiceConfig],
    force_detach: bool,
    dpf_sdk: &dyn DpfOperations,
    db_pool: &sqlx::PgPool,
) -> Result<HashMap<MachineId, InstanceExtensionServiceStatusObservation>, StateHandlerError> {
    if dpf_service_configs.is_empty() {
        return Ok(HashMap::new());
    }
    if !mh_snapshot.host_snapshot.config.dpf.used_for_ingestion {
        return Err(StateHandlerError::GenericError(eyre!(
            "a DPF Helm chart extension service is attached to a host that is not DPF-managed"
        )));
    }

    let (_, device_to_id_map) = mh_snapshot
        .host_snapshot
        .get_dpu_device_and_id_mappings()
        .map_err(|error| StateHandlerError::GenericError(eyre!(error)))?;
    let target_dpu_ids: HashSet<_> = if force_detach {
        HashSet::new()
    } else {
        // Every non-target DPU is explicitly patched as well, so target-set
        // changes cannot leave a stale placement label behind.
        instance
            .config
            .network
            .get_used_dpus(
                &device_to_id_map,
                mh_snapshot.host_snapshot.primary_attached_dpu_machine_id(),
            )
            .into_iter()
            .collect()
    };

    // Clear any previous success before attempting the external operation.
    // A failed retry must be observable as Pending, never as a stale Running
    // or Terminated result from an earlier attempt.
    persist_dpf_helm_chart_placement_observations(
        mh_snapshot,
        instance,
        dpf_service_configs,
        &target_dpu_ids,
        force_detach,
        ExtensionServiceDeploymentStatus::Pending,
        db_pool,
    )
    .await?;

    for dpu in &mh_snapshot.dpu_snapshots {
        let dpu_device_name = dpu.dpf_id().ok_or_else(|| StateHandlerError::MissingData {
            object_id: dpu.id.to_string(),
            missing: "DPU BMC MAC required for DPUDevice placement reconciliation",
        })?;
        let is_target = target_dpu_ids.contains(&dpu.id);
        let changes = dpf_helm_chart_placement_label_changes(dpf_service_configs, is_target);
        let requires_device = changes.values().any(Option::is_some);

        match dpf_sdk
            .merge_dpu_device_node_labels(&dpu_device_name, changes)
            .await
        {
            // An absent DPUDevice already has no NICo placement label, so it
            // satisfies a detach/non-target cleanup. It must remain an error
            // for an active target because NICo cannot claim placement there.
            Err(carbide_dpf::DpfError::NotFound { .. }) if !requires_device => {
                tracing::debug!(
                    dpu_machine_id = %dpu.id,
                    "DPUDevice is already absent while removing DPF Helm chart placement"
                );
            }
            Ok(()) => {}
            Err(error) => return Err(StateHandlerError::GenericError(eyre!(error))),
        }
    }

    // Do not publish success until *all* DPUDevice operations have completed.
    // The transaction makes an RPC reader see either Pending or one complete
    // placement generation, never a partial set of Running statuses.
    persist_dpf_helm_chart_placement_observations(
        mh_snapshot,
        instance,
        dpf_service_configs,
        &target_dpu_ids,
        force_detach,
        if force_detach {
            ExtensionServiceDeploymentStatus::Terminated
        } else {
            ExtensionServiceDeploymentStatus::Running
        },
        db_pool,
    )
    .await
}

/// Persists the controller-owned DPF Helm placement result for every physical
/// DPU. Active services have a status only on their current target DPUs;
/// removed (or force-detached) services report on every physical DPU because
/// removal must be confirmed everywhere before instance termination proceeds.
pub(super) async fn persist_dpf_helm_chart_placement_observations(
    mh_snapshot: &ManagedHostStateSnapshot,
    instance: &InstanceSnapshot,
    dpf_service_configs: &[&InstanceExtensionServiceConfig],
    target_dpu_ids: &HashSet<MachineId>,
    force_detach: bool,
    state: ExtensionServiceDeploymentStatus,
    db_pool: &sqlx::PgPool,
) -> Result<HashMap<MachineId, InstanceExtensionServiceStatusObservation>, StateHandlerError> {
    let observed_at = Utc::now();
    let mut txn = db_pool.begin().await?;
    let mut persisted = HashMap::new();
    for dpu in &mh_snapshot.dpu_snapshots {
        let is_target = target_dpu_ids.contains(&dpu.id);
        let extension_service_statuses = dpf_service_configs
            .iter()
            .filter(|config| force_detach || config.removed.is_some() || is_target)
            .map(|config| ExtensionServiceStatusObservation {
                service_id: config.service_id,
                service_type: ExtensionServiceType::DpfHelmChart,
                service_name: String::new(),
                version: config.version,
                removed: config.removed.as_ref().map(ToString::to_string),
                overall_state: state.clone(),
                components: vec![],
                message: String::new(),
            })
            .collect();
        let observation = InstanceExtensionServiceStatusObservation {
            config_version: instance.extension_services_config_version,
            instance_config_version: None,
            extension_service_statuses,
            observed_at,
        };
        db::machine::update_extension_service_status_observation(
            txn.as_mut(),
            &dpu.id,
            ExtensionServiceType::DpfHelmChart,
            &observation,
        )
        .await?;
        persisted.insert(dpu.id, observation);
    }
    txn.commit().await?;
    Ok(persisted)
}

/// Builds the NICo-owned label changes for one physical DPU.
///
/// An active DPF Helm chart service is enabled only when this DPU is currently
/// targeted by the instance network configuration. Removed services, and
/// active services on a DPU removed from that target set, are represented by a
/// `None` value so the DPUDevice merge patch deletes only that service's
/// placement label.
pub(super) fn dpf_helm_chart_placement_label_changes(
    dpf_service_configs: &[&InstanceExtensionServiceConfig],
    is_target: bool,
) -> BTreeMap<String, Option<String>> {
    dpf_service_configs
        .iter()
        .map(|config| {
            let identity = DpfHelmChartIdentity::from_service_id(config.service_id);
            let value = if config.removed.is_none() && is_target {
                Some(DPF_HELM_CHART_PLACEMENT_LABEL_VALUE.to_string())
            } else {
                None
            };
            (identity.placement_label_key, value)
        })
        .collect()
}

pub(super) async fn cleanup_terminated_extension_services(
    instance: &InstanceSnapshot,
    extension_services_status: &mut InstanceExtensionServicesStatus,
    txn: &mut PgConnection,
) -> Result<(), StateHandlerError> {
    if extension_services_status.configs_synced != SyncState::Synced {
        return Ok(());
    }

    let terminated_service_keys = extension_services_status.get_terminated_service_keys();
    if terminated_service_keys.is_empty() {
        return Ok(());
    }

    tracing::info!(
        instance_id = %instance.id,
        terminated_extension_services = ?terminated_service_keys,
        "Cleaning up fully terminated extension services from instance config"
    );
    let new_config = instance
        .config
        .extension_services
        .remove_terminated_services(&terminated_service_keys);

    db::instance::update_extension_services_config(
        txn,
        instance.id,
        instance.extension_services_config_version,
        &new_config,
        false,
    )
    .await?;

    extension_services_status.extension_services.retain(|svc| {
        !terminated_service_keys
            .iter()
            .any(|&(id, ver)| id == svc.service_id && ver == svc.version)
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use carbide_uuid::extension_service::ExtensionServiceId;
    use chrono::Utc;
    use config_version::ConfigVersion;

    use super::*;

    #[test]
    fn dpf_helm_placement_changes_cover_attach_detach_and_target_changes() {
        let active_service =
            ExtensionServiceId::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let removed_service =
            ExtensionServiceId::from_str("00000000-0000-0000-0000-000000000002").unwrap();
        let version = ConfigVersion::initial();
        let active = InstanceExtensionServiceConfig {
            service_id: active_service,
            version,
            removed: None,
        };
        let removed = InstanceExtensionServiceConfig {
            service_id: removed_service,
            version,
            removed: Some(Utc::now()),
        };
        let configs = vec![&active, &removed];
        let active_label =
            DpfHelmChartIdentity::from_service_id(active_service).placement_label_key;
        let removed_label =
            DpfHelmChartIdentity::from_service_id(removed_service).placement_label_key;

        assert_eq!(
            dpf_helm_chart_placement_label_changes(&configs, true),
            BTreeMap::from([
                (
                    active_label.clone(),
                    Some(DPF_HELM_CHART_PLACEMENT_LABEL_VALUE.to_string()),
                ),
                (removed_label.clone(), None),
            ])
        );
        assert_eq!(
            dpf_helm_chart_placement_label_changes(&configs, false),
            BTreeMap::from([(active_label, None), (removed_label, None)])
        );
    }
}
