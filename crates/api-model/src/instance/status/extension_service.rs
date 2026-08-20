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

use std::collections::{BTreeMap, HashMap};

use carbide_utils::none_if_empty::NoneIfEmpty;
use carbide_uuid::extension_service::ExtensionServiceId;
use carbide_uuid::machine::MachineId;
use chrono::{DateTime, Utc};
use config_version::{ConfigVersion, Versioned};
use serde::{Deserialize, Serialize};

use crate::extension_service::ExtensionServiceType;
use crate::instance::config::extension_services::InstanceExtensionServicesConfig;
use crate::instance::status::SyncState;
use crate::machine::Machine;

/// The status of all extension services configured on an instance
#[derive(Clone, Debug)]
pub struct InstanceExtensionServicesStatus {
    /// The status of each configured extension service
    pub extension_services: Vec<InstanceExtensionServiceStatus>,

    /// Whether all desired extension service changes that the user has applied have taken effect
    pub configs_synced: SyncState,
}

impl InstanceExtensionServicesStatus {
    /// Derives the extension services status from the user's desired config
    /// and the observations from DPUs.
    /// For each extension service, we aggregate the statuses from all DPUs.
    /// The config passed must be from database (not rpc InstanceConfig), and must contain any terminating services.
    pub fn from_config_and_observations(
        dpu_ids: &[MachineId],
        config: Versioned<&InstanceExtensionServicesConfig>,
        observations: &HashMap<MachineId, InstanceExtensionServiceStatusObservation>,
    ) -> Self {
        let service_types = config
            .service_configs
            .iter()
            .map(|service| (service.service_id, ExtensionServiceType::KubernetesPod))
            .collect();
        let required_dpus: Vec<_> = config
            .service_configs
            .iter()
            .map(|service| (service.service_id, service.version, dpu_ids.to_vec()))
            .collect();
        let observations = observations
            .iter()
            .map(|(machine_id, observation)| {
                (
                    *machine_id,
                    InstanceExtensionServiceObservations::from_agent_observation(
                        observation.clone(),
                    ),
                )
            })
            .collect();
        Self::from_config_and_service_type_observations(
            config,
            &service_types,
            &required_dpus,
            &observations,
        )
    }

    /// Derives status from type-partitioned observations when the caller does
    /// not have the persisted service-type rows available (notably the pure
    /// RPC conversion path). A DPF Helm entry is recognizable from its own
    /// type-keyed observation; otherwise the legacy KubernetesPod path is
    /// selected. A newly requested DPF service with no observation is still
    /// reported as unsynced/Unknown, which is the safe result.
    pub fn from_config_and_type_observations(
        dpu_ids: &[MachineId],
        config: Versioned<&InstanceExtensionServicesConfig>,
        observations: &HashMap<MachineId, InstanceExtensionServiceObservations>,
    ) -> Self {
        let service_types = config
            .service_configs
            .iter()
            .map(|service| {
                let dpf_observed = observations.values().any(|observation| {
                    observation
                        .for_service_type(ExtensionServiceType::DpfHelmChart)
                        .is_some_and(|dpf| {
                            dpf.extension_service_statuses.iter().any(|status| {
                                status.service_id == service.service_id
                                    && status.version == service.version
                                    && status.service_type == ExtensionServiceType::DpfHelmChart
                            })
                        })
                });
                (
                    service.service_id,
                    if dpf_observed {
                        ExtensionServiceType::DpfHelmChart
                    } else {
                        ExtensionServiceType::KubernetesPod
                    },
                )
            })
            .collect();
        let required_dpus: Vec<_> = config
            .service_configs
            .iter()
            .map(|service| (service.service_id, service.version, dpu_ids.to_vec()))
            .collect();
        Self::from_config_and_service_type_observations(
            config,
            &service_types,
            &required_dpus,
            observations,
        )
    }

    /// Derives every extension-service status through the observation keyed by
    /// its persisted service type. The payload is shared by all writers; only
    /// service-type selection differs. This keeps DPF Helm placement as a
    /// Stage-1 implementation detail rather than a second status model.
    pub fn from_config_and_service_type_observations(
        config: Versioned<&InstanceExtensionServicesConfig>,
        service_types: &HashMap<ExtensionServiceId, ExtensionServiceType>,
        required_dpus: &[(ExtensionServiceId, ConfigVersion, Vec<MachineId>)],
        observations: &HashMap<MachineId, InstanceExtensionServiceObservations>,
    ) -> Self {
        // This means the instance has no extension services configured and all once terminating
        // services has been terminated from all DPUs and hence not present any more
        if config.service_configs.is_empty() {
            return Self {
                extension_services: vec![],
                configs_synced: SyncState::Synced,
            };
        }

        let mut is_configs_synced = true;
        let mut extension_services = vec![];

        // Iterate through each configured service and aggregate status from all DPUs
        for service in config.service_configs.iter() {
            let mut dpu_statuses = vec![];
            let Some(service_type) = service_types.get(&service.service_id) else {
                is_configs_synced = false;
                extension_services.push(InstanceExtensionServiceStatus {
                    service_id: service.service_id,
                    version: service.version,
                    overall_status: ExtensionServiceDeploymentStatus::Unknown,
                    dpu_statuses,
                    removed: service.removed.as_ref().map(ToString::to_string),
                });
                continue;
            };
            let Some((_, _, dpu_ids)) = required_dpus.iter().find(|(service_id, version, _)| {
                *service_id == service.service_id && *version == service.version
            }) else {
                is_configs_synced = false;
                extension_services.push(InstanceExtensionServiceStatus {
                    service_id: service.service_id,
                    version: service.version,
                    overall_status: ExtensionServiceDeploymentStatus::Unknown,
                    dpu_statuses,
                    removed: service.removed.as_ref().map(ToString::to_string),
                });
                continue;
            };
            if dpu_ids.is_empty() {
                is_configs_synced = false;
            }
            for dpu_id in dpu_ids {
                let observation = observations
                    .get(dpu_id)
                    .and_then(|observation| observation.for_service_type(service_type.clone()));

                match observation {
                    // DPU has observation with matching config version
                    Some(obs) if obs.config_version == config.version => {
                        // Find the specific service in the DPU's observation
                        let service_status = obs.extension_service_statuses.iter().find(|s| {
                            s.service_id == service.service_id
                                && s.service_type == *service_type
                                && s.version == service.version
                        });

                        if let Some(service_status) = service_status {
                            dpu_statuses.push(MachineExtensionServiceStatus {
                                machine_id: *dpu_id,
                                status: service_status.overall_state.clone(),
                                error_message: service_status.message.clone().none_if_empty(),
                                components: service_status.components.clone(),
                            });
                        } else {
                            // DPU has observation but service is not in it - mark as Unknown
                            dpu_statuses.push(MachineExtensionServiceStatus {
                                machine_id: *dpu_id,
                                status: ExtensionServiceDeploymentStatus::Unknown,
                                error_message: Some(
                                    format!("Status observation is found for DPU {} but service is not in it.", dpu_id)
                                ),
                                components: vec![],
                            });
                        }
                    }
                    // DPU either has no observation, or observation is for a different config version
                    _ => {
                        is_configs_synced = false;
                        dpu_statuses.push(MachineExtensionServiceStatus {
                            machine_id: *dpu_id,
                            status: ExtensionServiceDeploymentStatus::Unknown,
                            // Note: This is a normal transitional state, not necessarily an error
                            error_message: Some("No status observation observed for this extension service config version yet.".to_string()),
                            components: vec![],
                        });
                    }
                }
            }

            // Calculate overall status based on DPU statuses
            let overall_status = Self::calculate_overall_status(&dpu_statuses);

            extension_services.push(InstanceExtensionServiceStatus {
                service_id: service.service_id,
                version: service.version,
                overall_status,
                dpu_statuses,
                removed: service.removed.as_ref().map(|removed| removed.to_string()),
            });
        }

        Self {
            extension_services,
            configs_synced: if is_configs_synced {
                SyncState::Synced
            } else {
                SyncState::Pending
            },
        }
    }

    /// Calculate the overall status based on the statuses from all DPUs.
    ///
    /// Priority order (highest to lowest):
    /// 1. Error/Failed - Any DPU in error state makes the entire service in error state
    /// 2. Unknown - Any DPU with unknown status means overall status is unknown
    /// 3. Pending - Any DPU pending means the service is not fully deployed yet
    /// 4. Running - All DPUs must be running for overall status to be running
    /// 5. Terminating - Any DPU terminating (and none in higher priority states)
    /// 6. Terminated - All DPUs must be terminated for overall status to be terminated
    /// 7. Unknown - Fallback for unexpected state combinations (e.g., mixed Running/Terminated)
    fn calculate_overall_status(
        dpu_statuses: &[MachineExtensionServiceStatus],
    ) -> ExtensionServiceDeploymentStatus {
        if dpu_statuses.is_empty() {
            return ExtensionServiceDeploymentStatus::Unknown;
        }

        // If any DPU reports Failed or Error, the overall status is Failed
        if dpu_statuses.iter().any(|s| {
            matches!(
                s.status,
                ExtensionServiceDeploymentStatus::Failed | ExtensionServiceDeploymentStatus::Error
            )
        }) {
            return ExtensionServiceDeploymentStatus::Error;
        }

        if dpu_statuses
            .iter()
            .any(|s| matches!(s.status, ExtensionServiceDeploymentStatus::Unknown))
        {
            return ExtensionServiceDeploymentStatus::Unknown;
        }

        // If any DPU is Pending, the overall status is Pending
        if dpu_statuses
            .iter()
            .any(|s| matches!(s.status, ExtensionServiceDeploymentStatus::Pending))
        {
            return ExtensionServiceDeploymentStatus::Pending;
        }

        // If all DPUs are Running, the overall status is Running
        if dpu_statuses
            .iter()
            .all(|s| matches!(s.status, ExtensionServiceDeploymentStatus::Running))
        {
            return ExtensionServiceDeploymentStatus::Running;
        }

        // If any DPU is Terminating, the overall status is Terminating
        if dpu_statuses
            .iter()
            .any(|s| matches!(s.status, ExtensionServiceDeploymentStatus::Terminating))
        {
            return ExtensionServiceDeploymentStatus::Terminating;
        }

        // If all DPUs are Terminated, the overall status is Terminated
        if dpu_statuses
            .iter()
            .all(|s| matches!(s.status, ExtensionServiceDeploymentStatus::Terminated))
        {
            return ExtensionServiceDeploymentStatus::Terminated;
        }

        // Otherwise, Unknown. But we should not reach here.
        ExtensionServiceDeploymentStatus::Unknown
    }

    /// Returns `(service_id, extension service config version)` for extension services that are
    /// marked removed and fully `Terminated` on every DPU. Cleanup must use this pair, not
    /// `service_id` alone, because multiple config versions for the same service can exist during
    /// rolldown/upgrade.
    pub fn get_terminated_service_keys(&self) -> Vec<(ExtensionServiceId, ConfigVersion)> {
        self.extension_services
            .iter()
            .filter(|svc| {
                svc.removed.is_some()
                    && svc.overall_status == ExtensionServiceDeploymentStatus::Terminated
                    // @TODO(Felicity): handle zero dpu case
                    && !svc.dpu_statuses.is_empty()
                    && svc.dpu_statuses.iter().all(|dpu_status| {
                        matches!(
                            dpu_status.status,
                            ExtensionServiceDeploymentStatus::Terminated
                        )
                    })
            })
            .map(|svc| (svc.service_id, svc.version))
            .collect()
    }
}

/// Status of an extension service on a single DPU/machine
#[derive(Clone, Debug)]
pub struct MachineExtensionServiceStatus {
    /// The ID of the DPU this status is from
    pub machine_id: MachineId,
    /// The deployment status of the extension service on this specific DPU
    pub status: ExtensionServiceDeploymentStatus,
    /// Optional error message if the service encountered issues on this DPU
    pub error_message: Option<String>,
    /// The status of individual components/containers of the extension service on this DPU
    pub components: Vec<ExtensionServiceComponent>,
}

/// Aggregated status of a single extension service across all DPUs
#[derive(Clone, Debug)]
pub struct InstanceExtensionServiceStatus {
    /// The unique identifier of the extension service
    pub service_id: ExtensionServiceId,
    /// The version of the extension service configuration
    pub version: ConfigVersion,
    /// The aggregated status across all DPUs (calculated from dpu_statuses)
    pub overall_status: ExtensionServiceDeploymentStatus,
    /// Per-DPU status details for this service
    pub dpu_statuses: Vec<MachineExtensionServiceStatus>,
    /// Timestamp when the service was marked for removal, if applicable
    /// When Some, the service is in the process of being terminated
    pub removed: Option<String>,
}

/// Extension service deployment status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtensionServiceDeploymentStatus {
    Unknown,
    Pending,
    Running,
    Terminating,
    Terminated,
    Failed,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionServiceComponent {
    pub name: String,
    pub version: String, // This is the version of the component, not the version of the extension service
    pub url: String,
    pub status: String,
}

/// A single extension service status reported by DPU agent
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionServiceStatusObservation {
    pub service_id: ExtensionServiceId,
    pub service_type: ExtensionServiceType,
    pub service_name: String,
    pub version: ConfigVersion,
    pub removed: Option<String>,
    pub overall_state: ExtensionServiceDeploymentStatus,
    pub components: Vec<ExtensionServiceComponent>,
    pub message: String,
}

/// Observation of extension-service statuses for a single DPU.
///
/// The payload is deliberately source-neutral.  It is reported by the DPU
/// agent for KubernetesPod services, by NICo for the Stage-1 DPF Helm
/// placement contract, and later by DPF for workload status.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceExtensionServiceStatusObservation {
    /// The config version that the DPU has applied for extension services
    /// This is compared against the desired config version to determine if configs are synced
    pub config_version: ConfigVersion,

    /// The observed version of the instance config
    pub instance_config_version: Option<ConfigVersion>,

    /// The status of each extension service running on this DPU
    pub extension_service_statuses: Vec<ExtensionServiceStatusObservation>,

    /// The timestamp when this source made the observation.
    pub observed_at: DateTime<Utc>,
}

/// Current extension-service observations for one DPU, partitioned by service
/// type.  There is exactly one authoritative status writer for each service
/// type: the DPU agent for Kubernetes Pod services and, in Stage 1, the NICo
/// machine controller for DPF Helm chart placement.  A future DPF status
/// integration replaces the DPF Helm writer for the same key rather than
/// adding another observation shape or column.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceExtensionServiceObservations {
    #[serde(default)]
    pub by_service_type: BTreeMap<String, InstanceExtensionServiceStatusObservation>,
}

impl InstanceExtensionServiceObservations {
    fn service_type_key(service_type: ExtensionServiceType) -> String {
        service_type.to_string()
    }

    pub fn for_service_type(
        &self,
        service_type: ExtensionServiceType,
    ) -> Option<&InstanceExtensionServiceStatusObservation> {
        self.by_service_type
            .get(&Self::service_type_key(service_type))
    }

    pub fn set_for_service_type(
        &mut self,
        service_type: ExtensionServiceType,
        observation: InstanceExtensionServiceStatusObservation,
    ) {
        self.by_service_type
            .insert(Self::service_type_key(service_type), observation);
    }

    pub fn from_agent_observation(observation: InstanceExtensionServiceStatusObservation) -> Self {
        let mut observations = Self::default();
        observations.set_for_service_type(ExtensionServiceType::KubernetesPod, observation);
        observations
    }

    /// Aggregates persisted type-partitioned observations.
    pub fn aggregate_instance_observation(dpu_snapshots: &[Machine]) -> HashMap<MachineId, Self> {
        dpu_snapshots
            .iter()
            .filter_map(|dpu| {
                let observations = dpu.status.extension_service_status_observations.clone();
                (!observations.by_service_type.is_empty()).then_some((dpu.id, observations))
            })
            .collect()
    }
}

impl InstanceExtensionServiceStatusObservation {
    /// Drops statuses for services which are not managed by the legacy DPU
    /// agent. DPF Helm services are reconciled through DPF and must never be
    /// used as an agent status source.
    pub fn retain_agent_managed_statuses(&mut self) {
        self.extension_service_statuses
            .retain(|service| service.service_type == ExtensionServiceType::KubernetesPod);
    }

    pub fn any_observed_version_changed(&self, other: &Self) -> bool {
        if (self.config_version != other.config_version)
            || (self.instance_config_version != other.instance_config_version)
        {
            return true;
        }

        let self_extension_service_versions: HashMap<ExtensionServiceId, ConfigVersion> =
            HashMap::from_iter(
                self.extension_service_statuses
                    .iter()
                    .filter(|service| service.service_type == ExtensionServiceType::KubernetesPod)
                    .map(|svc| (svc.service_id, svc.version)),
            );
        let other_extension_service_versions: HashMap<ExtensionServiceId, ConfigVersion> =
            HashMap::from_iter(
                other
                    .extension_service_statuses
                    .iter()
                    .filter(|service| service.service_type == ExtensionServiceType::KubernetesPod)
                    .map(|svc| (svc.service_id, svc.version)),
            );

        self_extension_service_versions != other_extension_service_versions
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionServicesReadiness {
    /// Configs are fully applied, and all non-removed (if any) services are Running.
    Ready,
    /// Configs are not yet applied across all DPUs.
    ConfigsPending,
    /// Some non-removed service is not Running.
    NotFullyRunning,
    /// Some removed services are still terminating on some DPU.
    SomeTerminating,
}

pub fn compute_extension_services_readiness(
    extension_services_status: &InstanceExtensionServicesStatus,
) -> ExtensionServicesReadiness {
    if extension_services_status.configs_synced == SyncState::Pending {
        return ExtensionServicesReadiness::ConfigsPending;
    }

    if extension_services_status
        .extension_services
        .iter()
        .any(|s| {
            s.removed.is_none() && s.overall_status != ExtensionServiceDeploymentStatus::Running
        })
    {
        return ExtensionServicesReadiness::NotFullyRunning;
    }

    if extension_services_status
        .extension_services
        .iter()
        .any(|s| {
            s.removed.is_some() && s.overall_status != ExtensionServiceDeploymentStatus::Terminated
        })
    {
        return ExtensionServicesReadiness::SomeTerminating;
    }

    // All checks passed: configs synced, all active services running, no services terminating
    ExtensionServicesReadiness::Ready
}

pub fn is_extension_services_ready(
    extension_services_status: &InstanceExtensionServicesStatus,
) -> bool {
    compute_extension_services_readiness(extension_services_status)
        == ExtensionServicesReadiness::Ready
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use carbide_test_support::value_scenarios;
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::extension_service::ExtensionServiceType;
    use crate::instance::config::extension_services::{
        InstanceExtensionServiceConfig, InstanceExtensionServicesConfig,
    };

    fn get_dpu_ids() -> Vec<MachineId> {
        vec![
            MachineId::from_str("fm100dskla0ihp0pn4tv7v1js2k2mo37sl0jjr8141okqg8pjpdpfihaa80")
                .unwrap(),
            MachineId::from_str("fm100ds27v4uuq7sgs4gsjummskt0b3tedugtpevjrbfh6su081n9jufcq0")
                .unwrap(),
        ]
    }

    fn get_test_service_id() -> ExtensionServiceId {
        ExtensionServiceId::from_str("00000000-0000-0000-0000-000000000000").unwrap()
    }

    fn create_service_config(version: ConfigVersion) -> InstanceExtensionServicesConfig {
        InstanceExtensionServicesConfig {
            service_configs: vec![InstanceExtensionServiceConfig {
                service_id: get_test_service_id(),
                version,
                removed: None,
            }],
        }
    }

    fn create_observation(
        config_version: ConfigVersion,
        service_version: ConfigVersion,
        status: ExtensionServiceDeploymentStatus,
    ) -> InstanceExtensionServiceStatusObservation {
        InstanceExtensionServiceStatusObservation {
            config_version,
            instance_config_version: None,
            extension_service_statuses: vec![ExtensionServiceStatusObservation {
                service_id: get_test_service_id(),
                service_type: ExtensionServiceType::KubernetesPod,
                service_name: "test-service".to_string(),
                version: service_version,
                removed: None,
                overall_state: status,
                components: vec![],
                message: String::new(),
            }],
            observed_at: Utc::now(),
        }
    }

    fn create_observations(
        statuses: impl IntoIterator<
            Item = (
                MachineId,
                ConfigVersion,
                ConfigVersion,
                ExtensionServiceDeploymentStatus,
            ),
        >,
    ) -> HashMap<MachineId, InstanceExtensionServiceStatusObservation> {
        statuses
            .into_iter()
            .map(|(dpu_id, config_version, service_version, status)| {
                (
                    dpu_id,
                    create_observation(config_version, service_version, status),
                )
            })
            .collect()
    }

    #[test]
    fn dpf_helm_status_uses_the_standard_type_keyed_observation() {
        let dpu_ids = get_dpu_ids();
        let config_version = ConfigVersion::initial();
        let service_version = ConfigVersion::initial();
        let observations = dpu_ids
            .iter()
            .map(|dpu_id| {
                let mut per_type = InstanceExtensionServiceObservations::default();
                per_type.set_for_service_type(
                    ExtensionServiceType::DpfHelmChart,
                    InstanceExtensionServiceStatusObservation {
                        config_version,
                        instance_config_version: None,
                        extension_service_statuses: vec![ExtensionServiceStatusObservation {
                            service_id: get_test_service_id(),
                            service_type: ExtensionServiceType::DpfHelmChart,
                            service_name: String::new(),
                            version: service_version,
                            removed: None,
                            overall_state: ExtensionServiceDeploymentStatus::Running,
                            components: vec![],
                            message: String::new(),
                        }],
                        observed_at: Utc::now(),
                    },
                );
                (*dpu_id, per_type)
            })
            .collect();

        let status = InstanceExtensionServicesStatus::from_config_and_type_observations(
            &dpu_ids,
            Versioned::new(&create_service_config(service_version), config_version),
            &observations,
        );

        assert_eq!(status.configs_synced, SyncState::Synced);
        assert_eq!(
            status.extension_services[0].overall_status,
            ExtensionServiceDeploymentStatus::Running
        );
    }

    #[test]
    fn aggregate_instance_observation_keeps_service_type_writers_independent() {
        let mut dpu = crate::test_support::machine_snapshot::dpu_machine(0);
        let mut observations = InstanceExtensionServiceObservations::default();
        observations.set_for_service_type(
            ExtensionServiceType::KubernetesPod,
            create_observation(
                ConfigVersion::initial(),
                ConfigVersion::initial(),
                ExtensionServiceDeploymentStatus::Running,
            ),
        );
        let mut dpf_observation = create_observation(
            ConfigVersion::initial(),
            ConfigVersion::initial(),
            ExtensionServiceDeploymentStatus::Running,
        );
        dpf_observation.extension_service_statuses[0].service_type =
            ExtensionServiceType::DpfHelmChart;
        observations.set_for_service_type(ExtensionServiceType::DpfHelmChart, dpf_observation);
        dpu.status.extension_service_status_observations = observations;

        let aggregated =
            InstanceExtensionServiceObservations::aggregate_instance_observation(&[dpu.clone()]);
        let aggregated = &aggregated[&dpu.id];
        assert!(
            aggregated
                .for_service_type(ExtensionServiceType::KubernetesPod)
                .is_some()
        );
        assert!(
            aggregated
                .for_service_type(ExtensionServiceType::DpfHelmChart)
                .is_some()
        );
    }

    struct StatusInput {
        dpu_ids: Vec<MachineId>,
        config: InstanceExtensionServicesConfig,
        config_version: ConfigVersion,
        observations: HashMap<MachineId, InstanceExtensionServiceStatusObservation>,
    }

    fn status_input(
        dpu_ids: Vec<MachineId>,
        service_version: ConfigVersion,
        config_version: ConfigVersion,
        observations: HashMap<MachineId, InstanceExtensionServiceStatusObservation>,
    ) -> StatusInput {
        StatusInput {
            dpu_ids,
            config: create_service_config(service_version),
            config_version,
            observations,
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct DpuStatusProjection {
        machine_id: MachineId,
        status: ExtensionServiceDeploymentStatus,
        error_message: Option<String>,
        components: Vec<ExtensionServiceComponent>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ServiceStatusProjection {
        service_id: ExtensionServiceId,
        version: u64,
        overall_status: ExtensionServiceDeploymentStatus,
        dpu_statuses: Vec<DpuStatusProjection>,
        removed: Option<String>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct StatusProjection {
        configs_synced: SyncState,
        extension_services: Vec<ServiceStatusProjection>,
    }

    fn project_status(status: InstanceExtensionServicesStatus) -> StatusProjection {
        StatusProjection {
            configs_synced: status.configs_synced,
            extension_services: status
                .extension_services
                .into_iter()
                .map(|service| ServiceStatusProjection {
                    service_id: service.service_id,
                    version: service.version.version_nr(),
                    overall_status: service.overall_status,
                    dpu_statuses: service
                        .dpu_statuses
                        .into_iter()
                        .map(|dpu| DpuStatusProjection {
                            machine_id: dpu.machine_id,
                            status: dpu.status,
                            error_message: dpu.error_message,
                            components: dpu.components,
                        })
                        .collect(),
                    removed: service.removed,
                })
                .collect(),
        }
    }

    fn expected_dpu_status(
        machine_id: MachineId,
        status: ExtensionServiceDeploymentStatus,
        error_message: Option<&str>,
    ) -> DpuStatusProjection {
        DpuStatusProjection {
            machine_id,
            status,
            error_message: error_message.map(str::to_string),
            components: vec![],
        }
    }

    fn expected_status(
        configs_synced: SyncState,
        overall_status: ExtensionServiceDeploymentStatus,
        dpu_statuses: Vec<DpuStatusProjection>,
    ) -> StatusProjection {
        StatusProjection {
            extension_services: vec![ServiceStatusProjection {
                service_id: get_test_service_id(),
                version: 1,
                overall_status,
                dpu_statuses,
                removed: None,
            }],
            configs_synced,
        }
    }

    #[test]
    fn extension_service_status_from_config_and_observations() {
        let service_version = ConfigVersion::initial();
        let config_version = ConfigVersion::initial();
        let [dpu1_id, dpu2_id] = get_dpu_ids().try_into().unwrap();
        let missing_observation =
            "No status observation observed for this extension service config version yet.";
        let component = ExtensionServiceComponent {
            name: "test-component".to_string(),
            version: "1.0.0".to_string(),
            url: "registry.example.test/test-component:1.0.0".to_string(),
            status: "Running".to_string(),
        };
        let service_message = "service is running";
        let mut synced_observation = create_observations([(
            dpu1_id,
            config_version,
            service_version,
            ExtensionServiceDeploymentStatus::Running,
        )]);
        let synced_service = &mut synced_observation
            .get_mut(&dpu1_id)
            .unwrap()
            .extension_service_statuses[0];
        synced_service.message = service_message.to_string();
        synced_service.components = vec![component.clone()];
        let stale_service_observation = create_observations([(
            dpu1_id,
            config_version,
            service_version.increment(),
            ExtensionServiceDeploymentStatus::Running,
        )]);
        let mut other_service_observation = create_observations([(
            dpu1_id,
            config_version,
            service_version,
            ExtensionServiceDeploymentStatus::Running,
        )]);
        other_service_observation
            .get_mut(&dpu1_id)
            .unwrap()
            .extension_service_statuses[0]
            .service_id =
            ExtensionServiceId::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let missing_service =
            format!("Status observation is found for DPU {dpu1_id} but service is not in it.");
        let removed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let removed_at_text = "2026-01-01 00:00:00 UTC".to_string();
        let second_service_version = service_version.increment();

        value_scenarios!(
            run = |StatusInput {
                dpu_ids,
                config,
                config_version,
                observations,
            }| {
                project_status(InstanceExtensionServicesStatus::from_config_and_observations(
                    &dpu_ids,
                    Versioned::new(&config, config_version),
                    &observations,
                ))
            };
            "without configured services" {
                StatusInput {
                    dpu_ids: vec![dpu1_id],
                    config: InstanceExtensionServicesConfig {
                        service_configs: vec![],
                    },
                    config_version,
                    observations: HashMap::new(),
                } => StatusProjection {
                    configs_synced: SyncState::Synced,
                    extension_services: vec![],
                },
            }

            "with configured services but no target DPUs" {
                StatusInput {
                    dpu_ids: vec![],
                    config: InstanceExtensionServicesConfig {
                        service_configs: vec![
                            InstanceExtensionServiceConfig {
                                service_id: get_test_service_id(),
                                version: second_service_version,
                                removed: None,
                            },
                            InstanceExtensionServiceConfig {
                                service_id: get_test_service_id(),
                                version: service_version,
                                removed: Some(removed_at),
                            },
                        ],
                    },
                    config_version,
                    observations: HashMap::new(),
                } => StatusProjection {
                    configs_synced: SyncState::Pending,
                    extension_services: vec![
                        ServiceStatusProjection {
                            service_id: get_test_service_id(),
                            version: 2,
                            overall_status: ExtensionServiceDeploymentStatus::Unknown,
                            dpu_statuses: vec![],
                            removed: None,
                        },
                        ServiceStatusProjection {
                            service_id: get_test_service_id(),
                            version: 1,
                            overall_status: ExtensionServiceDeploymentStatus::Unknown,
                            dpu_statuses: vec![],
                            removed: Some(removed_at_text),
                        },
                    ],
                },
            }

            "without observations" {
                status_input(
                    vec![dpu1_id],
                    service_version,
                    config_version,
                    HashMap::new(),
                ) => expected_status(
                    SyncState::Pending,
                    ExtensionServiceDeploymentStatus::Unknown,
                    vec![expected_dpu_status(
                        dpu1_id,
                        ExtensionServiceDeploymentStatus::Unknown,
                        Some(missing_observation),
                    )],
                ),
            }

            "with a synced observation" {
                status_input(
                    vec![dpu1_id],
                    service_version,
                    config_version,
                    synced_observation,
                ) => expected_status(
                    SyncState::Synced,
                    ExtensionServiceDeploymentStatus::Running,
                    vec![DpuStatusProjection {
                        machine_id: dpu1_id,
                        status: ExtensionServiceDeploymentStatus::Running,
                        error_message: Some(service_message.to_string()),
                        components: vec![component],
                    }],
                ),
            }

            "when a synced observation only has a stale service version" {
                status_input(
                    vec![dpu1_id],
                    service_version,
                    config_version,
                    stale_service_observation,
                ) => expected_status(
                    SyncState::Synced,
                    ExtensionServiceDeploymentStatus::Unknown,
                    vec![expected_dpu_status(
                        dpu1_id,
                        ExtensionServiceDeploymentStatus::Unknown,
                        Some(missing_service.as_str()),
                    )],
                ),
            }

            "when a synced observation only has another service" {
                status_input(
                    vec![dpu1_id],
                    service_version,
                    config_version,
                    other_service_observation,
                ) => expected_status(
                    SyncState::Synced,
                    ExtensionServiceDeploymentStatus::Unknown,
                    vec![expected_dpu_status(
                        dpu1_id,
                        ExtensionServiceDeploymentStatus::Unknown,
                        Some(missing_service.as_str()),
                    )],
                ),
            }

            "with an outdated observation" {
                status_input(
                    vec![dpu1_id],
                    service_version,
                    config_version.increment(),
                    create_observations([(
                        dpu1_id,
                        config_version,
                        service_version,
                        ExtensionServiceDeploymentStatus::Running,
                    )]),
                ) => expected_status(
                    SyncState::Pending,
                    ExtensionServiceDeploymentStatus::Unknown,
                    vec![expected_dpu_status(
                        dpu1_id,
                        ExtensionServiceDeploymentStatus::Unknown,
                        Some(missing_observation),
                    )],
                ),
            }

            "with one of two DPU observations missing" {
                status_input(
                    vec![dpu1_id, dpu2_id],
                    service_version,
                    config_version,
                    create_observations([(
                        dpu1_id,
                        config_version,
                        service_version,
                        ExtensionServiceDeploymentStatus::Running,
                    )]),
                ) => expected_status(
                    SyncState::Pending,
                    ExtensionServiceDeploymentStatus::Unknown,
                    vec![
                        expected_dpu_status(
                            dpu1_id,
                            ExtensionServiceDeploymentStatus::Running,
                            None,
                        ),
                        expected_dpu_status(
                            dpu2_id,
                            ExtensionServiceDeploymentStatus::Unknown,
                            Some(missing_observation),
                        ),
                    ],
                ),
            }

            "with all DPU observations present" {
                status_input(
                    vec![dpu1_id, dpu2_id],
                    service_version,
                    config_version,
                    create_observations([
                        (
                            dpu1_id,
                            config_version,
                            service_version,
                            ExtensionServiceDeploymentStatus::Running,
                        ),
                        (
                            dpu2_id,
                            config_version,
                            service_version,
                            ExtensionServiceDeploymentStatus::Running,
                        ),
                    ]),
                ) => expected_status(
                    SyncState::Synced,
                    ExtensionServiceDeploymentStatus::Running,
                    vec![
                        expected_dpu_status(
                            dpu1_id,
                            ExtensionServiceDeploymentStatus::Running,
                            None,
                        ),
                        expected_dpu_status(
                            dpu2_id,
                            ExtensionServiceDeploymentStatus::Running,
                            None,
                        ),
                    ],
                ),
            }

            "scoped to target DPUs" {
                status_input(
                    vec![dpu1_id],
                    service_version,
                    config_version,
                    create_observations([
                        (
                            dpu1_id,
                            config_version,
                            service_version,
                            ExtensionServiceDeploymentStatus::Running,
                        ),
                        (
                            dpu2_id,
                            config_version,
                            service_version,
                            ExtensionServiceDeploymentStatus::Pending,
                        ),
                    ]),
                ) => expected_status(
                    SyncState::Synced,
                    ExtensionServiceDeploymentStatus::Running,
                    vec![expected_dpu_status(
                        dpu1_id,
                        ExtensionServiceDeploymentStatus::Running,
                        None,
                    )],
                ),
            }
        );
    }

    fn readiness_status(
        configs_synced: SyncState,
        services: impl IntoIterator<Item = (bool, ExtensionServiceDeploymentStatus)>,
    ) -> InstanceExtensionServicesStatus {
        InstanceExtensionServicesStatus {
            extension_services: services
                .into_iter()
                .map(|(removed, overall_status)| InstanceExtensionServiceStatus {
                    service_id: get_test_service_id(),
                    version: ConfigVersion::initial(),
                    overall_status,
                    dpu_statuses: vec![],
                    removed: removed.then(|| Utc::now().to_string()),
                })
                .collect(),
            configs_synced,
        }
    }

    #[test]
    fn extension_service_readiness() {
        value_scenarios!(
            run = |status| (
                compute_extension_services_readiness(&status),
                is_extension_services_ready(&status),
            );
            "configs pending" {
                readiness_status(
                    SyncState::Pending,
                    [(false, ExtensionServiceDeploymentStatus::Unknown)],
                ) => (ExtensionServicesReadiness::ConfigsPending, false),
            }

            "configs synced without services" {
                readiness_status(SyncState::Synced, []) =>
                    (ExtensionServicesReadiness::Ready, true),
            }

            "configs synced and service running" {
                readiness_status(
                    SyncState::Synced,
                    [(false, ExtensionServiceDeploymentStatus::Running)],
                ) => (ExtensionServicesReadiness::Ready, true),
            }

            "active service not running" {
                readiness_status(
                    SyncState::Synced,
                    [(false, ExtensionServiceDeploymentStatus::Pending)],
                ) => (ExtensionServicesReadiness::NotFullyRunning, false),
            }

            "removed service still terminating" {
                readiness_status(
                    SyncState::Synced,
                    [(true, ExtensionServiceDeploymentStatus::Terminating)],
                ) => (ExtensionServicesReadiness::SomeTerminating, false),
            }

            "removed service terminated" {
                readiness_status(
                    SyncState::Synced,
                    [(true, ExtensionServiceDeploymentStatus::Terminated)],
                ) => (ExtensionServicesReadiness::Ready, true),
            }

            "active failure takes precedence over removed termination" {
                readiness_status(
                    SyncState::Synced,
                    [
                        (false, ExtensionServiceDeploymentStatus::Failed),
                        (true, ExtensionServiceDeploymentStatus::Terminating),
                    ],
                ) => (ExtensionServicesReadiness::NotFullyRunning, false),
            }
        );
    }

    #[test]
    fn extension_service_calculate_overall_status() {
        value_scenarios!(
            run = |statuses: Vec<ExtensionServiceDeploymentStatus>| {
                let machine_id = get_dpu_ids()[0];
                let dpu_statuses = statuses
                    .into_iter()
                    .map(|status| MachineExtensionServiceStatus {
                        machine_id,
                        status,
                        error_message: None,
                        components: vec![],
                    })
                    .collect::<Vec<_>>();
                InstanceExtensionServicesStatus::calculate_overall_status(&dpu_statuses)
            };
            "all running" {
                vec![
                    ExtensionServiceDeploymentStatus::Running,
                    ExtensionServiceDeploymentStatus::Running,
                ] => ExtensionServiceDeploymentStatus::Running,
            }

            "one failed" {
                vec![
                    ExtensionServiceDeploymentStatus::Running,
                    ExtensionServiceDeploymentStatus::Failed,
                ] => ExtensionServiceDeploymentStatus::Error,
            }

            "error takes precedence over unknown" {
                vec![
                    ExtensionServiceDeploymentStatus::Unknown,
                    ExtensionServiceDeploymentStatus::Error,
                ] => ExtensionServiceDeploymentStatus::Error,
            }

            "unknown takes precedence over pending" {
                vec![
                    ExtensionServiceDeploymentStatus::Pending,
                    ExtensionServiceDeploymentStatus::Unknown,
                ] => ExtensionServiceDeploymentStatus::Unknown,
            }

            "one pending" {
                vec![
                    ExtensionServiceDeploymentStatus::Running,
                    ExtensionServiceDeploymentStatus::Pending,
                ] => ExtensionServiceDeploymentStatus::Pending,
            }

            "pending takes precedence over terminating" {
                vec![
                    ExtensionServiceDeploymentStatus::Terminating,
                    ExtensionServiceDeploymentStatus::Pending,
                ] => ExtensionServiceDeploymentStatus::Pending,
            }

            "one terminating" {
                vec![
                    ExtensionServiceDeploymentStatus::Running,
                    ExtensionServiceDeploymentStatus::Terminating,
                ] => ExtensionServiceDeploymentStatus::Terminating,
            }

            "terminating takes precedence over terminated" {
                vec![
                    ExtensionServiceDeploymentStatus::Terminated,
                    ExtensionServiceDeploymentStatus::Terminating,
                ] => ExtensionServiceDeploymentStatus::Terminating,
            }

            "all terminated" {
                vec![
                    ExtensionServiceDeploymentStatus::Terminated,
                    ExtensionServiceDeploymentStatus::Terminated,
                ] => ExtensionServiceDeploymentStatus::Terminated,
            }

            "mixed running and terminated" {
                vec![
                    ExtensionServiceDeploymentStatus::Running,
                    ExtensionServiceDeploymentStatus::Terminated,
                ] => ExtensionServiceDeploymentStatus::Unknown,
            }

            "empty" {
                vec![] => ExtensionServiceDeploymentStatus::Unknown,
            }
        );
    }

    #[test]
    fn agent_observation_version_comparison_ignores_dpf_helm_statuses() {
        let config_version = ConfigVersion::initial();
        let observation = create_observation(
            config_version,
            ConfigVersion::initial(),
            ExtensionServiceDeploymentStatus::Running,
        );
        let mut observation_with_dpf_status = observation.clone();
        let mut dpf_status = observation.extension_service_statuses[0].clone();
        dpf_status.service_type = ExtensionServiceType::DpfHelmChart;
        dpf_status.version = ConfigVersion::initial().increment();
        observation_with_dpf_status
            .extension_service_statuses
            .push(dpf_status);

        assert!(!observation.any_observed_version_changed(&observation_with_dpf_status));
    }

    fn create_observation_two_versions(
        dpu_id: MachineId,
        cfg_version: ConfigVersion,
        v_new: ConfigVersion,
        new_state: ExtensionServiceDeploymentStatus,
        v_old: ConfigVersion,
        old_state: ExtensionServiceDeploymentStatus,
    ) -> HashMap<MachineId, InstanceExtensionServiceStatusObservation> {
        let mut observations = HashMap::new();
        observations.insert(
            dpu_id,
            InstanceExtensionServiceStatusObservation {
                config_version: cfg_version,
                instance_config_version: None,
                extension_service_statuses: vec![
                    ExtensionServiceStatusObservation {
                        service_id: get_test_service_id(),
                        service_type: ExtensionServiceType::KubernetesPod,
                        service_name: "test-service".to_string(),
                        version: v_new,
                        removed: None,
                        overall_state: new_state,
                        components: vec![],
                        message: String::new(),
                    },
                    ExtensionServiceStatusObservation {
                        service_id: get_test_service_id(),
                        service_type: ExtensionServiceType::KubernetesPod,
                        service_name: "test-service".to_string(),
                        version: v_old,
                        removed: Some(Utc::now().to_rfc3339()),
                        overall_state: old_state,
                        components: vec![],
                        message: String::new(),
                    },
                ],
                observed_at: chrono::Utc::now(),
            },
        );
        observations
    }

    fn machine_statuses(
        statuses: impl IntoIterator<Item = (MachineId, ExtensionServiceDeploymentStatus)>,
    ) -> Vec<MachineExtensionServiceStatus> {
        statuses
            .into_iter()
            .map(|(machine_id, status)| MachineExtensionServiceStatus {
                machine_id,
                status,
                error_message: None,
                components: vec![],
            })
            .collect()
    }

    fn service_status(
        version: ConfigVersion,
        removed: bool,
        overall_status: ExtensionServiceDeploymentStatus,
        dpu_statuses: Vec<MachineExtensionServiceStatus>,
    ) -> InstanceExtensionServiceStatus {
        InstanceExtensionServiceStatus {
            service_id: get_test_service_id(),
            version,
            overall_status,
            dpu_statuses,
            removed: removed.then(|| "removed".to_string()),
        }
    }

    #[test]
    fn extension_service_get_terminated_service_keys() {
        let [dpu1_id, dpu2_id] = get_dpu_ids().try_into().unwrap();

        let init_version = ConfigVersion::initial();
        let second_version = init_version.increment();
        let config = InstanceExtensionServicesConfig {
            service_configs: vec![
                InstanceExtensionServiceConfig {
                    service_id: get_test_service_id(),
                    version: second_version,
                    removed: None,
                },
                InstanceExtensionServiceConfig {
                    service_id: get_test_service_id(),
                    version: init_version,
                    removed: Some(Utc::now()),
                },
            ],
        };
        let config_version = ConfigVersion::initial();
        let observations = create_observation_two_versions(
            dpu1_id,
            config_version,
            second_version,
            ExtensionServiceDeploymentStatus::Running,
            init_version,
            ExtensionServiceDeploymentStatus::Terminated,
        );

        let aggregated_status = InstanceExtensionServicesStatus::from_config_and_observations(
            &[dpu1_id],
            Versioned::new(&config, config_version),
            &observations,
        );

        value_scenarios!(
            run = |status: InstanceExtensionServicesStatus| {
                status.get_terminated_service_keys()
            };
            "removed version terminated on every DPU" {
                aggregated_status => vec![(get_test_service_id(), init_version)],
            }

            "active version" {
                InstanceExtensionServicesStatus {
                    extension_services: vec![service_status(
                        init_version,
                        false,
                        ExtensionServiceDeploymentStatus::Terminated,
                        machine_statuses([(
                            dpu1_id,
                            ExtensionServiceDeploymentStatus::Terminated,
                        )]),
                    )],
                    configs_synced: SyncState::Synced,
                } => vec![],
            }

            "removed version not terminated overall" {
                InstanceExtensionServicesStatus {
                    extension_services: vec![service_status(
                        init_version,
                        true,
                        ExtensionServiceDeploymentStatus::Terminating,
                        machine_statuses([(
                            dpu1_id,
                            ExtensionServiceDeploymentStatus::Terminated,
                        )]),
                    )],
                    configs_synced: SyncState::Synced,
                } => vec![],
            }

            "removed version without DPU statuses" {
                InstanceExtensionServicesStatus {
                    extension_services: vec![service_status(
                        init_version,
                        true,
                        ExtensionServiceDeploymentStatus::Terminated,
                        vec![],
                    )],
                    configs_synced: SyncState::Synced,
                } => vec![],
            }

            "removed version with one DPU not terminated" {
                InstanceExtensionServicesStatus {
                    extension_services: vec![service_status(
                        init_version,
                        true,
                        ExtensionServiceDeploymentStatus::Terminated,
                        machine_statuses([
                            (
                                dpu1_id,
                                ExtensionServiceDeploymentStatus::Terminated,
                            ),
                            (dpu2_id, ExtensionServiceDeploymentStatus::Running),
                        ]),
                    )],
                    configs_synced: SyncState::Synced,
                } => vec![],
            }
        );
    }
}
