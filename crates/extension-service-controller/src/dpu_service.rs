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

//! Pure projection and ownership validation for NICo-owned `DPUService` CRs.
//!
//! This module deliberately has no controller context and performs no DPF
//! operation.  The controller can use its results in a later component, but
//! keeping projection here makes the desired-object contract independently
//! testable and prevents a live object from influencing what NICo intends to
//! manage.

use std::collections::BTreeMap;

use carbide_dpf::{DetachedDpuServiceDefinition, DetachedHelmChart, DpuServiceObservation};
use carbide_uuid::extension_service::ExtensionServiceId;
use model::extension_service::{
    DPF_HELM_CHART_OWNER_LABEL, DPF_HELM_CHART_PLACEMENT_LABEL_VALUE, DpfHelmChartIdentity,
    DpfHelmChartServiceData,
};
use serde_json::{Map, Value, json};

/// A projected DPUService.
#[derive(Debug, Clone)]
pub struct DpuServiceProjection {
    pub service: DetachedDpuServiceDefinition,
}

/// Builds the complete, detached DPUService definition owned by one extension
/// service. The DPF SDK alone converts this definition to the checked CR type.
///
/// The service is deliberately detached until an instance lifecycle operation
/// applies `placement_label_key=enabled` to a DPU.  Its `nodeSelector` is an
/// exact-match selector, so it has no target before that label exists.
pub fn project_dpu_service(
    extension_service_id: ExtensionServiceId,
    namespace: &str,
    data: &DpfHelmChartServiceData,
) -> DpuServiceProjection {
    let identity = DpfHelmChartIdentity::from_service_id(extension_service_id);
    let service = DetachedDpuServiceDefinition {
        name: identity.dpu_service_name.clone(),
        namespace: namespace.to_owned(),
        labels: BTreeMap::from([(
            DPF_HELM_CHART_OWNER_LABEL.to_owned(),
            extension_service_id.to_string(),
        )]),
        helm_chart: projected_helm_chart(&identity, data),
        deploy_in_cluster: false,
        security_privileged: data.security_privileged,
        node_selector_labels: detached_node_selector_labels(&identity),
    };

    DpuServiceProjection { service }
}

/// Returns a JSON merge patch containing only the mutable DPUService fields
/// that NICo owns.  Identity and attachment-bound fields are intentionally
/// absent: they must be validated before applying this patch, never repaired
/// or overwritten.
///
/// An absent `values` is represented by `null` in the patch so a previously
/// stored values object is removed.  The full projected CR, by contrast,
/// serializes absent values by omitting that field.
pub fn dpu_service_mutable_patch(projected: &DetachedDpuServiceDefinition) -> Value {
    let helm_chart = &projected.helm_chart;
    let mut helm_chart_patch = Map::from_iter([(
        "source".to_owned(),
        json!({
            "repoURL": helm_chart.repo_url,
            "chart": helm_chart.chart,
            "version": helm_chart.version,
        }),
    )]);
    helm_chart_patch.insert(
        "values".to_owned(),
        helm_chart
            .values
            .as_ref()
            .map_or(Value::Null, |values| json!(values)),
    );

    json!({
        "spec": {
            "helmChart": helm_chart_patch,
            "security": {"privileged": projected.security_privileged},
        },
    })
}

/// Validates that a live DPUService is the object NICo is allowed to manage.
///
/// Errors name the conflicting contract field only.  They intentionally never
/// include live or desired Helm values, which may contain tenant secrets.
pub fn verify_dpu_service_ownership(
    existing: &DpuServiceObservation,
    extension_service_id: ExtensionServiceId,
    namespace: &str,
) -> Result<(), DpuServiceOwnershipConflict> {
    let identity = DpfHelmChartIdentity::from_service_id(extension_service_id);

    verify_dpu_service_owner_label(existing, extension_service_id)?;

    immutable_field_matches(
        existing.name.as_deref(),
        Some(identity.dpu_service_name.as_str()),
        "metadata.name",
    )?;
    immutable_field_matches(
        existing.namespace.as_deref(),
        Some(namespace),
        "metadata.namespace",
    )?;
    immutable_field_matches(
        existing.deploy_in_cluster,
        Some(false),
        "spec.deployInCluster",
    )?;
    immutable_absent(
        !existing.dpu_cluster_selector_present,
        "spec.dpuClusterSelector",
    )?;
    immutable_absent(existing.service_id.is_none(), "spec.serviceID")?;
    immutable_absent(!existing.interfaces_present, "spec.interfaces")?;
    immutable_absent(!existing.config_ports_present, "spec.configPorts")?;
    immutable_field_matches(
        existing.helm_chart.release_name.as_deref(),
        Some(identity.helm_release_name.as_str()),
        "spec.helmChart.source.releaseName",
    )?;
    let expected_node_selector = node_selector_json(&detached_node_selector_labels(&identity));
    immutable_field_matches(
        existing.service_daemon_set_node_selector.as_ref(),
        Some(&expected_node_selector),
        "spec.serviceDaemonSet.nodeSelector",
    )
}

/// Verifies the only ownership condition required before deleting a
/// deterministically named DPUService. Immutable fields are intentionally not
/// checked here: an object owned by this service must still be removable when
/// it has been otherwise modified.
pub fn verify_dpu_service_owner_label(
    existing: &DpuServiceObservation,
    extension_service_id: ExtensionServiceId,
) -> Result<(), DpuServiceOwnershipConflict> {
    (existing.labels.get(DPF_HELM_CHART_OWNER_LABEL) == Some(&extension_service_id.to_string()))
        .then_some(())
        .ok_or(DpuServiceOwnershipConflict::OwnershipLabel)
}

/// A non-sensitive reason that NICo must neither patch nor delete a live
/// DPUService.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DpuServiceOwnershipConflict {
    #[error("DPUService ownership label does not identify this extension service")]
    OwnershipLabel,
    #[error("DPUService immutable field conflicts with NICo contract: {field}")]
    ImmutableField { field: &'static str },
}

fn projected_helm_chart(
    identity: &DpfHelmChartIdentity,
    data: &DpfHelmChartServiceData,
) -> DetachedHelmChart {
    DetachedHelmChart {
        chart: data.chart_name.clone(),
        release_name: identity.helm_release_name.clone(),
        repo_url: data.repo_url.clone(),
        version: data.chart_version.clone(),
        values: data
            .values
            .as_ref()
            .map(|values| values.clone().into_iter().collect()),
    }
}

fn detached_node_selector_labels(identity: &DpfHelmChartIdentity) -> BTreeMap<String, String> {
    BTreeMap::from([(
        identity.placement_label_key.clone(),
        DPF_HELM_CHART_PLACEMENT_LABEL_VALUE.to_owned(),
    )])
}

fn node_selector_json(labels: &BTreeMap<String, String>) -> Value {
    json!({
        "nodeSelectorTerms": [{
            "matchExpressions": labels.iter().map(|(key, value)| json!({
                "key": key,
                "operator": "In",
                "values": [value],
            })).collect::<Vec<_>>(),
        }],
    })
}

fn immutable_field_matches<T: PartialEq>(
    actual: T,
    expected: T,
    field: &'static str,
) -> Result<(), DpuServiceOwnershipConflict> {
    (actual == expected)
        .then_some(())
        .ok_or(DpuServiceOwnershipConflict::ImmutableField { field })
}

fn immutable_absent(
    is_absent: bool,
    field: &'static str,
) -> Result<(), DpuServiceOwnershipConflict> {
    is_absent
        .then_some(())
        .ok_or(DpuServiceOwnershipConflict::ImmutableField { field })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    const SERVICE_ID: &str = "00000000-0000-0000-0000-000000000001";
    const NAMESPACE: &str = "dpf-operator-system";

    fn service_id() -> ExtensionServiceId {
        ExtensionServiceId::from_str(SERVICE_ID).unwrap()
    }

    fn data(values: Option<Map<String, Value>>) -> DpfHelmChartServiceData {
        DpfHelmChartServiceData {
            repo_url: "oci://registry.example.com/charts".to_owned(),
            chart_name: "tenant-service".to_owned(),
            chart_version: "1.2.3".to_owned(),
            security_privileged: true,
            values,
        }
    }

    fn observation(projected: &DetachedDpuServiceDefinition) -> DpuServiceObservation {
        DpuServiceObservation {
            name: Some(projected.name.clone()),
            namespace: Some(projected.namespace.clone()),
            labels: projected.labels.clone(),
            is_deleting: false,
            helm_chart: carbide_dpf::DpuServiceHelmChartObservation {
                repo_url: projected.helm_chart.repo_url.clone(),
                chart: Some(projected.helm_chart.chart.clone()),
                version: projected.helm_chart.version.clone(),
                release_name: Some(projected.helm_chart.release_name.clone()),
                values: projected.helm_chart.values.clone(),
            },
            deploy_in_cluster: Some(projected.deploy_in_cluster),
            dpu_cluster_selector_present: false,
            interfaces_present: false,
            paused: None,
            security_privileged: Some(projected.security_privileged),
            service_daemon_set_node_selector: Some(node_selector_json(
                &projected.node_selector_labels,
            )),
            service_id: None,
            config_ports_present: false,
        }
    }

    #[test]
    fn projection_builds_the_detached_dpu_service_contract() {
        let projected = project_dpu_service(
            service_id(),
            NAMESPACE,
            &data(Some(Map::from_iter([(
                "image".to_owned(),
                json!({"tag": "1.2.3", "repository": "registry.example.com/tenant/service"}),
            )]))),
        );

        assert_eq!(
            projected.service.name,
            "extsvc-00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(projected.service.namespace, NAMESPACE);
        assert_eq!(
            projected.service.labels.get(DPF_HELM_CHART_OWNER_LABEL),
            Some(&SERVICE_ID.to_owned())
        );
        assert!(!projected.service.deploy_in_cluster);
        assert_eq!(
            projected.service.helm_chart.release_name,
            "extsvc-00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(
            projected.service.helm_chart.values.as_ref().unwrap()["image"],
            json!({"tag": "1.2.3", "repository": "registry.example.com/tenant/service"})
        );
        assert!(projected.service.security_privileged);
        assert_eq!(
            projected.service.node_selector_labels,
            BTreeMap::from([(
                "nico/extsvc-00000000-0000-0000-0000-000000000001".to_owned(),
                "enabled".to_owned(),
            )])
        );
    }

    #[test]
    fn projection_omits_absent_values_and_all_attachment_bound_fields() {
        let projected = project_dpu_service(service_id(), NAMESPACE, &data(None));
        assert!(projected.service.helm_chart.values.is_none());
        assert!(!projected.service.deploy_in_cluster);
    }

    #[test]
    fn mutable_patch_has_no_identity_or_attachment_fields() {
        let projected = project_dpu_service(service_id(), NAMESPACE, &data(None));
        let patch = dpu_service_mutable_patch(&projected.service);

        assert_eq!(patch["spec"]["helmChart"]["values"], Value::Null);
        assert!(patch["metadata"].is_null());
        assert!(patch["spec"].get("deployInCluster").is_none());
        assert!(patch["spec"].get("serviceID").is_none());
        assert!(patch["spec"].get("interfaces").is_none());
        assert!(patch["spec"].get("configPorts").is_none());
        assert!(patch["spec"].get("dpuClusterSelector").is_none());
        assert!(patch["spec"].get("serviceDaemonSet").is_none());
        assert!(
            patch["spec"]["helmChart"]["source"]
                .get("releaseName")
                .is_none()
        );
    }

    #[test]
    fn ownership_and_immutable_contract_is_enforced_without_value_diagnostics() {
        let projected = project_dpu_service(service_id(), NAMESPACE, &data(None));
        assert_eq!(
            verify_dpu_service_ownership(&observation(&projected.service), service_id(), NAMESPACE),
            Ok(())
        );

        let mut wrong_owner = observation(&projected.service);
        wrong_owner.labels.insert(
            DPF_HELM_CHART_OWNER_LABEL.to_owned(),
            "someone-else".to_owned(),
        );
        assert_eq!(
            verify_dpu_service_ownership(&wrong_owner, service_id(), NAMESPACE),
            Err(DpuServiceOwnershipConflict::OwnershipLabel)
        );

        let mut wrong_release_name = observation(&projected.service);
        wrong_release_name.helm_chart.release_name = Some("other".to_owned());
        let conflict =
            verify_dpu_service_ownership(&wrong_release_name, service_id(), NAMESPACE).unwrap_err();
        assert_eq!(
            conflict,
            DpuServiceOwnershipConflict::ImmutableField {
                field: "spec.helmChart.source.releaseName",
            }
        );
        assert!(!conflict.to_string().contains("other"));

        let mut attached = wrong_release_name;
        attached.helm_chart.release_name =
            Some("extsvc-00000000-0000-0000-0000-000000000001".to_owned());
        attached.service_id = Some("DPF-assigned-service-id".to_owned());
        assert_eq!(
            verify_dpu_service_ownership(&attached, service_id(), NAMESPACE),
            Err(DpuServiceOwnershipConflict::ImmutableField {
                field: "spec.serviceID",
            })
        );

        let mut wrong_deployment_mode = attached;
        wrong_deployment_mode.service_id = None;
        wrong_deployment_mode.deploy_in_cluster = Some(true);
        assert_eq!(
            verify_dpu_service_ownership(&wrong_deployment_mode, service_id(), NAMESPACE),
            Err(DpuServiceOwnershipConflict::ImmutableField {
                field: "spec.deployInCluster",
            })
        );

        let mut wrong_placement = observation(&projected.service);
        wrong_placement.service_daemon_set_node_selector = Some(json!({
            "nodeSelectorTerms": [{"matchExpressions": []}],
        }));
        assert_eq!(
            verify_dpu_service_ownership(&wrong_placement, service_id(), NAMESPACE),
            Err(DpuServiceOwnershipConflict::ImmutableField {
                field: "spec.serviceDaemonSet.nodeSelector",
            })
        );
    }

    #[test]
    fn delete_ownership_check_requires_only_the_owner_label() {
        let projected = project_dpu_service(service_id(), NAMESPACE, &data(None));
        let mut modified_but_owned = observation(&projected.service);
        modified_but_owned.deploy_in_cluster = Some(true);

        // NICo must not repair an immutable conflict, but the object is still
        // ours and must remain deletable during extension-service cleanup.
        assert_eq!(
            verify_dpu_service_ownership(&modified_but_owned, service_id(), NAMESPACE),
            Err(DpuServiceOwnershipConflict::ImmutableField {
                field: "spec.deployInCluster",
            })
        );
        assert_eq!(
            verify_dpu_service_owner_label(&modified_but_owned, service_id()),
            Ok(())
        );

        modified_but_owned.labels.clear();
        assert_eq!(
            verify_dpu_service_owner_label(&modified_but_owned, service_id()),
            Err(DpuServiceOwnershipConflict::OwnershipLabel)
        );
    }
}
