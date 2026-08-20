-- Stores the latest extension-service status observations for this DPUDevice.
--
-- The JSON object is keyed by extension-service type (currently
-- `kubernetes_pod` and `dpf_helm_chart`). Each value is an
-- `InstanceExtensionServiceStatusObservation`. Each type has one
-- authoritative writer: forge-dpu-agent reports KubernetesPod workload
-- status, while the machine controller records the Stage-1 DPF Helm
-- placement result. When DPF provides per-DPU workload status, it replaces
-- the writer for `dpf_helm_chart`; it does not require another column or
-- observation shape.
--
-- This must remain separate from `network_status_observation`, which is an
-- agent-owned document replaced as a whole on every network-status report.
-- Type-scoped JSONB updates preserve observations written for other service
-- types and make instance-status derivation independent of how a type obtains
-- its status.
ALTER TABLE machines
    ADD COLUMN extension_service_status_observations jsonb NOT NULL DEFAULT '{}'::jsonb;

-- Preserve the latest KubernetesPod observation already embedded in the
-- agent-owned network-status document. New reports write this canonical source
-- entry directly, but the backfill avoids a temporary Unknown state for
-- existing instances during rollout.
UPDATE machines
SET extension_service_status_observations = jsonb_build_object(
    'kubernetes_pod',
    network_status_observation->'extension_service_observation'
)
WHERE network_status_observation->'extension_service_observation' IS NOT NULL;
