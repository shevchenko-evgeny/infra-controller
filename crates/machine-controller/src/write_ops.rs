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

use std::net::IpAddr;

use async_trait::async_trait;
use carbide_uuid::machine::MachineId;
use chrono::{DateTime, Utc};
use config_version::ConfigVersion;
use health_report::{HealthReport, HealthReportApplyMode};
use model::machine::{MachineLastRebootRequested, MachineLastRebootRequestedMode};
use sqlx::PgTransaction;
use state_controller::db_write_batch::WriteOp;
use state_controller::state_handler::StateHandlerError;

/// A deferred-write operation for use in [`MachineStateHandler`].
///
/// Operations that are appropriate here are ones where:
///
/// - The operation can be deferred to the end without worrying about whether it will succeed. This
///   means operations mustn't have preconditions other than there being a valid machine ID.
///   For example, bumping timestamps or clearing errors.
/// - We can't open a transaction and do the write operation directly because we have to a
///   long-running operation next (like rebooting a host) and we don't want to hold the transaction
///   across an await point.
///
/// New deferred mutation variants should remain exceptional. Prefer structuring
/// state-handler work in three phases:
///
/// 1. DB read: Get data needed from the database with `DbReader` or `PgPool`,
///    without requiring a transaction.
/// 2. External operations: Await non-database work.
/// 3. DB write: Perform the writes in a transaction, then return it through
///    [`StateHandlerOutcome::with_txn`].
///
/// `MachineWriteOp` exists for writes that must be registered before slow
/// external work and for transaction-ordering barriers shared by those writes.
/// Most states should use the three-phase structure instead of adding a variant.
/// [`MachineWriteOp::LockMachine`] is coordination-only: it establishes the
/// host-first row-lock order within an existing deferred-write batch and does
/// not represent a domain mutation. It cannot reorder writes a handler already
/// performed in a transaction returned through [`StateHandlerOutcome::with_txn`].
pub enum MachineWriteOp {
    /// Acquires the host `Machine` row lock for the remainder of the
    /// deferred-write transaction.
    ///
    /// Queue this before any operation in the same batch that may write an
    /// attached DPU `Machine` or the host's `Instance`. A handler-owned
    /// transaction must acquire the host lock before making either write; the
    /// deferred batch runs afterward. This barrier does not mutate the host row.
    LockMachine {
        /// The host `Machine` whose row is locked.
        machine_id: MachineId,
    },
    UpdateRebootRequestedTime {
        machine_id: MachineId,
        mode: MachineLastRebootRequestedMode,
        time: DateTime<Utc>,
    },
    PersistMachineHealthHistory {
        machine_id: MachineId,
        health_report: HealthReport,
    },
    ResetHostReprovisioningRequest {
        machine_id: MachineId,
        clear_reset: bool,
    },
    UpdateDpuReprovisionStartTime {
        machine_id: MachineId,
        time: DateTime<Utc>,
    },
    UpdateHostReprovisionStartTime {
        machine_id: MachineId,
        time: DateTime<Utc>,
    },
    ClearFailureDetails {
        machine_id: MachineId,
    },
    UpdateRestartVerificationStatus {
        machine_id: MachineId,
        current_reboot: MachineLastRebootRequested,
        verified: Option<bool>,
        attempts: i32,
    },
    UpdateFirmwareVersionByMachineId {
        machine_id: MachineId,
        bmc_version: String,
        bios_version: String,
    },
    SetTopologyUpdateNeeded {
        machine_id: MachineId,
        value: bool,
    },
    SetCustomPxeRebootRequested {
        machine_id: MachineId,
        requested: bool,
    },
    InsertMachineHealthReport {
        machine_id: MachineId,
        mode: HealthReportApplyMode,
        health_report: HealthReport,
    },
    ReExploreIfVersionMatches {
        address: IpAddr,
        version: ConfigVersion,
    },
    UseCustomIpxeOnNextBoot {
        machine_id: MachineId,
        boot_with_custom_ipxe: bool,
    },
}

#[async_trait]
impl WriteOp for MachineWriteOp {
    async fn apply<'a, 't: 'a>(
        self: Box<Self>,
        txn: &'a mut PgTransaction<'t>,
    ) -> Result<(), StateHandlerError> {
        use MachineWriteOp::*;
        match *self {
            LockMachine { machine_id } => {
                db::machine::lock_by_id(txn.as_mut(), &machine_id).await?
            }
            UpdateRebootRequestedTime {
                machine_id,
                mode,
                time,
            } => {
                db::machine::update_reboot_requested_explicit_time(&machine_id, txn, mode, time)
                    .await?
            }
            PersistMachineHealthHistory {
                machine_id,
                health_report,
            } => {
                db::health_history::persist(
                    txn,
                    db::health_history::HealthHistoryTableId::Machine,
                    &machine_id,
                    &health_report,
                )
                .await?
            }
            ResetHostReprovisioningRequest {
                machine_id,
                clear_reset,
            } => {
                db::host_machine_update::reset_host_reprovisioning_request(
                    txn,
                    &machine_id,
                    clear_reset,
                )
                .await?
            }
            UpdateDpuReprovisionStartTime { machine_id, time } => {
                db::machine::update_dpu_reprovision_explicit_start_time(&machine_id, time, txn)
                    .await?
            }
            UpdateHostReprovisionStartTime { machine_id, time } => {
                db::machine::update_host_reprovision_explicit_start_time(&machine_id, time, txn)
                    .await?
            }
            ClearFailureDetails { machine_id } => {
                db::machine::clear_failure_details(&machine_id, txn).await?
            }
            UpdateRestartVerificationStatus {
                machine_id,
                current_reboot,
                verified,
                attempts,
            } => {
                db::machine::update_restart_verification_status(
                    &machine_id,
                    current_reboot,
                    verified,
                    attempts,
                    txn,
                )
                .await?
            }
            UpdateFirmwareVersionByMachineId {
                machine_id,
                bmc_version,
                bios_version,
            } => {
                db::machine_topology::update_firmware_version_by_machine_id(
                    txn,
                    &machine_id,
                    &bmc_version,
                    &bios_version,
                )
                .await?
            }
            SetTopologyUpdateNeeded { machine_id, value } => {
                db::machine_topology::set_topology_update_needed(txn, &machine_id, value).await?
            }
            SetCustomPxeRebootRequested {
                machine_id,
                requested,
            } => db::instance::set_custom_pxe_reboot_requested(&machine_id, requested, txn).await?,
            InsertMachineHealthReport {
                machine_id,
                mode,
                health_report,
            } => {
                db::machine::insert_health_report(txn, &machine_id, mode, &health_report, false)
                    .await?
            }
            ReExploreIfVersionMatches { address, version } => {
                db::explored_endpoints::re_explore_if_version_matches(address, version, txn)
                    .await?;
            }
            UseCustomIpxeOnNextBoot {
                machine_id,
                boot_with_custom_ipxe,
            } => {
                db::instance::use_custom_ipxe_on_next_boot(&machine_id, boot_with_custom_ipxe, txn)
                    .await?;
            }
        };
        Ok(())
    }
}
