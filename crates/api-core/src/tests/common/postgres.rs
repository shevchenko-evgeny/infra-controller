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

use std::future::Future;

use carbide_uuid::machine::MachineId;
use model::ib::IbMembership;
use sqlx::PgPool;

/// Test-specific marker for the short query that locks one `Machine` by ID.
pub(in crate::tests) const MACHINE_BY_ID_LOCK_QUERY_MARKER: &str = "SELECT id FROM machines";

/// Test-specific marker for the visible prefix of the generated `Machine`
/// snapshot query. The suffix is longer than PostgreSQL's default
/// `pg_stat_activity.query` capture, so lock-wait tests match this prefix.
pub(in crate::tests) const MACHINE_SNAPSHOT_LOCK_QUERY_MARKER: &str = "SELECT row_to_json";

/// `wait_for_blocked_query` is a test-specific helper that waits for a query
/// containing `query_fragment` to enter a lock wait behind `blocker_pid`. It
/// returns the blocked PostgreSQL backend PID. The fragment match is literal so
/// underscores in SQL identifiers cannot act as `LIKE` wildcards.
pub(in crate::tests) async fn wait_for_blocked_query(
    pool: &PgPool,
    blocker_pid: i32,
    query_fragment: &str,
) -> i32 {
    for _ in 0..300 {
        let blocked_pid: Option<i32> = sqlx::query_scalar(
            r#"
                SELECT activity.pid
                FROM pg_stat_activity AS activity
                WHERE activity.datname = current_database()
                  AND activity.wait_event_type = 'Lock'
                  AND $1 = ANY(pg_blocking_pids(activity.pid))
                  AND strpos(lower(activity.query), lower($2)) > 0
                ORDER BY activity.pid
                LIMIT 1
            "#,
        )
        .bind(blocker_pid)
        .bind(query_fragment)
        .fetch_optional(pool)
        .await
        .expect("database lock state should be readable");
        if let Some(blocked_pid) = blocked_pid {
            return blocked_pid;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    panic!("query containing {query_fragment:?} never waited for database lock");
}

/// Test-specific helper that inserts one exact retired IB membership directly.
///
/// This deliberately bypasses the production `Machine` lock and `Instance`
/// transition and is only for seeding durable retirement state.
pub(in crate::tests) async fn insert_retired_ib_membership(
    pool: &PgPool,
    membership: &IbMembership,
) {
    sqlx::query("INSERT INTO retired_ib_memberships (fabric, pkey, guid) VALUES ($1, $2, $3)")
        .bind(&membership.fabric)
        .bind(i32::from(u16::from(membership.pkey)))
        .bind(&membership.guid)
        .execute(pool)
        .await
        .expect("retired IB membership fixture should be insertable");
}

/// Test-specific helper that proves an operation reaches the owning host
/// `Machine` lock barrier before it can touch an attached DPU `Machine`.
pub(in crate::tests) async fn run_dpu_write_with_host_lock_order_probe<F>(
    pool: &PgPool,
    host_machine_id: MachineId,
    dpu_machine_id: MachineId,
    operation: F,
) where
    F: Future<Output = ()>,
{
    let mut host_guard = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM machines WHERE id = $1 FOR UPDATE")
        .bind(host_machine_id)
        .fetch_one(host_guard.as_mut())
        .await
        .unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(host_guard.as_mut())
        .await
        .unwrap();
    let lock_order_probe = async {
        wait_for_blocked_query(pool, blocker_pid, MACHINE_BY_ID_LOCK_QUERY_MARKER).await;
        let dpu_lock = sqlx::query("SELECT id FROM machines WHERE id = $1 FOR UPDATE NOWAIT")
            .bind(dpu_machine_id)
            .fetch_one(pool)
            .await;
        host_guard.commit().await?;
        Ok::<_, sqlx::Error>(dpu_lock)
    };

    let ((), dpu_lock) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        tokio::join!(operation, lock_order_probe)
    })
    .await
    .expect("DPU write must finish without a lock-order deadlock");
    let dpu_lock = dpu_lock.expect("host Machine lock guard should commit");
    assert!(
        dpu_lock.is_ok(),
        "operation locked the DPU before its host Machine: {dpu_lock:?}"
    );
}
