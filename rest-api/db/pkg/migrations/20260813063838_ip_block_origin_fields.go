// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/uptrace/bun"
)

var errIPBlockOriginFieldsRollback = errors.New(
	"cannot roll back IPBlock origin fields because doing so would discard source identity",
)

func init() {
	Migrations.MustRegister(ipBlockOriginFieldsUpMigration, ipBlockOriginFieldsDownMigration)
}

func ipBlockOriginFieldsUpMigration(ctx context.Context, db *bun.DB) error {
	err := db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		statements := []string{
			// Fresh databases create ip_block from the current Bun model and already
			// have these columns; upgraded databases reach this migration without them.
			`ALTER TABLE ip_block
				ADD COLUMN IF NOT EXISTS origin TEXT,
				ADD COLUMN IF NOT EXISTS parent_ip_block_id UUID,
				ADD COLUMN IF NOT EXISTS site_prefix_id UUID`,
			`ALTER TABLE ip_block
				DROP CONSTRAINT IF EXISTS ip_block_parent_ip_block_id_fkey,
				ADD CONSTRAINT ip_block_parent_ip_block_id_fkey
					FOREIGN KEY (parent_ip_block_id) REFERENCES ip_block(id)`,
			`UPDATE ip_block AS child
			SET
				origin = CASE
					WHEN child.tenant_id IS NULL THEN 'SiteFabric'
					ELSE 'Allocation'
				END,
				parent_ip_block_id = CASE
					WHEN child.tenant_id IS NULL THEN NULL
					ELSE (
						SELECT ac.resource_type_id
						FROM allocation_constraint AS ac
						WHERE ac.resource_type = 'IPBlock'
							AND ac.deleted IS NULL
							AND ac.derived_resource_id = child.id
					)
				END
			WHERE child.origin IS NULL`,
			// Replace the check so rerunning the callback after a migration bookkeeping
			// failure remains safe. Deleted Allocations may outlive their constraints, so
			// only live Allocation records require a reconstructable parent.
			`ALTER TABLE ip_block
				DROP CONSTRAINT IF EXISTS ip_block_origin_fields_check,
				ADD CONSTRAINT ip_block_origin_fields_check CHECK (
					(
						(origin IS NULL AND parent_ip_block_id IS NULL AND site_prefix_id IS NULL)
						OR (
							origin IS NOT NULL
							AND (
								(origin = 'SiteFabric' AND tenant_id IS NULL AND parent_ip_block_id IS NULL AND site_prefix_id IS NULL)
								OR (
									origin = 'Allocation'
									AND tenant_id IS NOT NULL
									AND site_prefix_id IS NULL
									AND (parent_ip_block_id IS NOT NULL OR deleted IS NOT NULL)
								)
								OR (origin = 'Tenant' AND tenant_id IS NOT NULL AND parent_ip_block_id IS NULL AND site_prefix_id IS NOT NULL)
							)
						)
					)
					AND (parent_ip_block_id IS NULL OR parent_ip_block_id <> id)
				)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS ip_block_live_site_prefix_id_key
			ON ip_block (site_prefix_id)
			WHERE deleted IS NULL AND site_prefix_id IS NOT NULL`,
		}
		for _, statement := range statements {
			_, err := tx.ExecContext(ctx, statement)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Print(" [up migration] Added source fields to 'ip_block'. ")
	return nil
}

func ipBlockOriginFieldsDownMigration(_ context.Context, _ *bun.DB) error {
	// Older binaries tolerate these additive columns. Refuse schema rollback
	// because Tenant source identity cannot be reconstructed after it is dropped.
	return errIPBlockOriginFieldsRollback
}
