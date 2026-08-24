// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/NVIDIA/infra-controller/rest-api/flow/internal/converter/protobuf"
	pb "github.com/NVIDIA/infra-controller/rest-api/flow/pkg/proto/v1"
)

// populateTaskSummaries attaches one batched snapshot of non-terminal task IDs
// to rack and component protobufs. Rack summaries include every task persisted
// against the rack; component summaries include only tasks whose attributes
// explicitly target that component.
func (rs *FlowServerImpl) populateTaskSummaries(
	ctx context.Context,
	racks []*pb.Rack,
	components []*pb.Component,
) error {
	rackIDs := make([]uuid.UUID, 0, len(racks)+len(components))
	seenRackIDs := make(map[uuid.UUID]struct{}, len(racks)+len(components))

	addRackID := func(id uuid.UUID) {
		if id == uuid.Nil {
			return
		}
		if _, exists := seenRackIDs[id]; exists {
			return
		}
		seenRackIDs[id] = struct{}{}
		rackIDs = append(rackIDs, id)
	}

	for _, r := range racks {
		if r == nil {
			continue
		}
		r.TaskSummary = &pb.TaskSummary{ActiveTaskIds: []*pb.UUID{}}
		addRackID(protobuf.UUIDFrom(r.GetInfo().GetId()))
	}
	for _, c := range components {
		if c == nil {
			continue
		}
		c.TaskSummary = &pb.TaskSummary{ActiveTaskIds: []*pb.UUID{}}
		addRackID(protobuf.UUIDFrom(c.GetRackId()))
	}

	// Many focused service tests construct FlowServerImpl without a store.
	// Preserve their inventory-only setup while production instances always
	// populate summaries through the configured store.
	if len(rackIDs) == 0 || rs.taskStore == nil {
		return nil
	}

	tasks, err := rs.taskStore.ListNonTerminalTasksForRacks(ctx, rackIDs)
	if err != nil {
		return err
	}

	rackTaskIDs := make(map[uuid.UUID][]*pb.UUID, len(rackIDs))
	componentTaskIDs := make(map[uuid.UUID][]*pb.UUID)
	for _, task := range tasks {
		if task == nil || task.ID == uuid.Nil {
			continue
		}
		rackTaskIDs[task.RackID] = append(
			rackTaskIDs[task.RackID],
			protobuf.UUIDTo(task.ID),
		)

		seenComponents := make(map[uuid.UUID]struct{})
		for _, componentID := range task.Attributes.AllComponentUUIDs() {
			if componentID == uuid.Nil {
				continue
			}
			if _, exists := seenComponents[componentID]; exists {
				continue
			}
			seenComponents[componentID] = struct{}{}
			componentTaskIDs[componentID] = append(
				componentTaskIDs[componentID],
				protobuf.UUIDTo(task.ID),
			)
		}
	}

	for _, r := range racks {
		if r == nil {
			continue
		}
		rackID := protobuf.UUIDFrom(r.GetInfo().GetId())
		r.TaskSummary.ActiveTaskIds = append([]*pb.UUID{}, rackTaskIDs[rackID]...)
	}
	for _, c := range components {
		if c == nil {
			continue
		}
		componentID := protobuf.UUIDFrom(c.GetInfo().GetId())
		c.TaskSummary.ActiveTaskIds = append([]*pb.UUID{}, componentTaskIDs[componentID]...)
	}

	return nil
}
