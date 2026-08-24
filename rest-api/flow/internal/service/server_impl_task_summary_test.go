// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	taskcommon "github.com/NVIDIA/infra-controller/rest-api/flow/internal/task/common"
	taskstore "github.com/NVIDIA/infra-controller/rest-api/flow/internal/task/store"
	taskdef "github.com/NVIDIA/infra-controller/rest-api/flow/internal/task/task"
	"github.com/NVIDIA/infra-controller/rest-api/flow/pkg/common/devicetypes"
	pb "github.com/NVIDIA/infra-controller/rest-api/flow/pkg/proto/v1"
)

type taskSummaryStore struct {
	taskstore.Store
	tasks   []*taskdef.Task
	rackIDs []uuid.UUID
	err     error
}

func (s *taskSummaryStore) ListNonTerminalTasksForRacks(
	_ context.Context,
	rackIDs []uuid.UUID,
) ([]*taskdef.Task, error) {
	s.rackIDs = append([]uuid.UUID{}, rackIDs...)
	return s.tasks, s.err
}

func TestPopulateTaskSummaries_StoreError(t *testing.T) {
	rackID := uuid.New()
	wantErr := errors.New("task store unavailable")
	server := &FlowServerImpl{taskStore: &taskSummaryStore{err: wantErr}}
	rack := &pb.Rack{Info: &pb.DeviceInfo{Id: &pb.UUID{Id: rackID.String()}}}

	err := server.populateTaskSummaries(context.Background(), []*pb.Rack{rack}, nil)
	assert.ErrorIs(t, err, wantErr)
}

func TestPopulateTaskSummaries_Empty(t *testing.T) {
	rack := &pb.Rack{}
	component := &pb.Component{}

	err := (&FlowServerImpl{}).populateTaskSummaries(
		context.Background(),
		[]*pb.Rack{rack},
		[]*pb.Component{component},
	)
	require.NoError(t, err)
	assert.Empty(t, rack.GetTaskSummary().GetActiveTaskIds())
	assert.Empty(t, component.GetTaskSummary().GetActiveTaskIds())
}

func TestPopulateTaskSummaries(t *testing.T) {
	rackID := uuid.New()
	componentID := uuid.New()
	rackTaskID := uuid.New()
	componentTaskID := uuid.New()
	store := &taskSummaryStore{tasks: []*taskdef.Task{
		{ID: rackTaskID, RackID: rackID},
		{
			ID:     componentTaskID,
			RackID: rackID,
			Attributes: taskcommon.TaskAttributes{ComponentsByType: map[devicetypes.ComponentType][]uuid.UUID{
				devicetypes.ComponentTypeCompute: {componentID, componentID},
			}},
		},
	}}
	server := &FlowServerImpl{taskStore: store}
	rack := &pb.Rack{Info: &pb.DeviceInfo{Id: &pb.UUID{Id: rackID.String()}}}
	component := &pb.Component{
		Info:   &pb.DeviceInfo{Id: &pb.UUID{Id: componentID.String()}},
		RackId: &pb.UUID{Id: rackID.String()},
	}

	err := server.populateTaskSummaries(
		context.Background(),
		[]*pb.Rack{rack},
		[]*pb.Component{component},
	)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{rackID}, store.rackIDs)
	assert.Equal(t, []string{rackTaskID.String(), componentTaskID.String()}, uuidStrings(rack.GetTaskSummary().GetActiveTaskIds()))
	assert.Equal(t, []string{componentTaskID.String()}, uuidStrings(component.GetTaskSummary().GetActiveTaskIds()))
}

func uuidStrings(ids []*pb.UUID) []string {
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		result = append(result, id.GetId())
	}
	return result
}
