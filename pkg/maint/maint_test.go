package maint

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

func TestPreFilter(t *testing.T) {
	m := &Maint{logger: klog.Background()}
	state := &testCycleState{data: make(map[fwk.StateKey]fwk.StateData)}
	pod := &v1.Pod{}
	previous := &PreFilterState{timestamp: time.Unix(0, 0)}
	state.Write(preFilterStateKey, previous)

	before := time.Now()
	result, status := m.PreFilter(context.Background(), state, pod, nil)
	after := time.Now()
	if !status.IsSuccess() {
		t.Fatalf("PreFilter returned %v", status)
	}
	if result != nil {
		t.Fatalf("PreFilter restricted candidate nodes: %v", result)
	}
	data, err := state.Read(preFilterStateKey)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := data.(*PreFilterState)
	if !ok {
		t.Fatalf("stored state has type %T, want *PreFilterState", data)
	}
	if s.timestamp.Before(before) || s.timestamp.After(after) {
		t.Fatalf("timestamp %v is outside call interval [%v, %v]", s.timestamp, before, after)
	}
	if previous.timestamp != time.Unix(0, 0) {
		t.Fatal("PreFilter mutated the previous attempt's state")
	}
}

func TestFilter(t *testing.T) {
	// A fixed past timestamp makes using time.Now() in Filter observable.
	now := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	node := func(start string) *v1.Node {
		return &v1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:        "worker",
			Annotations: map[string]string{maintAnnotation: start},
		}}
	}
	tests := []struct {
		name         string
		node         *v1.Node
		missingState bool
		wrongState   bool
		want         fwk.Code
	}{
		{name: "no maintenance", node: &v1.Node{}, want: fwk.Success},
		{name: "more than 30 minutes", node: node(now.Add(30*time.Minute + time.Second).Format(time.RFC3339)), want: fwk.Success},
		{name: "exactly 30 minutes", node: node(now.Add(30 * time.Minute).Format(time.RFC3339)), want: fwk.UnschedulableAndUnresolvable},
		{name: "less than 30 minutes", node: node(now.Add(30*time.Minute - time.Second).Format(time.RFC3339)), want: fwk.UnschedulableAndUnresolvable},
		{name: "maintenance starts now", node: node(now.Format(time.RFC3339)), want: fwk.UnschedulableAndUnresolvable},
		{name: "maintenance already started", node: node(now.Add(-time.Minute).Format(time.RFC3339)), want: fwk.UnschedulableAndUnresolvable},
		{name: "timezone offset", node: node("2020-01-01T13:30:00+01:00"), want: fwk.UnschedulableAndUnresolvable},
		{name: "invalid timestamp", node: node("tomorrow"), want: fwk.Error},
		{name: "empty timestamp", node: node(""), want: fwk.Error},
		{name: "missing node", want: fwk.Error},
		{name: "missing PreFilter state", node: node(now.Add(time.Hour).Format(time.RFC3339)), missingState: true, want: fwk.Error},
		{name: "wrong state type", node: node(now.Add(time.Hour).Format(time.RFC3339)), wrongState: true, want: fwk.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &testCycleState{data: make(map[fwk.StateKey]fwk.StateData)}
			if tt.wrongState {
				state.Write(preFilterStateKey, &testStateData{})
			} else if !tt.missingState {
				state.Write(preFilterStateKey, &PreFilterState{timestamp: now})
			}
			m := &Maint{logger: klog.Background()}
			status := m.Filter(context.Background(), state, &v1.Pod{}, &testNodeInfo{node: tt.node})
			if status.Code() != tt.want {
				t.Fatalf("Filter returned %v (%s), want %v", status.Code(), status.Message(), tt.want)
			}
		})
	}
}

func TestScore(t *testing.T) {
	now := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	node := func(start string) *v1.Node {
		return &v1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:        "worker",
			Annotations: map[string]string{maintAnnotation: start},
		}}
	}
	tests := []struct {
		name         string
		node         *v1.Node
		missingState bool
		wrongState   bool
		wantScore    int64
		wantStatus   fwk.Code
	}{
		{name: "no maintenance", node: &v1.Node{}, wantScore: 100, wantStatus: fwk.Success},
		{name: "past maintenance clamps to zero", node: node(now.Add(-time.Hour).Format(time.RFC3339)), wantStatus: fwk.Success},
		{name: "below cutoff clamps to zero", node: node(now.Add(15 * time.Minute).Format(time.RFC3339)), wantStatus: fwk.Success},
		{name: "30 minute cutoff", node: node(now.Add(30 * time.Minute).Format(time.RFC3339)), wantStatus: fwk.Success},
		{name: "one hour truncates fractional score", node: node(now.Add(time.Hour).Format(time.RFC3339)), wantScore: 9, wantStatus: fwk.Success},
		{name: "midpoint", node: node(now.Add(3*time.Hour + 15*time.Minute).Format(time.RFC3339)), wantScore: 50, wantStatus: fwk.Success},
		{name: "just below six hours", node: node(now.Add(6*time.Hour - time.Second).Format(time.RFC3339)), wantScore: 99, wantStatus: fwk.Success},
		{name: "six hours", node: node(now.Add(6 * time.Hour).Format(time.RFC3339)), wantScore: 100, wantStatus: fwk.Success},
		{name: "beyond six hours clamps to 100", node: node(now.Add(12 * time.Hour).Format(time.RFC3339)), wantScore: 100, wantStatus: fwk.Success},
		{name: "timezone offset", node: node("2020-01-01T16:15:00+01:00"), wantScore: 50, wantStatus: fwk.Success},
		{name: "invalid timestamp", node: node("tomorrow"), wantStatus: fwk.Error},
		{name: "empty timestamp", node: node(""), wantStatus: fwk.Error},
		{name: "missing node", wantStatus: fwk.Error},
		{name: "missing PreFilter state", node: node(now.Add(time.Hour).Format(time.RFC3339)), missingState: true, wantStatus: fwk.Error},
		{name: "wrong state type", node: node(now.Add(time.Hour).Format(time.RFC3339)), wrongState: true, wantStatus: fwk.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &testCycleState{data: make(map[fwk.StateKey]fwk.StateData)}
			if tt.wrongState {
				state.Write(preFilterStateKey, &testStateData{})
			} else if !tt.missingState {
				state.Write(preFilterStateKey, &PreFilterState{timestamp: now})
			}
			m := &Maint{logger: klog.Background()}
			score, status := m.Score(context.Background(), state, &v1.Pod{}, &testNodeInfo{node: tt.node})
			if status.Code() != tt.wantStatus {
				t.Fatalf("Score returned status %v (%s), want %v", status.Code(), status.Message(), tt.wantStatus)
			}
			if score != tt.wantScore {
				t.Errorf("Score returned %d, want %d", score, tt.wantScore)
			}
		})
	}
}

type testCycleState struct {
	fwk.CycleState
	data map[fwk.StateKey]fwk.StateData
}

func (s *testCycleState) Read(key fwk.StateKey) (fwk.StateData, error) {
	data, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("state %q not found", key)
	}
	return data, nil
}

func (s *testCycleState) Write(key fwk.StateKey, data fwk.StateData) {
	s.data[key] = data
}

type testNodeInfo struct {
	fwk.NodeInfo
	node *v1.Node
}

func (n *testNodeInfo) Node() *v1.Node { return n.node }

type testStateData struct{}

func (s *testStateData) Clone() fwk.StateData { return s }
