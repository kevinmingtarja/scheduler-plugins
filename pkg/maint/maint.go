package maint

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

type PreFilterState struct {
	timestamp time.Time
}

func (s *PreFilterState) Clone() fwk.StateData {
	return s
}

const (
	Name = "Maint"

	preFilterStateKey = "PreFilter" + Name

	maintAnnotation           = "scheduling.example.com/maintenance-start"
	minimumSchedulableMinutes = 30
)

// Maint is a plugin that is aware of system maintenance windows.
// Inspired by Slurm's https://slurm.schedmd.com/reservations.html#maint.
type Maint struct {
	logger           klog.Logger
	frameworkHandler fwk.Handle
}

var _ fwk.PreFilterPlugin = &Maint{}
var _ fwk.FilterPlugin = &Maint{}
var _ fwk.ScorePlugin = &Maint{}
var _ fwk.EnqueueExtensions = &Maint{}

func New(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	lh := klog.FromContext(ctx).WithValues("plugin", Name)
	lh.V(5).Info("creating new maint plugin")

	plugin := &Maint{
		logger:           lh,
		frameworkHandler: handle,
	}

	return plugin, nil
}

func (m *Maint) Name() string {
	return Name
}

func (m *Maint) EventsToRegister(context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeAnnotation}},
	}, nil
}

func (m *Maint) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	lh := klog.FromContext(klog.NewContext(ctx, m.logger)).WithValues("ExtensionPoint", "PreFilter")
	preFilterState := &PreFilterState{
		timestamp: time.Now(),
	}
	lh.V(5).Info("PreFilter state", "timestamp", preFilterState.timestamp)
	state.Write(preFilterStateKey, preFilterState)
	// nil means all nodes are eligible
	return nil, fwk.NewStatus(fwk.Success, "")
}

func (m *Maint) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

func (m *Maint) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	timeLeft, scheduled, status := timeUntilMaintenance(state, nodeInfo.Node())
	if status != nil {
		return status
	}
	if !scheduled {
		return fwk.NewStatus(fwk.Success, "no scheduled maintenance")
	}
	if timeLeft.Minutes() <= minimumSchedulableMinutes {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node will start maintenance in %f minutes", timeLeft.Minutes()))
	}

	return nil
}

func (m *Maint) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	timeLeft, scheduled, status := timeUntilMaintenance(state, nodeInfo.Node())
	if status != nil {
		return fwk.MinScore, status
	}
	if !scheduled {
		return fwk.MaxScore, fwk.NewStatus(fwk.Success, "no scheduled maintenance")
	}

	cutoff := time.Duration(minimumSchedulableMinutes) * time.Minute
	fullScoreAfter := 6 * time.Hour
	fraction := float64(timeLeft-cutoff) / float64(fullScoreAfter-cutoff)
	fraction = max(0.0, min(1.0, fraction))
	score := int64(fraction * float64(fwk.MaxScore))
	return score, nil
}

func timeUntilMaintenance(state fwk.CycleState, node *v1.Node) (timeLeft time.Duration, scheduled bool, status *fwk.Status) {
	if node == nil {
		return 0, false, fwk.NewStatus(fwk.Error, "node not found")
	}

	maintStartStr, ok := node.Annotations[maintAnnotation]
	if !ok {
		return 0, false, nil
	}
	maintStart, err := time.Parse(time.RFC3339, maintStartStr)
	if err != nil {
		return 0, true, fwk.NewStatus(fwk.Error, fmt.Sprintf("error parsing time %s", maintStartStr))
	}

	sd, err := state.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return 0, true, fwk.NewStatus(fwk.Error, fmt.Sprintf("error reading %q from cycleState: %v", preFilterStateKey, err))
	}
	s, ok := sd.(*PreFilterState)
	if !ok {
		return 0, true, fwk.NewStatus(fwk.Error, "error converting to PreFilterState")
	}

	return maintStart.Sub(s.timestamp), true, nil
}

// ScoreExtensions returns a ScoreExtensions interface if it implements one, or nil if does not.
func (m *Maint) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}
