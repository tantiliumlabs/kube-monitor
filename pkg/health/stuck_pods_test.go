package health

import (
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var testNow = time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

func oldPod(name string, age time.Duration, status v1.PodStatus) v1.Pod {
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, CreationTimestamp: metav1.NewTime(testNow.Add(-age))},
		Status:     status,
	}
}

func waiting(reason string) []v1.ContainerStatus {
	return []v1.ContainerStatus{{State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: reason}}}}
}

func TestFindStuckPods(t *testing.T) {
	const old, young = time.Hour, time.Minute
	deleting := oldPod("deleting", old, v1.PodStatus{Phase: v1.PodPending})
	now := metav1.NewTime(testNow)
	deleting.DeletionTimestamp = &now

	pods := []v1.Pod{
		oldPod("running", old, v1.PodStatus{Phase: v1.PodRunning}),
		oldPod("finished", old, v1.PodStatus{Phase: v1.PodSucceeded}),
		oldPod("crash", old, v1.PodStatus{Phase: v1.PodRunning, ContainerStatuses: waiting("CrashLoopBackOff")}),
		oldPod("young-pending", young, v1.PodStatus{Phase: v1.PodPending}),
		oldPod("young-pull", young, v1.PodStatus{Phase: v1.PodPending, ContainerStatuses: waiting("ErrImagePull")}),
		deleting,
		oldPod("no-secret", old, v1.PodStatus{Phase: v1.PodPending, InitContainerStatuses: waiting("CreateContainerConfigError")}),
		oldPod("bad-image", old, v1.PodStatus{Phase: v1.PodRunning, ContainerStatuses: waiting("ImagePullBackOff")}),
		oldPod("no-room", old, v1.PodStatus{Phase: v1.PodPending, Conditions: []v1.PodCondition{
			{Type: v1.PodScheduled, Status: v1.ConditionFalse, Reason: "Unschedulable"},
		}}),
		oldPod("pending", old, v1.PodStatus{Phase: v1.PodPending}),
	}

	got := map[string]string{}
	for _, p := range findStuckPods(pods, testNow) {
		got[p.Name] = p.Reason
	}
	want := map[string]string{
		"no-secret": "CreateContainerConfigError",
		"bad-image": "ImagePullBackOff",
		"no-room":   "Unschedulable",
		"pending":   "Pending",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for name, reason := range want {
		if got[name] != reason {
			t.Errorf("%s: got reason %q, want %q", name, got[name], reason)
		}
	}
}

func stuck(n int) []StuckPod {
	pods := make([]StuckPod, n)
	for i := range pods {
		pods[i] = StuckPod{Namespace: "ns", Name: "pod", Reason: "Unschedulable"}
	}
	return pods
}

func TestStuckPodsLowerHealthScoreUpToACap(t *testing.T) {
	score := func(n int) int {
		h := &ClusterHealth{}
		h.ControlPlaneStatus.OverallHealthy = true
		h.ControlPlaneStatus.APIServerHealthy = true
		h.PodStatus.StuckPods = stuck(n)
		return calculateHealthScore(h)
	}
	if got := score(0); got != 100 {
		t.Errorf("no stuck pods: score %d", got)
	}
	if got := score(2); got != 95 {
		t.Errorf("2 stuck pods: score %d, want 95", got)
	}
	if got := score(50); got != 75 {
		t.Errorf("50 stuck pods: score %d, want the 25 point cap", got)
	}
}

func TestStuckPodsMessageNamesAFewAndCountsTheRest(t *testing.T) {
	msg := stuckPodsMessage(stuck(7))
	if !strings.HasPrefix(msg, "7 pods can't start: ns/pod (Unschedulable)") || !strings.HasSuffix(msg, "and 2 more") {
		t.Errorf("message: %s", msg)
	}
	if strings.Contains(stuckPodsMessage(stuck(2)), "more") {
		t.Errorf("message for 2 pods mentions more: %s", stuckPodsMessage(stuck(2)))
	}
}
