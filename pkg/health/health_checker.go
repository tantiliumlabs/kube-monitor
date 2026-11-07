package health

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

// ClusterHealth represents overall cluster health status
type ClusterHealth struct {
	Timestamp          time.Time                  `json:"timestamp"`
	NodeStatus         NodeHealthStatus           `json:"nodeStatus"`
	PodStatus          PodHealthStatus            `json:"podStatus"`
	ControlPlaneStatus ControlPlaneStatus         `json:"controlPlaneStatus"`
	NetworkStatus      NetworkStatus              `json:"networkStatus"`
	ResourceUsage      ResourceUsageStatus        `json:"resourceUsage"`
	ComponentStatuses  []ComponentStatus          `json:"componentStatuses"`
	NamespaceHealth    map[string]NamespaceHealth `json:"namespaceHealth"`
	HealthScore        int                        `json:"healthScore"` // 0-100
	Issues             []HealthIssue              `json:"issues"`
	// CheckErrors contains non-fatal errors from individual sub-checks, keyed
	// by check name.  A non-empty map means the report is partial.
	CheckErrors map[string]string `json:"checkErrors,omitempty"`
}

// NodeHealthStatus contains node health information
type NodeHealthStatus struct {
	TotalNodes              int                 `json:"totalNodes"`
	ReadyNodes              int                 `json:"readyNodes"`
	MemoryPressureNodes     int                 `json:"memoryPressureNodes"`
	DiskPressureNodes       int                 `json:"diskPressureNodes"`
	PIDPressureNodes        int                 `json:"pidPressureNodes"`
	NetworkUnavailableNodes int                 `json:"networkUnavailableNodes"`
	NodeConditions          map[string][]string `json:"nodeConditions"` // Node name -> conditions
	AverageLoad             float64             `json:"averageLoad"`
}

// PodHealthStatus contains pod health information
type PodHealthStatus struct {
	TotalPods        int            `json:"totalPods"`
	RunningPods      int            `json:"runningPods"`
	PendingPods      int            `json:"pendingPods"`
	SucceededPods    int            `json:"succeededPods"`
	FailedPods       int            `json:"failedPods"`
	UnknownPods      int            `json:"unknownPods"`
	RestartingPods   int            `json:"restartingPods"`
	PodsPerNode      map[string]int `json:"podsPerNode"`
	CrashLoopingPods []string       `json:"crashLoopingPods"`
	// StuckPods can't start and aren't crash-looping: unschedulable, missing
	// a Secret, unable to pull an image. See findStuckPods.
	StuckPods []StuckPod `json:"stuckPods"`
}

// StuckPod is a pod that has been unable to start for longer than a normal
// rollout or node start would explain.
type StuckPod struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	Reason    string    `json:"reason"`
	Since     time.Time `json:"since"`
}

// ControlPlaneStatus contains control plane health information
type ControlPlaneStatus struct {
	APIServerHealthy  bool    `json:"apiServerHealthy"`
	ControllerHealthy bool    `json:"controllerHealthy"`
	SchedulerHealthy  bool    `json:"schedulerHealthy"`
	EtcdHealthy       bool    `json:"etcdHealthy"`
	CoreDNSHealthy    bool    `json:"coreDNSHealthy"`
	OverallHealthy    bool    `json:"overallHealthy"`
	APIServerLatency  float64 `json:"apiServerLatency"` // in milliseconds
}

// NetworkStatus contains network health information
type NetworkStatus struct {
	CNIHealthy              bool `json:"cniHealthy"`
	DNSResolutionOK         bool `json:"dnsResolutionOK"`
	ServiceEndpointsHealthy bool `json:"serviceEndpointsHealthy"`
	IngressHealthy          bool `json:"ingressHealthy"`
	NetworkPoliciesCount    int  `json:"networkPoliciesCount"`
}

// ResourceUsageStatus contains resource usage information
type ResourceUsageStatus struct {
	ClusterCPUUsage     float64 `json:"clusterCPUUsage"`     // percentage
	ClusterMemoryUsage  float64 `json:"clusterMemoryUsage"`  // percentage
	ClusterStorageUsage float64 `json:"clusterStorageUsage"` // percentage
	// Memory in bytes, since the percentage's denominator changes with the
	// number of nodes and so can't be turned back into bytes later.
	ClusterMemoryUsedBytes        int64    `json:"clusterMemoryUsedBytes"`
	ClusterMemoryAllocatableBytes int64    `json:"clusterMemoryAllocatableBytes"`
	HighCPUNodes                  []string `json:"highCPUNodes"`
	HighMemoryNodes               []string `json:"highMemoryNodes"`
	LowResourceNodes              []string `json:"lowResourceNodes"`
	HighUsageNamespaces           []string `json:"highUsageNamespaces"`
}

// ComponentStatus represents a cluster component's health
type ComponentStatus struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
	Message string `json:"message,omitempty"`
	Version string `json:"version,omitempty"`
}

// NamespaceHealth contains health information for a namespace
type NamespaceHealth struct {
	PodStatus        PodHealthStatus     `json:"podStatus"`
	DeploymentStatus DeploymentStatus    `json:"deploymentStatus"`
	ServiceStatus    ServiceStatus       `json:"serviceStatus"`
	ResourceUsage    ResourceUsageStatus `json:"resourceUsage"`
	HealthScore      int                 `json:"healthScore"` // 0-100
}

// DeploymentStatus contains deployment health information
type DeploymentStatus struct {
	TotalDeployments       int `json:"totalDeployments"`
	HealthyDeployments     int `json:"healthyDeployments"`
	ProgressingDeployments int `json:"progressingDeployments"`
	FailedDeployments      int `json:"failedDeployments"`
}

// ServiceStatus contains service health information
type ServiceStatus struct {
	TotalServices            int `json:"totalServices"`
	ServicesWithEndpoints    int `json:"servicesWithEndpoints"`
	ServicesWithoutEndpoints int `json:"servicesWithoutEndpoints"`
}

// HealthIssue represents a detected health issue
type HealthIssue struct {
	Severity   string    `json:"severity"` // "critical", "warning", "info"
	Resource   string    `json:"resource"`
	Namespace  string    `json:"namespace,omitempty"`
	Name       string    `json:"name,omitempty"`
	Message    string    `json:"message"`
	Timestamp  time.Time `json:"timestamp"`
	Suggestion string    `json:"suggestion,omitempty"`
}

// GetClusterHealth performs a comprehensive health check of the Kubernetes cluster
func GetClusterHealth(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	metricsClient *metricsv.Clientset,
) (*ClusterHealth, error) {
	health := &ClusterHealth{
		Timestamp:       time.Now(),
		NamespaceHealth: make(map[string]NamespaceHealth),
		Issues:          make([]HealthIssue, 0),
		CheckErrors:     make(map[string]string),
	}

	// Node and pod checks are required — a failure here means we cannot produce
	// a meaningful report at all.
	if err := checkNodeHealth(ctx, clientset, &health.NodeStatus); err != nil {
		return nil, fmt.Errorf("node health check failed: %w", err)
	}
	if err := checkPodHealth(ctx, clientset, &health.PodStatus); err != nil {
		return nil, fmt.Errorf("pod health check failed: %w", err)
	}

	// The remaining checks are best-effort: failures are recorded in CheckErrors
	// so callers can distinguish a complete report from a partial one.
	if err := checkControlPlaneHealth(ctx, clientset, &health.ControlPlaneStatus); err != nil {
		log.Printf("Control plane health check failed: %v", err)
		health.CheckErrors["controlPlane"] = err.Error()
	}
	if err := checkNetworkHealth(ctx, clientset, &health.NetworkStatus); err != nil {
		log.Printf("Network health check failed: %v", err)
		health.CheckErrors["network"] = err.Error()
	}
	if err := checkResourceUsage(ctx, clientset, metricsClient, &health.ResourceUsage); err != nil {
		log.Printf("Resource usage check failed: %v", err)
		health.CheckErrors["resourceUsage"] = err.Error()
	}
	if err := checkComponentStatuses(ctx, clientset, &health.ComponentStatuses); err != nil {
		log.Printf("Component status check failed: %v", err)
		health.CheckErrors["componentStatuses"] = err.Error()
	}
	if err := checkNamespaceHealth(ctx, clientset, metricsClient, health); err != nil {
		log.Printf("Namespace health check failed: %v", err)
		health.CheckErrors["namespaceHealth"] = err.Error()
	}

	if len(health.CheckErrors) == 0 {
		health.CheckErrors = nil // omitempty: omit from JSON when all checks passed
	}

	// Identify health issues
	identifyHealthIssues(health)

	// Calculate overall health score
	health.HealthScore = calculateHealthScore(health)

	return health, nil
}

// checkNodeHealth checks the health status of all nodes
func checkNodeHealth(ctx context.Context, clientset *kubernetes.Clientset, status *NodeHealthStatus) error {
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	status.TotalNodes = len(nodes.Items)
	status.NodeConditions = make(map[string][]string)
	totalLoad := 0.0

	for _, node := range nodes.Items {
		nodeConditions := make([]string, 0)

		for _, condition := range node.Status.Conditions {
			if condition.Status == v1.ConditionTrue {
				nodeConditions = append(nodeConditions, string(condition.Type))

				switch condition.Type {
				case v1.NodeReady:
					status.ReadyNodes++
				case v1.NodeMemoryPressure:
					status.MemoryPressureNodes++
				case v1.NodeDiskPressure:
					status.DiskPressureNodes++
				case v1.NodePIDPressure:
					status.PIDPressureNodes++
				case v1.NodeNetworkUnavailable:
					status.NetworkUnavailableNodes++
				}
			}
		}

		status.NodeConditions[node.Name] = nodeConditions

		// Get node load (simplified)
		for _, metric := range node.Status.Allocatable {
			totalLoad += float64(metric.MilliValue()) / 1000
		}
	}

	if status.TotalNodes > 0 {
		status.AverageLoad = totalLoad / float64(status.TotalNodes)
	}

	return nil
}

// checkPodHealth checks the health status of all pods
func checkPodHealth(ctx context.Context, clientset *kubernetes.Clientset, status *PodHealthStatus) error {
	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	status.TotalPods = len(pods.Items)
	status.PodsPerNode = make(map[string]int)
	status.CrashLoopingPods = make([]string, 0)

	const restartRecentWindow = 15 * time.Minute

	for _, pod := range pods.Items {
		// Update pod count per node
		nodeName := pod.Spec.NodeName
		if nodeName != "" {
			if _, exists := status.PodsPerNode[nodeName]; !exists {
				status.PodsPerNode[nodeName] = 0
			}
			status.PodsPerNode[nodeName]++
		}

		// Update pod phase counts
		switch pod.Status.Phase {
		case v1.PodRunning:
			status.RunningPods++
		case v1.PodPending:
			status.PendingPods++
		case v1.PodSucceeded:
			status.SucceededPods++
		case v1.PodFailed:
			status.FailedPods++
		default:
			status.UnknownPods++
		}

		// Check for restarting pods (recent restarts per pod)
		restartedRecently := false
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.RestartCount > 0 {
				if containerStatus.LastTerminationState.Terminated != nil {
					finishedAt := containerStatus.LastTerminationState.Terminated.FinishedAt.Time
					if time.Since(finishedAt) <= restartRecentWindow {
						restartedRecently = true
					}
				}
				if containerStatus.State.Waiting != nil &&
					containerStatus.State.Waiting.Reason == "CrashLoopBackOff" {
					restartedRecently = true
				}
			}

			// Check for crash loop back off
			if containerStatus.State.Waiting != nil &&
				containerStatus.State.Waiting.Reason == "CrashLoopBackOff" {
				podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
				status.CrashLoopingPods = append(status.CrashLoopingPods, podKey)
			}
		}

		if restartedRecently {
			status.RestartingPods++
		}
	}

	status.StuckPods = findStuckPods(pods.Items, time.Now())

	return nil
}

const (
	// stuckAfter is longer than a normal image pull, rollout or scale-up, so
	// starting a node or deploying doesn't report pods that are just starting.
	stuckAfter = 10 * time.Minute
	// stuckPodPenalty and stuckPodMaxPenalty are in health score points.
	stuckPodPenalty    = 2.5
	stuckPodMaxPenalty = 25.0
	// stuckPodsNamedInIssue keeps the issue message readable.
	stuckPodsNamedInIssue = 5
)

// Waiting reasons that won't resolve by waiting. CrashLoopBackOff isn't here
// because it's reported on its own.
var stuckWaitingReasons = map[string]bool{
	"CreateContainerConfigError": true,
	"CreateContainerError":       true,
	"ImagePullBackOff":           true,
	"ErrImagePull":               true,
	"InvalidImageName":           true,
	"RunContainerError":          true,
}

// findStuckPods returns pods older than stuckAfter that can't start: a
// container or init container is waiting on a fault, or the pod is still
// Pending. Pods being deleted or that have finished are left out.
func findStuckPods(pods []v1.Pod, now time.Time) []StuckPod {
	stuck := make([]StuckPod, 0)
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			continue
		}
		if now.Sub(pod.CreationTimestamp.Time) < stuckAfter {
			continue
		}
		if reason := stuckReason(pod); reason != "" {
			stuck = append(stuck, StuckPod{
				Namespace: pod.Namespace,
				Name:      pod.Name,
				Reason:    reason,
				Since:     pod.CreationTimestamp.Time,
			})
		}
	}
	return stuck
}

func stuckReason(pod *v1.Pod) string {
	for _, statuses := range [][]v1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting != nil && stuckWaitingReasons[cs.State.Waiting.Reason] {
				return cs.State.Waiting.Reason
			}
		}
	}
	if pod.Status.Phase != v1.PodPending {
		return ""
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == v1.PodScheduled && c.Status == v1.ConditionFalse && c.Reason != "" {
			return c.Reason
		}
	}
	return "Pending"
}

// stuckPodsMessage names the first few stuck pods, so the issue says what to look at.
func stuckPodsMessage(pods []StuckPod) string {
	named := make([]string, 0, stuckPodsNamedInIssue)
	for i, p := range pods {
		if i == stuckPodsNamedInIssue {
			break
		}
		named = append(named, fmt.Sprintf("%s/%s (%s)", p.Namespace, p.Name, p.Reason))
	}
	msg := fmt.Sprintf("%d pods can't start: %s", len(pods), strings.Join(named, ", "))
	if extra := len(pods) - len(named); extra > 0 {
		msg += fmt.Sprintf(", and %d more", extra)
	}
	return msg
}

// checkControlPlaneHealth checks the health of control plane components
func checkControlPlaneHealth(ctx context.Context, clientset *kubernetes.Clientset, status *ControlPlaneStatus) error {
	// Check API server
	startTime := time.Now()
	_, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
	apiCallDuration := time.Since(startTime)

	status.APIServerLatency = float64(apiCallDuration.Milliseconds())
	status.APIServerHealthy = err == nil && apiCallDuration < 1*time.Second

	// Check kube-system components
	pods, err := clientset.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list kube-system pods: %w", err)
	}

	status.ControllerHealthy = true
	status.SchedulerHealthy = true
	status.EtcdHealthy = true
	status.CoreDNSHealthy = true

	for _, pod := range pods.Items {
		if strings.Contains(pod.Name, "kube-controller-manager") && pod.Status.Phase != v1.PodRunning {
			status.ControllerHealthy = false
		}
		if strings.Contains(pod.Name, "kube-scheduler") && pod.Status.Phase != v1.PodRunning {
			status.SchedulerHealthy = false
		}
		if strings.Contains(pod.Name, "etcd") && pod.Status.Phase != v1.PodRunning {
			status.EtcdHealthy = false
		}
		if strings.Contains(pod.Name, "coredns") && pod.Status.Phase != v1.PodRunning {
			status.CoreDNSHealthy = false
		}
	}

	status.OverallHealthy = status.APIServerHealthy && status.ControllerHealthy &&
		status.SchedulerHealthy && status.EtcdHealthy && status.CoreDNSHealthy

	return nil
}

// checkNetworkHealth checks the health of network components
func checkNetworkHealth(ctx context.Context, clientset *kubernetes.Clientset, status *NetworkStatus) error {
	// Check CNI pods (assuming they're in kube-system)
	cniPods, err := clientset.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{
		LabelSelector: "k8s-app in (calico-node,flannel,weave-net,cilium)",
	})

	if err != nil {
		log.Printf("Failed to check CNI pods: %v", err)
		status.CNIHealthy = false
	} else {
		status.CNIHealthy = true
		for _, pod := range cniPods.Items {
			if pod.Status.Phase != v1.PodRunning {
				status.CNIHealthy = false
				break
			}
		}
	}

	// Check DNS resolution - CoreDNS
	coredns, err := clientset.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{
		LabelSelector: "k8s-app=kube-dns",
	})

	if err != nil {
		log.Printf("Failed to check CoreDNS pods: %v", err)
		status.DNSResolutionOK = false
	} else {
		status.DNSResolutionOK = true
		for _, pod := range coredns.Items {
			if pod.Status.Phase != v1.PodRunning {
				status.DNSResolutionOK = false
				break
			}
		}
	}

	// Check service endpoints health
	services, err := clientset.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("Failed to list services: %v", err)
		status.ServiceEndpointsHealthy = false
	} else {
		status.ServiceEndpointsHealthy = true

		for _, svc := range services.Items {
			if svc.Spec.Selector == nil || len(svc.Spec.Selector) == 0 {
				// Skip services without selectors (e.g., ExternalName)
				continue
			}

			// Check if service has endpoints
			endpoints, err := clientset.CoreV1().Endpoints(svc.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
			if err != nil || len(endpoints.Subsets) == 0 {
				status.ServiceEndpointsHealthy = false
				break
			}
		}
	}

	// Check Ingress controller
	ingressControllers, err := clientset.AppsV1().Deployments("").List(ctx, metav1.ListOptions{
		LabelSelector: "app in (ingress-nginx,traefik,istio-ingressgateway)",
	})

	if err != nil {
		log.Printf("Failed to check ingress controllers: %v", err)
		status.IngressHealthy = false
	} else {
		if len(ingressControllers.Items) == 0 {
			// No ingress controller found - might be normal for some clusters
			status.IngressHealthy = true
		} else {
			status.IngressHealthy = true
			for _, ingress := range ingressControllers.Items {
				if ingress.Status.ReadyReplicas < *ingress.Spec.Replicas {
					status.IngressHealthy = false
					break
				}
			}
		}
	}

	// Count network policies
	netpols, err := clientset.NetworkingV1().NetworkPolicies("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("Failed to count network policies: %v", err)
	} else {
		status.NetworkPoliciesCount = len(netpols.Items)
	}

	return nil
}

// checkResourceUsage collects basic cluster resource usage (CPU, Memory) using metrics API when available.
// It populates ClusterCPUUsage, ClusterMemoryUsage, HighCPUNodes, HighMemoryNodes and LowResourceNodes.
// If metricsClient is nil or metrics cannot be retrieved, the function logs and returns nil (non-fatal).
func checkResourceUsage(ctx context.Context, clientset *kubernetes.Clientset, metricsClient *metricsv.Clientset, status *ResourceUsageStatus) error {
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	// Initialize slices/maps
	status.HighCPUNodes = make([]string, 0)
	status.HighMemoryNodes = make([]string, 0)
	status.LowResourceNodes = make([]string, 0)
	status.HighUsageNamespaces = make([]string, 0)

	if metricsClient == nil {
		// Metrics client not provided; can't compute usage percentages reliably
		log.Printf("metrics client not provided, skipping detailed resource usage")
		return nil
	}

	// Get node metrics
	nodeMetricsList, err := metricsClient.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("failed to get node metrics: %v", err)
		return nil
	}

	var totalCPUCapacityMilli int64
	var totalMemoryCapacityBytes int64
	nodeCapacityCPU := make(map[string]int64)
	nodeCapacityMem := make(map[string]int64)

	for _, n := range nodes.Items {
		cpu := n.Status.Allocatable.Cpu().MilliValue()
		mem := n.Status.Allocatable.Memory().Value()
		totalCPUCapacityMilli += cpu
		totalMemoryCapacityBytes += mem
		nodeCapacityCPU[n.Name] = cpu
		nodeCapacityMem[n.Name] = mem
	}

	var totalCPUUsageMilli int64
	var totalMemoryUsageBytes int64

	for _, m := range nodeMetricsList.Items {
		cpuUsage := m.Usage.Cpu().MilliValue()
		memUsage := m.Usage.Memory().Value()
		totalCPUUsageMilli += cpuUsage
		totalMemoryUsageBytes += memUsage

		if capCPU, ok := nodeCapacityCPU[m.Name]; ok && capCPU > 0 {
			pct := float64(cpuUsage) / float64(capCPU) * 100.0
			if pct >= 80.0 {
				status.HighCPUNodes = append(status.HighCPUNodes, m.Name)
			}
			if pct <= 10.0 {
				status.LowResourceNodes = appendIfMissing(status.LowResourceNodes, m.Name)
			}
		}
		if capMem, ok := nodeCapacityMem[m.Name]; ok && capMem > 0 {
			pct := float64(memUsage) / float64(capMem) * 100.0
			if pct >= 80.0 {
				status.HighMemoryNodes = append(status.HighMemoryNodes, m.Name)
			}
			if pct <= 10.0 {
				status.LowResourceNodes = appendIfMissing(status.LowResourceNodes, m.Name)
			}
		}
	}

	if totalCPUCapacityMilli > 0 {
		status.ClusterCPUUsage = float64(totalCPUUsageMilli) / float64(totalCPUCapacityMilli) * 100.0
	}
	if totalMemoryCapacityBytes > 0 {
		status.ClusterMemoryUsage = float64(totalMemoryUsageBytes) / float64(totalMemoryCapacityBytes) * 100.0
		status.ClusterMemoryUsedBytes = totalMemoryUsageBytes
		status.ClusterMemoryAllocatableBytes = totalMemoryCapacityBytes
	}

	// ClusterStorageUsage and HighUsageNamespaces require additional metrics (e.g., CSI or volume usage) or pod metrics by namespace;
	// leave them empty / zero for now.
	status.ClusterStorageUsage = 0.0

	return nil
}

// checkComponentStatuses attempts to populate component status from the ComponentStatus API,
// falling back to scanning kube-system pods for common control-plane and core components.
func checkComponentStatuses(ctx context.Context, clientset *kubernetes.Clientset, statuses *[]ComponentStatus) error {
	comps := make([]ComponentStatus, 0)

	// Try Cluster ComponentStatuses API first (may be deprecated/unavailable on some clusters)
	csList, err := clientset.CoreV1().ComponentStatuses().List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, c := range csList.Items {
			healthy := true
			msg := ""
			for _, cond := range c.Conditions {
				if cond.Type == v1.ComponentHealthy && cond.Status != v1.ConditionTrue {
					healthy = false
					msg = fmt.Sprintf("%s=%s", cond.Type, cond.Status)
					break
				}
			}
			comps = append(comps, ComponentStatus{
				Name:    c.Name,
				Healthy: healthy,
				Message: msg,
			})
		}
		*statuses = comps
		return nil
	}

	// Fallback: inspect kube-system pods for common components
	pods, err2 := clientset.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{})
	if err2 != nil {
		return fmt.Errorf("componentstatuses API error: %v; kube-system pods fallback error: %v", err, err2)
	}

	targets := []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd", "coredns"}
	compMap := map[string]ComponentStatus{}
	for _, t := range targets {
		compMap[t] = ComponentStatus{Name: t, Healthy: false}
	}

	for _, pod := range pods.Items {
		for _, t := range targets {
			if strings.Contains(pod.Name, t) {
				healthy := pod.Status.Phase == v1.PodRunning
				msg := ""
				if !healthy {
					msg = fmt.Sprintf("pod %s phase=%s", pod.Name, pod.Status.Phase)
				}
				cs := compMap[t]
				// prefer marking healthy if any matching pod is running
				if healthy {
					cs.Healthy = true
					cs.Message = ""
				} else if cs.Message == "" {
					cs.Message = msg
				}
				compMap[t] = cs
			}
		}
	}

	for _, t := range targets {
		comps = append(comps, compMap[t])
	}

	*statuses = comps
	return nil
}

// appendIfMissing appends s to slice if it's not already present.
func appendIfMissing(slice []string, s string) []string {
	for _, v := range slice {
		if v == s {
			return slice
		}
	}
	return append(slice, s)
}

// checkNamespaceHealth collects basic health information per namespace: pod counts, deployments summary and services/endpoints.
// It favors best-effort behaviour: errors for a single namespace are logged and do not abort the whole scan.
func checkNamespaceHealth(ctx context.Context, clientset *kubernetes.Clientset, metricsClient *metricsv.Clientset, health *ClusterHealth) error {
	namespaces, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	for _, ns := range namespaces.Items {
		nsName := ns.Name
		nsHealth := NamespaceHealth{
			PodStatus:        PodHealthStatus{PodsPerNode: make(map[string]int), CrashLoopingPods: make([]string, 0)},
			DeploymentStatus: DeploymentStatus{},
			ServiceStatus:    ServiceStatus{},
			ResourceUsage:    ResourceUsageStatus{},
			HealthScore:      100,
		}

		// Pods in namespace
		pods, err := clientset.CoreV1().Pods(nsName).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("failed to list pods in namespace %s: %v", nsName, err)
		} else {
			ps := PodHealthStatus{PodsPerNode: make(map[string]int), CrashLoopingPods: make([]string, 0)}
			ps.TotalPods = len(pods.Items)
			for _, pod := range pods.Items {
				if pod.Spec.NodeName != "" {
					ps.PodsPerNode[pod.Spec.NodeName]++
				}
				switch pod.Status.Phase {
				case v1.PodRunning:
					ps.RunningPods++
				case v1.PodPending:
					ps.PendingPods++
				case v1.PodSucceeded:
					ps.SucceededPods++
				case v1.PodFailed:
					ps.FailedPods++
				default:
					ps.UnknownPods++
				}
				for _, cs := range pod.Status.ContainerStatuses {
					if cs.RestartCount > 5 {
						ps.RestartingPods++
					}
					if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
						ps.CrashLoopingPods = append(ps.CrashLoopingPods, fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
					}
				}
			}
			nsHealth.PodStatus = ps
		}

		// Deployments in namespace
		depls, err := clientset.AppsV1().Deployments(nsName).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("failed to list deployments in namespace %s: %v", nsName, err)
		} else {
			ds := DeploymentStatus{TotalDeployments: len(depls.Items)}
			for _, d := range depls.Items {
				desired := int32(1)
				if d.Spec.Replicas != nil {
					desired = *d.Spec.Replicas
				}
				// Consider healthy when ready replicas >= desired and at least one desired replica exists
				if desired > 0 && d.Status.ReadyReplicas >= desired {
					ds.HealthyDeployments++
				} else if d.Status.UnavailableReplicas > 0 {
					ds.FailedDeployments++
				} else if d.Status.ReadyReplicas < desired {
					ds.ProgressingDeployments++
				} else {
					// fallback classification
					ds.ProgressingDeployments++
				}
			}
			nsHealth.DeploymentStatus = ds
		}

		// Services and endpoints in namespace
		svcs, err := clientset.CoreV1().Services(nsName).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("failed to list services in namespace %s: %v", nsName, err)
		} else {
			ss := ServiceStatus{TotalServices: len(svcs.Items)}
			for _, svc := range svcs.Items {
				if svc.Spec.Selector == nil || len(svc.Spec.Selector) == 0 {
					// treat services without selectors as not applicable
					continue
				}
				ep, err := clientset.CoreV1().Endpoints(nsName).Get(ctx, svc.Name, metav1.GetOptions{})
				if err != nil || len(ep.Subsets) == 0 {
					ss.ServicesWithoutEndpoints++
				} else {
					ss.ServicesWithEndpoints++
				}
			}
			nsHealth.ServiceStatus = ss
		}

		// Resource usage per-namespace could be implemented with pod metrics aggregated by namespace when metricsClient is available.
		// For now leave ResourceUsage empty/zero (best-effort); not having metrics should not be fatal.
		_ = metricsClient // placeholder to acknowledge metricsClient parameter

		health.NamespaceHealth[nsName] = nsHealth
	}

	return nil
}

// identifyHealthIssues inspects the ClusterHealth and appends simple, best-effort HealthIssue entries
// based on node/pod/component/resource conditions.
func identifyHealthIssues(h *ClusterHealth) {
	issues := make([]HealthIssue, 0)
	now := time.Now()

	// Node readiness
	if h.NodeStatus.TotalNodes > 0 && h.NodeStatus.ReadyNodes < h.NodeStatus.TotalNodes {
		issues = append(issues, HealthIssue{
			Severity:   "critical",
			Resource:   "nodes",
			Message:    fmt.Sprintf("%d/%d nodes are Ready", h.NodeStatus.ReadyNodes, h.NodeStatus.TotalNodes),
			Timestamp:  now,
			Suggestion: "Investigate node conditions (MemoryPressure/DiskPressure/PIDPressure/NetworkUnavailable) and kubelet logs; consider cordoning or draining unhealthy nodes.",
		})
	}

	// Specific node pressures
	if h.NodeStatus.MemoryPressureNodes > 0 {
		severity := "warning"
		if h.NodeStatus.MemoryPressureNodes > 1 {
			severity = "critical"
		}
		issues = append(issues, HealthIssue{
			Severity:   severity,
			Resource:   "nodes",
			Message:    fmt.Sprintf("%d nodes reporting MemoryPressure", h.NodeStatus.MemoryPressureNodes),
			Timestamp:  now,
			Suggestion: "Check node memory usage and resident processes; consider freeing memory, resizing nodes, or enabling swap if appropriate.",
		})
	}
	if h.NodeStatus.DiskPressureNodes > 0 {
		severity := "warning"
		if h.NodeStatus.DiskPressureNodes > 1 {
			severity = "critical"
		}
		issues = append(issues, HealthIssue{
			Severity:   severity,
			Resource:   "nodes",
			Message:    fmt.Sprintf("%d nodes reporting DiskPressure", h.NodeStatus.DiskPressureNodes),
			Timestamp:  now,
			Suggestion: "Inspect disk usage and inode consumption on affected nodes; consider cleaning logs, expanding storage, or rescheduling pods.",
		})
	}
	if h.NodeStatus.PIDPressureNodes > 0 {
		issues = append(issues, HealthIssue{
			Severity:   "warning",
			Resource:   "nodes",
			Message:    fmt.Sprintf("%d nodes reporting PIDPressure", h.NodeStatus.PIDPressureNodes),
			Timestamp:  now,
			Suggestion: "Investigate processes causing high PID usage and consider restarting or scaling workloads.",
		})
	}
	if h.NodeStatus.NetworkUnavailableNodes > 0 {
		issues = append(issues, HealthIssue{
			Severity:   "critical",
			Resource:   "nodes",
			Message:    fmt.Sprintf("%d nodes reporting NetworkUnavailable", h.NodeStatus.NetworkUnavailableNodes),
			Timestamp:  now,
			Suggestion: "Check CNI/plugin status and node network configuration; network-unavailable nodes can cause pod disruption.",
		})
	}

	// Pod issues
	if h.PodStatus.FailedPods > 0 {
		issues = append(issues, HealthIssue{
			Severity:   "critical",
			Resource:   "pods",
			Message:    fmt.Sprintf("%d failed pods detected", h.PodStatus.FailedPods),
			Timestamp:  now,
			Suggestion: "Inspect pod events and container logs to determine failure causes and restart or fix failing workloads.",
		})
	}
	if len(h.PodStatus.CrashLoopingPods) > 0 {
		issues = append(issues, HealthIssue{
			Severity:   "warning",
			Resource:   "pods",
			Message:    fmt.Sprintf("%d pods in CrashLoopBackOff", len(h.PodStatus.CrashLoopingPods)),
			Timestamp:  now,
			Suggestion: "Check container logs and recent changes; consider backoff, probe configuration, or image issues.",
		})
	}
	if len(h.PodStatus.StuckPods) > 0 {
		issues = append(issues, HealthIssue{
			Severity:   "warning",
			Resource:   "pods",
			Message:    stuckPodsMessage(h.PodStatus.StuckPods),
			Timestamp:  now,
			Suggestion: "Describe each pod for its events: a missing Secret or ConfigMap, an image that can't be pulled, or no node with room or a matching selector. These don't resolve by waiting.",
		})
	}
	if h.PodStatus.RestartingPods > 10 {
		issues = append(issues, HealthIssue{
			Severity:   "warning",
			Resource:   "pods",
			Message:    fmt.Sprintf("%d containers restarted frequently", h.PodStatus.RestartingPods),
			Timestamp:  now,
			Suggestion: "Investigate frequent restarts and consider improving readiness/liveness probes or stabilizing the application.",
		})
	}

	// Control plane
	if !h.ControlPlaneStatus.OverallHealthy {
		issues = append(issues, HealthIssue{
			Severity:   "critical",
			Resource:   "control-plane",
			Message:    "Control plane components unhealthy",
			Timestamp:  now,
			Suggestion: "Check API server, controller-manager, scheduler, etcd and CoreDNS pod statuses and logs.",
		})
	} else if !h.ControlPlaneStatus.APIServerHealthy {
		issues = append(issues, HealthIssue{
			Severity:   "critical",
			Resource:   "apiserver",
			Message:    fmt.Sprintf("API server responding slowly: %.2fms", h.ControlPlaneStatus.APIServerLatency),
			Timestamp:  now,
			Suggestion: "Review API server load and latency; consider scaling control plane or tuning requests.",
		})
	}

	// Components
	for _, comp := range h.ComponentStatuses {
		if !comp.Healthy {
			issues = append(issues, HealthIssue{
				Severity:   "critical",
				Resource:   "component",
				Name:       comp.Name,
				Message:    fmt.Sprintf("component unhealthy: %s", comp.Message),
				Timestamp:  now,
				Suggestion: "Inspect the component pods and logs in kube-system and any related system components.",
			})
		}
	}

	// Resource usage
	if h.ResourceUsage.ClusterCPUUsage >= 90.0 {
		issues = append(issues, HealthIssue{
			Severity:   "warning",
			Resource:   "cluster-cpu",
			Message:    fmt.Sprintf("cluster CPU usage is high: %.1f%%", h.ResourceUsage.ClusterCPUUsage),
			Timestamp:  now,
			Suggestion: "Consider scaling workloads, adding nodes, or optimizing CPU usage.",
		})
	}
	if h.ResourceUsage.ClusterMemoryUsage >= 90.0 {
		issues = append(issues, HealthIssue{
			Severity:   "warning",
			Resource:   "cluster-memory",
			Message:    fmt.Sprintf("cluster memory usage is high: %.1f%%", h.ResourceUsage.ClusterMemoryUsage),
			Timestamp:  now,
			Suggestion: "Consider scaling workloads, adding memory to nodes, or investigating memory leaks.",
		})
	}
	if len(h.ResourceUsage.HighCPUNodes) > 0 {
		issues = append(issues, HealthIssue{
			Severity:   "warning",
			Resource:   "nodes",
			Message:    fmt.Sprintf("nodes with high CPU usage: %s", strings.Join(h.ResourceUsage.HighCPUNodes, ",")),
			Timestamp:  now,
			Suggestion: "Investigate high-CPU nodes and consider rescheduling or scaling.",
		})
	}
	if len(h.ResourceUsage.HighMemoryNodes) > 0 {
		issues = append(issues, HealthIssue{
			Severity:   "warning",
			Resource:   "nodes",
			Message:    fmt.Sprintf("nodes with high memory usage: %s", strings.Join(h.ResourceUsage.HighMemoryNodes, ",")),
			Timestamp:  now,
			Suggestion: "Investigate high-memory nodes and consider rescheduling or scaling.",
		})
	}
	h.Issues = issues
}

// calculateHealthScore derives a simple 0-100 health score from ClusterHealth.
// The function applies weighted penalties for node/pod/component/resource issues and clamps the result to [0,100].
func calculateHealthScore(h *ClusterHealth) int {
	score := 100.0

	// Nodes: penalize for not-ready and pressure conditions
	if h.NodeStatus.TotalNodes > 0 {
		notReady := h.NodeStatus.TotalNodes - h.NodeStatus.ReadyNodes
		score -= float64(notReady) * 5.0 // 5 points per not-ready node
	}
	score -= float64(h.NodeStatus.MemoryPressureNodes) * 3.0
	score -= float64(h.NodeStatus.DiskPressureNodes) * 4.0
	score -= float64(h.NodeStatus.PIDPressureNodes) * 2.0
	score -= float64(h.NodeStatus.NetworkUnavailableNodes) * 6.0

	// Pods: failed, crashlooping and frequent restarts
	score -= float64(h.PodStatus.FailedPods) * 2.0
	score -= float64(len(h.PodStatus.CrashLoopingPods)) * 2.5
	score -= math.Min(float64(len(h.PodStatus.StuckPods))*stuckPodPenalty, stuckPodMaxPenalty)
	if h.PodStatus.RestartingPods > 10 {
		score -= 5.0
	}

	// Control plane: heavy penalty if overall unhealthy, smaller for API latency issues
	if !h.ControlPlaneStatus.OverallHealthy {
		score -= 30.0
	} else if !h.ControlPlaneStatus.APIServerHealthy {
		score -= 15.0
	}

	// Component statuses
	for _, c := range h.ComponentStatuses {
		if !c.Healthy {
			score -= 8.0
		}
	}

	// Resource usage penalties
	if h.ResourceUsage.ClusterCPUUsage >= 95.0 {
		score -= 10.0
	} else if h.ResourceUsage.ClusterCPUUsage >= 80.0 {
		score -= 5.0
	}
	if h.ResourceUsage.ClusterMemoryUsage >= 95.0 {
		score -= 10.0
	} else if h.ResourceUsage.ClusterMemoryUsage >= 80.0 {
		score -= 5.0
	}

	// Clamp and round
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return int(math.Round(score))
}
