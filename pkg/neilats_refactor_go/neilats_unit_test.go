package neilats_refactor_go

import (
	"fmt"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	fakeframework "k8s.io/kubernetes/pkg/scheduler/framework/fake"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"sigs.k8s.io/scheduler-plugins/apis/config"
	testutil "sigs.k8s.io/scheduler-plugins/test/util"
	"testing"
)

func TestNeilatsUnit(t *testing.T) {
	tests := []struct {
		name       string                              // 测试名称
		nodeInfos  []*framework.NodeInfo               // 模拟节点信息
		pods       []*v1.Pod                           // 待测试的 Pod
		configData config.NeilatsRefactorSchedulerArgs //插件的参数配置
	}{
		{
			name: "neilats unit test SLA False",
			nodeInfos: []*framework.NodeInfo{
				makeNodeInfo("node1"),
				makeNodeInfo("node2"),
				makeNodeInfo("master"),
			},
			pods: []*v1.Pod{
				makeNeilatsTestPod("test-pod-1", "", "", 100, 500*1024*1024, 1024, 1024*1024),
				makeNeilatsTestPod("test-pod-2", "", "", 100, 200*1024*1024, 2048, 2*1024*1024),
			},
			configData: config.NeilatsRefactorSchedulerArgs{
				PrometheusAddress: "http://39.98.76.224:32599",
				NetworkDevice: map[string]string{
					"master": "eth0",
					"node1":  "eth0",
					"node2":  "eth0",
				},
				StorageDevice: map[string]string{
					"master": "/dev/vda2",
					"node1":  "/dev/vda2",
					"node2":  "/dev/vda2",
				},
				EnableSLA:                false,
				KubeNodeAddressAndSecret: map[string]config.UserAddressSecretMap{},
			},
		},
		{
			name: "neilats unit test SLA True",
			nodeInfos: []*framework.NodeInfo{
				makeNodeInfo("node1"),
				makeNodeInfo("node2"),
				makeNodeInfo("master"),
			},
			pods: []*v1.Pod{
				makeNeilatsTestPod("test-pod-0", "", "", 100, 500*1024*1024, 1024, 1024*1024),
				makeNeilatsTestPod("test-pod-1", "node3", "0.5", 100, 500*1024*1024, 1024, 1024*1024),
				makeNeilatsTestPod("test-pod-2", "node1", "as", 100, 200*1024*1024, 2048, 2*1024*1024),
				makeNeilatsTestPod("test-pod-3", "master", "0.6", 100, 200*1024*1024, 2048, 2*1024*1024),
				makeNeilatsTestPod("test-pod-4", "node2", "", 100, 200*1024*1024, 2048, 2*1024*1024),
				makeNeilatsTestPod("test-pod-5", "", "0.1", 100, 200*1024*1024, 2048, 2*1024*1024),
			},
			configData: config.NeilatsRefactorSchedulerArgs{
				PrometheusAddress: "http://39.98.76.224:32599",
				NetworkDevice: map[string]string{
					"master": "eth0",
					"node1":  "eth0",
					"node2":  "eth0",
				},
				StorageDevice: map[string]string{
					"master": "/dev/vda2",
					"node1":  "/dev/vda2",
					"node2":  "/dev/vda2",
				},
				EnableSLA: true,
				KubeNodeAddressAndSecret: map[string]config.UserAddressSecretMap{
					"master": {"39.98.76.224", "Mobisys912!"},
					"node2":  {"47.92.233.4", "Mobisys912!"},
					"node1":  {"47.92.228.102", "Mobisys912!"},
				},
			},
		},
	}

	for _, test := range tests {
		// 执行子测试
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)                         // 创建日志记录器，上下文
			cs := clientsetfake.NewSimpleClientset()                     // 创建假的客户端集，模拟Kubernetes API
			informerFactory := informers.NewSharedInformerFactory(cs, 0) // 创建共享的 informer 工厂

			// 注册插件，包括绑定插件，队列排序插件，以及自定义得分插件，除了自定义得分插件，其他插件均为必须的调度插件
			registeredPlugins := []st.RegisterPluginFunc{
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),

				st.RegisterPluginAsExtensions(Name, New, "PreFilter"),
				st.RegisterPluginAsExtensions(Name, New, "Filter"),
				st.RegisterPluginAsExtensions(Name, New, "Score"),
			}

			// 使用fakeSharedLister模拟节点信息，注入测试用例中的节点信息
			fakeSharedLister := &fakeSharedLister{nodes: test.nodeInfos}
			// 初始化框架和插件，注入所有依赖项，客户端，Informer，节点快照
			fh, err := st.NewFramework(
				ctx,
				registeredPlugins,
				"default-scheduler",
				frameworkruntime.WithClientSet(cs),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithSnapshotSharedLister(fakeSharedLister),
				frameworkruntime.WithPodNominator(testutil.NewPodNominator(nil)),
			)
			if err != nil {
				t.Fatalf("fail to create framework: %s", err)
			}

			// 创建并初始化插件
			pe, _ := New(&test.configData, fh)
			// 由于实现了多个扩展点，我们将其断言为自定义调度插件类型，而不是单一的ScorePlugin类型
			neilatsPlugin := pe.(*NeilatsRefactorScheduler)
			//scorePlugin := pe.(framework.ScorePlugin)

			// 如果使用这种方式创建自定义调度器，fh传入的作用是？跟集成测试方式的区别在于？
			// 这种方式还能使用到自定义调度器的New函数吗？参数传递还需要在New函数中实现runtime.Object吗？
			//temp := &NeilatsRefactorScheduler{
			//	handle: fh,
			//	config: test.configData,
			//}

			// 用于PreFilter阶段的单元测试：直接调用方式
			prefilter_passed_pods := make([]*v1.Pod, 0)
			for _, pod := range test.pods {
				_, status := neilatsPlugin.PreFilter(ctx, nil, pod)
				if status.Code() == framework.Success {
					prefilter_passed_pods = append(prefilter_passed_pods, pod)
					t.Logf(fmt.Sprintf("Pod %s Pass PreFilter!", pod.Name))
				} else {
					t.Logf(fmt.Sprintf("Pod %s UnPass PreFilter!", pod.Name))
				}
			}

			// 用于Filter阶段的单元测试:直接调用方式
			for _, pod := range prefilter_passed_pods {
				for _, node := range test.nodeInfos {
					status := neilatsPlugin.Filter(ctx, nil, pod, node)
					if status.Code() == framework.Success {
						t.Logf(fmt.Sprintf("Node %s Pass Pod %s SLA Constraint!", node.Node().Name, pod.Name))
					} else if status.Code() == framework.Unschedulable {
						t.Logf(fmt.Sprintf("Node %s UnPass Pod %s SLA Constraint!", node.Node().Name, pod.Name))
					} else {
						t.Logf(fmt.Sprintf("Node %s Get rttMatrix Failed!", node.Node().Name))
					}
				}
			}

			// 收集所有节点得分用户归一化,并打印原始节点评分:直接调用方式
			var scoreList framework.NodeScoreList
			for _, nodeInfo := range test.nodeInfos {
				nodeName := nodeInfo.Node().Name
				score, err := neilatsPlugin.Score(ctx, nil, test.pods[0], nodeName)
				if err != nil {
					t.Logf("Scoring node %s failed: %v", nodeName, err)
					continue
				}
				t.Logf("Node %s score: %d", nodeName, score)
				scoreList = append(scoreList, framework.NodeScore{Name: nodeName, Score: score})
			}

			// 归一化并打印结果
			if status := neilatsPlugin.ScoreExtensions().NormalizeScore(ctx, nil, test.pods[0], scoreList); !status.IsSuccess() {
				t.Logf("Normalize score failed: %v", status)
			}
			t.Log("\n=== Normalized scores ===\n")
			for _, score := range scoreList {
				t.Logf("Node %s score: %d", score.Name, score.Score)
			}
		})
	}
}

func TestNeilatsUnitFK(t *testing.T) {
	tests := []struct {
		name       string                              // 测试名称
		nodeInfos  []*framework.NodeInfo               // 模拟节点信息
		pods       []*v1.Pod                           // 待测试的 Pod
		configData config.NeilatsRefactorSchedulerArgs //插件的参数配置
	}{
		{
			name: "neilats unit test SLA False",
			nodeInfos: []*framework.NodeInfo{
				makeNodeInfo("node1"),
				makeNodeInfo("node2"),
				makeNodeInfo("master"),
			},
			pods: []*v1.Pod{
				makeNeilatsTestPod("test-pod-1", "", "", 100, 500*1024*1024, 1024, 1024*1024),
				makeNeilatsTestPod("test-pod-2", "", "", 100, 200*1024*1024, 2048, 2*1024*1024),
			},
			configData: config.NeilatsRefactorSchedulerArgs{
				PrometheusAddress: "http://39.98.76.224:32599",
				NetworkDevice: map[string]string{
					"master": "eth0",
					"node1":  "eth0",
					"node2":  "eth0",
				},
				StorageDevice: map[string]string{
					"master": "/dev/vda2",
					"node1":  "/dev/vda2",
					"node2":  "/dev/vda2",
				},
				EnableSLA:                false,
				KubeNodeAddressAndSecret: map[string]config.UserAddressSecretMap{},
			},
		},
		{
			name: "neilats unit test SLA True",
			nodeInfos: []*framework.NodeInfo{
				makeNodeInfo("node1"),
				makeNodeInfo("node2"),
				makeNodeInfo("master"),
			},
			pods: []*v1.Pod{
				makeNeilatsTestPod("test-pod-0", "", "", 100, 500*1024*1024, 1024, 1024*1024),
				makeNeilatsTestPod("test-pod-1", "node3", "0.5", 100, 500*1024*1024, 1024, 1024*1024),
				makeNeilatsTestPod("test-pod-2", "node1", "as", 100, 200*1024*1024, 2048, 2*1024*1024),
				makeNeilatsTestPod("test-pod-3", "master", "0.6", 100, 200*1024*1024, 2048, 2*1024*1024),
				makeNeilatsTestPod("test-pod-4", "node2", "", 100, 200*1024*1024, 2048, 2*1024*1024),
				makeNeilatsTestPod("test-pod-5", "", "0.1", 100, 200*1024*1024, 2048, 2*1024*1024),
			},
			configData: config.NeilatsRefactorSchedulerArgs{
				PrometheusAddress: "http://39.98.76.224:32599",
				NetworkDevice: map[string]string{
					"master": "eth0",
					"node1":  "eth0",
					"node2":  "eth0",
				},
				StorageDevice: map[string]string{
					"master": "/dev/vda2",
					"node1":  "/dev/vda2",
					"node2":  "/dev/vda2",
				},
				EnableSLA: true,
				KubeNodeAddressAndSecret: map[string]config.UserAddressSecretMap{
					"master": {"39.98.76.224", "Mobisys912!"},
					"node2":  {"47.92.233.4", "Mobisys912!"},
					"node1":  {"47.92.228.102", "Mobisys912!"},
				},
			},
		},
	}

	// 执行子测试
	for _, test := range tests {
		// 针对测试案例中的每个Pod执行调度测试
		for _, pod := range test.pods {
			t.Run(test.name, func(t *testing.T) {
				_, ctx := ktesting.NewTestContext(t)                         // 创建日志记录器，上下文
				cs := clientsetfake.NewSimpleClientset()                     // 创建假的客户端集，模拟Kubernetes API
				informerFactory := informers.NewSharedInformerFactory(cs, 0) // 创建共享的 informer 工厂

				// 注册插件，包括绑定插件，队列排序插件，以及自定义得分插件，除了自定义得分插件，其他插件均为必须的调度插件
				registeredPlugins := []st.RegisterPluginFunc{
					st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
					st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),

					st.RegisterPluginAsExtensions(Name, New, "PreFilter"),
					st.RegisterPluginAsExtensions(Name, New, "Filter"),
					st.RegisterPluginAsExtensions(Name, New, "Score"),
				}

				// 使用fakeSharedLister模拟节点信息，注入测试用例中的节点信息
				fakeSharedLister := &fakeSharedLister{nodes: test.nodeInfos}
				// 初始化框架和插件，注入所有依赖项，客户端，Informer，节点快照
				fh, err := st.NewFramework(
					ctx,
					registeredPlugins,
					"default-scheduler",
					frameworkruntime.WithClientSet(cs),
					frameworkruntime.WithInformerFactory(informerFactory),
					frameworkruntime.WithSnapshotSharedLister(fakeSharedLister),
					frameworkruntime.WithPodNominator(testutil.NewPodNominator(nil)),
				)
				if err != nil {
					t.Fatalf("fail to create framework: %s", err)
				}

				// 用于PreFilter阶段的单元测试：使用框架集成方式
				state := framework.NewCycleState()
				_, status := fh.RunPreFilterPlugins(ctx, state, pod)
				if status.IsSuccess() {
					t.Logf(fmt.Sprintf("Pod %s Pass PreFilter!", pod.Name))
				} else {
					t.Logf(fmt.Sprintf("Pod %s UnPass PreFilter!", pod.Name))
				}

				// 验证PreFilter是否写入了state
				//preFilterResult, err := state.Read()

				// 用于Filter阶段的单元测试:框架集成方式
				for _, node := range test.nodeInfos {
					// 从集成框架中获取NodeInfo
					nodeInfo, err := fh.SnapshotSharedLister().NodeInfos().Get(node.Node().Name)
					if err != nil {
						t.Fatalf("failed to get node from framework for %s: %v", node.Node().Name, err)
					}

					status := fh.RunFilterPlugins(ctx, state, pod, nodeInfo)
					if status.IsSuccess() {
						t.Logf(fmt.Sprintf("Node %s Pass Pod %s SLA Constraint!", node.Node().Name, pod.Name))
					} else if status.IsUnschedulable() {
						t.Logf(fmt.Sprintf("Node %s UnPass Pod %s SLA Constraint!", node.Node().Name, pod.Name))
					} else {
						t.Logf(fmt.Sprintf("Node %s Get rttMatrix Failed!", node.Node().Name))
					}
				}

				// ScorePlugins测试：框架集成方式
				var nodeList []*v1.Node
				nodeInfos, err := fh.SnapshotSharedLister().NodeInfos().List()
				for _, nodeInfo := range nodeInfos {
					nodeList = append(nodeList, nodeInfo.Node())
				}
				scores, status := fh.RunScorePlugins(ctx, state, test.pods[0], nodeList)
				if !status.IsSuccess() {
					t.Errorf("RunScorePlugins Failed: %v", status)
				}
				t.Log("=== Normalized Scores (Via Framework) ===")
				for _, score := range scores {
					t.Logf("Node %s: %q", score.Name, score.Scores)
				}
			})
		}
	}
}

func makeNodeInfo(node string) *framework.NodeInfo {
	nodeInfo := framework.NewNodeInfo()
	// 设置节点名称和容量
	nodeInfo.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: node},
	})
	return nodeInfo
}

// 几个传入参数，CPU的单位是毫核， Memory的单位是字节， Network的单位是字节/秒， Disk的单位是字节
// 创建neilats调度器的单元测试Pod
func makeNeilatsTestPod(name, neiNode, sla string, cpu, memory, network, disk int64) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "test-container-1",
					Image: "nginx:latest",
					// 一次性初始化所有资源请求和限制的方法
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{}, // 需要先初始化然后再执行添加操作
						Limits:   v1.ResourceList{}, // 需要先初始化然后再执行添加操作
					},
				},
			},
		},
	}
	if cpu > 0 {
		pod.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = *resource.NewMilliQuantity(cpu, resource.DecimalSI)
	}
	if memory > 0 {
		pod.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = *resource.NewQuantity(memory, resource.BinarySI)
	}
	if network > 0 {
		pod.Labels["network-request"] = fmt.Sprintf("%d", network)
	}
	if disk > 0 {
		pod.Labels["disk-request"] = fmt.Sprintf("%d", disk)
	}
	if neiNode != "" {
		pod.Labels["nei_node"] = neiNode
	}
	if sla != "" {
		pod.Labels["sla"] = sla
	}
	return pod
}

// 插件参数配置辅助函数
//func makePluginConfig(config map[string]interface{}) runtime.Object {
//	return &unstructured.Unstructured{
//		Object: map[string]interface{}{
//			"apiVersion": "v2",
//			"kind":       "NeilatsPluginConfig",
//			"args":       config,
//		},
//	}
//}

type fakeSharedLister struct {
	nodes []*framework.NodeInfo
}

func (f *fakeSharedLister) StorageInfos() framework.StorageInfoLister {
	return nil
}
func (f *fakeSharedLister) NodeInfos() framework.NodeInfoLister {
	return fakeframework.NodeInfoLister(f.nodes)
}
