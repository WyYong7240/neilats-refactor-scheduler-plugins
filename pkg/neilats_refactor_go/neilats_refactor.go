package neilats_refactor_go

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"log"
	"math"
	"sigs.k8s.io/scheduler-plugins/apis/config"
	"strconv"
)

const Name = "NeilatsRefactorScheduler"

// 声明自定义调度器结构体
type NeilatsRefactorScheduler struct {
	config config.NeilatsRefactorSchedulerArgs
	handle framework.Handle
	logger klog.Logger
}

// 声明要实现的扩展点， 同时检测自定义调度器是否实现了这两个扩展点
var _ framework.PreFilterPlugin = &NeilatsRefactorScheduler{}
var _ framework.FilterPlugin = &NeilatsRefactorScheduler{}
var _ framework.ScorePlugin = &NeilatsRefactorScheduler{}

// 声明自定义调度器的自定义参数结构体
// 调度器添加自定义参数的方法，参考了其他调度插件中的pkg/nodeResources的实现方法
//type UserAddressSecretMap struct {
//	NodeAddress string
//	NodeSecret  string
//}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NeilatsRefactorSchedulerArgs holds arguments used to configure NeilatsRefactorScheduler plugin.
//type NeilatsRefactorSchedulerArgs struct {
//	// 如果自定义调度器有参数，需要参数结构体添加该内联类型，是Kubernetes库的要求
//	metav1.TypeMeta          `json:",inline"`
//	PrometheusAddress        string                          `json:"prometheusAddress"`
//	NetworkDevice            map[string]string               `json:"networkDevice"`
//	StorageDevice            map[string]string               `json:"storageDevice"`
//	EnableSLA                bool                            `json:"enableSLA"`
//	KubeNodeAddressAndSecret map[string]UserAddressSecretMap `json:"kubeNodeAddressAndSecret"`
//}

// 如果自定义调度器有参数，需要参数结构体实现runtime.Object类型的接口
//var _ runtime.Object = &NeilatsRefactorSchedulerArgs{}

// 需要实现DeepCopyObject、DeepCopyInto这两个接口， 是Kubernetes架构设计与代码生成工具的要求
// 该结构体要被设计为可以被序列化、反序列化、在组件之间传递，并且可以安全的拷贝
// 这两个方法也可以通过code-generator来自动生成
//func (in *NeilatsRefactorSchedulerArgs) DeepCopyInto(out *NeilatsRefactorSchedulerArgs) {
//	*out = *in
//	out.TypeMeta = in.TypeMeta
//
//	// 拷贝基本字段
//	out.PrometheusAddress = in.PrometheusAddress
//	out.EnableSLA = in.EnableSLA
//
//	// 拷贝map[string]string
//	if in.NetworkDevice != nil {
//		out.NetworkDevice = make(map[string]string, len(in.NetworkDevice))
//		for k, v := range in.NetworkDevice {
//			out.NetworkDevice[k] = v
//		}
//	}
//	if in.StorageDevice != nil {
//		out.StorageDevice = make(map[string]string, len(in.StorageDevice))
//		for k, v := range in.StorageDevice {
//			out.StorageDevice[k] = v
//		}
//	}
//	if in.KubeNodeAddressAndSecret != nil {
//		out.KubeNodeAddressAndSecret = make(map[string]UserAddressSecretMap, len(in.KubeNodeAddressAndSecret))
//		for k, v := range in.KubeNodeAddressAndSecret {
//			out.KubeNodeAddressAndSecret[k] = UserAddressSecretMap{NodeAddress: v.NodeAddress, NodeSecret: v.NodeSecret}
//		}
//	}
//	return
//}
//
//func (in *NeilatsRefactorSchedulerArgs) DeepCopy() *NeilatsRefactorSchedulerArgs {
//	if in == nil {
//		return nil
//	}
//	out := new(NeilatsRefactorSchedulerArgs)
//	in.DeepCopyInto(out)
//	return out
//}
//
//// 需要参数结构体实现DeepCopyObject接口
//func (in *NeilatsRefactorSchedulerArgs) DeepCopyObject() runtime.Object {
//	if c := in.DeepCopy(); c != nil {
//		return c
//	}
//	return nil
//}

// 返回插件名称
func (neilats *NeilatsRefactorScheduler) Name() string {
	return Name
}

// 新建并初始化一个新的插件并返回，将其注册到调度器中
func New(neilatsArgs runtime.Object, h framework.Handle) (framework.Plugin, error) {
	defaultConfig := config.NeilatsRefactorSchedulerArgs{
		PrometheusAddress: "http://192.168.3.226:31739", // 默认Prometheus地址
		NetworkDevice: map[string]string{ // 默认网络设备配置
			"master": "ens18",
			"node1":  "ens18",
			"node2":  "ens18",
			"node3":  "ens18",
		},
		StorageDevice: map[string]string{ // 默认存储设备配置
			"master": "/dev/mapper/ubuntu--vg-ubuntu--lv",
			"node1":  "/dev/mapper/ubuntu--vg-ubuntu--lv",
			"node2":  "/dev/mapper/ubuntu--vg-ubuntu--lv",
			"node3":  "/dev/mapper/ubuntu--vg-ubuntu--lv",
		},
		EnableSLA: false, // 默认不开启SLA约束要求
		// 键为Kubernetes中的节点名称
		KubeNodeAddressAndSecret: map[string]config.UserAddressSecretMap{
			"master": {"192.168.3.226", "mobisys912"},
			"node1":  {"192.168.3.229", "mobisys912"},
			"node2":  {"192.168.3.224", "mobisys912"},
			"node3":  {"192.168.3.228", "mobisys912"},
		}, // 由于默认不开启SLA约束，所以不要求kubeNodeAddressAndSecret参数
	}

	// 如果参数neilatsArgs不为空，则尝试解析其中的配置参数
	if neilatsArgs != nil {
		log.Printf("Custom Neilats Args Detected!\n")
		// 将runtime.Object类型的参数，转换为调度器所需的参数类型
		args, ok := neilatsArgs.(*config.NeilatsRefactorSchedulerArgs)
		if !ok {
			log.Printf("want args to be of type NeilatsRefactorSchedulerArgs, got %T\n", neilatsArgs)
			return nil, fmt.Errorf("want args to be of type NeilatsRefactorSchedulerArgs, got %T", neilatsArgs)
		}
		// 如果各个参数存在，将defaultConfig 中的各项参数改为传入的参数，否则使用默认参数
		if args.PrometheusAddress != "" {
			defaultConfig.PrometheusAddress = args.PrometheusAddress
		}
		if len(args.NetworkDevice) > 0 {
			defaultConfig.NetworkDevice = args.NetworkDevice
		}
		if len(args.StorageDevice) > 0 {
			defaultConfig.StorageDevice = args.StorageDevice
		}
		defaultConfig.EnableSLA = args.EnableSLA
		if args.EnableSLA {
			log.Printf("Config EnableSLA is True!\n")
			if len(args.KubeNodeAddressAndSecret) == 0 {
				log.Printf("when EnableSLA is True, KubeNodeAddressAndSecret must have content")
				return nil, fmt.Errorf("when EnableSLA is True, KubeNodeAddressAndSecret must have content")
			}
			defaultConfig.KubeNodeAddressAndSecret = args.KubeNodeAddressAndSecret

			// 启动调度器的延迟测试初始化
			if err := InitNodeLatencyTestTool(defaultConfig.KubeNodeAddressAndSecret); err != nil {
				log.Printf("enable SLA init Node Latency Test Tool Failed:%v\n", err)
				return nil, err
			}
			log.Printf("Init Node Latency Test Tool Success!\n")
			// 启动各个节点之间的延迟测试
			if err := StartLatencyTest(defaultConfig.KubeNodeAddressAndSecret); err != nil {
				log.Printf("enable SLA Start Node Latency Test Tool Failed:%v\n", err)
				return nil, err
			}
			log.Printf("Start Latency Test Success!\n")

			// 此处存在一个问题，子程序与主程序关系紧密（指子程序的启动与主程序相关）又疏远（子程序仅仅帮助主程序生成与修改文件）那么这种情况应该使用goroutine线程实现还是多进程实现？
			// 此处产生一个设想，能否在仅仅调度具有SLA需求的Pod时，才下载一次远程主机上的延迟测试文件，才进行一轮rttMatrix矩阵文件的构建
			// 但是让远程主机上的延迟测试一直运行，这样不仅能够省去rttMatrix文件，还可以省去多线程、文件锁等复杂操作
			// 开始不断间隔下载延迟测试结果文件
			//go GetLatencyTestResultInterval(defaultConfig.KubeNodeAddressAndSecret)
			// 开始构建rttMatrix文件
			//go BuildRtt(defaultConfig.KubeNodeAddressAndSecret)
		}
	}

	log.Printf("Final Neilats Config is:\n")
	log.Print(defaultConfig)
	// 这里返回我们创建并初始化完成的自定义调度器
	return &NeilatsRefactorScheduler{
		handle: h,
		config: defaultConfig,
		logger: klog.FromContext(context.Background()),
	}, nil
}

// 用于在开启了SLA的情况下，过滤掉没有nei_node和SLA约束要求的Pod
func (neilats *NeilatsRefactorScheduler) PreFilter(ctx context.Context, state *framework.CycleState, p *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// 仅当开启了SLA约束要求配置开始检查Pod是否存在有效标签
	if neilats.config.EnableSLA {
		neiNodeLabel := "nei_node"
		SLAConstraintLabel := "sla"

		// 检查Pod是否存在邻近节点标签
		neiNodeValue, neiNodeExist := p.GetLabels()[neiNodeLabel]
		// 检查Pod是否存在SLA约束要求标签
		slaConstraintValue, slaExist := p.GetLabels()[SLAConstraintLabel]

		// 如果两个标签同时都不存在，那么就是个普通Pod，不用被过滤
		if !neiNodeExist && !slaExist {
			log.Printf("Pod %s haven't NeiNodeLable And SLAConstraintLable, pass Prefilter\n", p.Name)
			return nil, framework.NewStatus(framework.Success, fmt.Sprintf("Pod %s haven't NeiNodeLable And SLAConstraintLable, pass Prefilter", p.Name))
			// 如果两个标签都存在，那么就是一个具有SLA要求的Pod，开始检查连个标签值有效性
		} else if neiNodeExist && slaExist {
			// 检查Pod指定的邻近节点是否在指定的需要具有SLA要求的集群节点中
			if _, exist := neilats.config.KubeNodeAddressAndSecret[neiNodeValue]; !exist {
				log.Printf("Pod %s NeiNodeValue %s not exist in Kubernetes Cluster\n", p.Name, neiNodeValue)
				return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Pod %s NeiNodeValue %s not exist in Kubernetes Cluster", p.Name, neiNodeValue))
			}
			// 检查Pod指定的SLAConstraint标签值能否转换为Float64类型
			if _, err := strconv.ParseFloat(slaConstraintValue, 64); err != nil {
				log.Printf("Pod %s SLAConstraintLable Value %s can't convert to Float64\n", p.Name, slaConstraintValue)
				return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Pod %s SLAConstraintLable Value %s can't convert to Float64", p.Name, slaConstraintValue))
			}
			log.Printf("Pod %s NeiNodeLable %s And SLAConstraintLabel %q, pass Prefilter\n", p.Name, neiNodeValue, slaConstraintValue)
			return nil, framework.NewStatus(framework.Success, fmt.Sprintf("Pod %s NeiNodeLable %s And SLAConstraintLabel %q, pass Prefilter", p.Name, neiNodeValue, slaConstraintValue))
		} else {
			// 如果两个标签只存在一个，那么这是无效的情况，需要被过滤掉
			log.Printf("Pod %s haven't NeiNodeLable: nei_node Or SLAConstraintLable: sla\n", p.Name)
			return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Pod %s haven't NeiNodeLable: nei_node Or SLAConstraintLable: sla", p.Name))
		}
	}
	return nil, framework.NewStatus(framework.Success, fmt.Sprintf("Enable SLA: %t, all pod pass Prefilter", neilats.config.EnableSLA))
}

func (neilats *NeilatsRefactorScheduler) PreFilterExtensions() framework.PreFilterExtensions {
	return neilats
}

// AddPod和RemovePod属于PreFilterExtension的实现部分，用于模拟Pod被正式调度之前，Pod被添加到节点和从节点上删除的操作；
// 主要用于统计Pod的资源使用量，用于增量更新资源使用量
// 由于此处实现PreFilter接口只是用于检查Pod是否具有某个标签，不涉及Pod的资源使用量问题，可以简单实现AddPod和RemovePod
func (neilats *NeilatsRefactorScheduler) AddPod(ctx context.Context, cycleState *framework.CycleState, podToSchedule *v1.Pod, podToAdd *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

func (neilats *NeilatsRefactorScheduler) RemovePod(ctx context.Context, cycleState *framework.CycleState, podToSchedule *v1.Pod, podToRemove *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

func (neilats *NeilatsRefactorScheduler) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	// 检查Pod是否存在邻近节点标签
	neiNodeValue, neiNodeExist := pod.GetLabels()["nei_node"]
	// 检查Pod是否存在SLA约束要求标签
	slaConstraintValue, slaExist := pod.GetLabels()["sla"]

	if neilats.config.EnableSLA && neiNodeExist && slaExist {
		// 获取Pod指定的邻近节点与SLA要求
		SLAConstraintValue, err := strconv.ParseFloat(slaConstraintValue, 64)
		if err != nil {
			log.Printf("Failed Convert SLAConstraint Str to Float64:%v\n", err)
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Failed Convert SLAConstraint Str to Float64:%v", err))
		}

		// 进行一次延迟测试文件的下载
		// 后面由于采用了远程打开文件读取数据的方式，所以就不用下载了
		//if err := DownloadLatencyTestResult(neilats.config.KubeNodeAddressAndSecret); err != nil {
		//	return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Failed Download Latency Test Result:%v", err))
		//}

		// 构建一次RttMatrix
		//rttMatrix, err := BuildRttWithOutFile(neilats.config.KubeNodeAddressAndSecret)
		rttMatrix, err := BuildRttWithOutFileOnRemote(neilats.config.KubeNodeAddressAndSecret)
		if err != nil {
			log.Printf("Failed Build RttMatrix:%v\n", err)
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Failed Build RttMatrix:%v", err))
		}

		// 根据RttMatrix，筛选满足Pod的SLA要求的Pod
		if rttMatrix[nodeInfo.Node().Name][neiNodeValue] <= SLAConstraintValue {
			log.Printf("Node %s rtt Satisfy Pod %s SLA Constraint\n", nodeInfo.Node().Name, pod.Name)
			return framework.NewStatus(framework.Success, "Node "+nodeInfo.Node().Name+" rtt Satisfy Pod "+pod.Name+" SLA Constraint")
		} else {
			log.Printf("Node %s rtt UnSatisfy Pod %s SLA Constraint\n", nodeInfo.Node().Name, pod.Name)
			return framework.NewStatus(framework.Unschedulable, "Node "+nodeInfo.Node().Name+" rtt UnSatisfy Pod "+pod.Name+" SLA Constraint")
		}
	}
	// 如果没有开启SLA，则所有节点通过过滤
	return framework.NewStatus(framework.Success, "Node:"+nodeInfo.Node().Name)
}

func (neilats *NeilatsRefactorScheduler) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	LBScore, err := neilats.LBScore(p, nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, err.Error())
	}
	return LBScore, nil
}

// 计算节点的负载均衡得分
func (neilats *NeilatsRefactorScheduler) LBScore(p *v1.Pod, nodeName string) (int64, error) {
	// 1. 计算Pod部署在节点上后，CPU的实际利用率
	podCpuRequest := float64(0)
	if p.Spec.Containers[0].Resources.Requests != nil {
		// 一个Pod的CPU资源请求，包含其中所有container的CPU资源请求之和
		for _, container := range p.Spec.Containers {
			if container.Resources.Requests.Cpu() != nil {
				// .MilliValue() 方法返回以毫秒为单位的CPU值，及CPU的毫核
				podCpuRequest += float64(container.Resources.Requests.Cpu().MilliValue())
			}
		}
	}
	nodeCpuCapacity, err := GetNodeTotalCpu(neilats.config.PrometheusAddress, nodeName)
	if err != nil {
		log.Printf("get node %s cpu capacity failed: %v\n", nodeName, err)
		return 0, fmt.Errorf("get node %s cpu capacity failed: %v", nodeName, err)
	}
	NodeCpuIdleRate, err := GetNodeCpuIdleRate(neilats.config.PrometheusAddress, nodeName)
	if err != nil {
		log.Printf("get node %s cpu idle rate failed: %v\n", nodeName, err)
		return 0, fmt.Errorf("get node %s cpu idle rate failed: %v", nodeName, err)
	}
	nodeCpuUsed := (1 - NodeCpuIdleRate) * nodeCpuCapacity
	realNodeCpuUseRate := (nodeCpuUsed + podCpuRequest) / nodeCpuCapacity

	// 2. 计算Pod部署在节点上后，内存的实际利用率
	podMemRequest := float64(0)
	if p.Spec.Containers[0].Resources.Requests != nil {
		for _, container := range p.Spec.Containers {
			if container.Resources.Requests.Memory() != nil {
				// .Value() 方法返回以字节为单位的内存值
				podMemRequest += float64(container.Resources.Requests.Memory().Value()) / 1024 / 1024
			}
		}
	}
	nodeMemCapacity, err := GetNodeTotalMemory(neilats.config.PrometheusAddress, nodeName)
	if err != nil {
		log.Printf("get node %s mem capacity failed: %v\n", nodeName, err)
		return 0, fmt.Errorf("get node %s mem capacity failed: %v", nodeName, err)
	}
	NodeMemAvailableRate, err := GetNodeMemoryAvailableRate(neilats.config.PrometheusAddress, nodeName)
	if err != nil {
		log.Printf("get node %s mem available rate failed: %v\n", nodeName, err)
		return 0, fmt.Errorf("get node %s mem available rate failed: %v", nodeName, err)
	}
	nodeMemUsed := (1 - NodeMemAvailableRate) * nodeMemCapacity
	realNodeMemUseRate := (nodeMemUsed + podMemRequest) / nodeMemCapacity

	// 3. 计算Pod部署在节点上后，网络的实际利用率
	podNetworkRequest := float64(0)
	if value, exist := p.GetLabels()["network-request"]; exist { // 如果Pod的标签中有"network-request"字段，则取出其值作为网络请求值，其单位为字节/s
		podNetworkRequest, _ = strconv.ParseFloat(value, 64)
	}
	nodeNetworkCapacity := GetNodeTotalNetwork()
	NodeNetworkAvailableRate, err := GetNodeNetworkAvailableRate(neilats.config.PrometheusAddress, nodeName, neilats.config.NetworkDevice[nodeName])
	if err != nil {
		log.Printf("get node %s network available rate failed: %v\n", nodeName, err)
		return 0, fmt.Errorf("get node %s network available rate failed: %v", nodeName, err)
	}
	nodeNetworkUsed := (1 - NodeNetworkAvailableRate) * nodeNetworkCapacity
	realNodeNetworkUseRate := (nodeNetworkUsed + podNetworkRequest) / nodeNetworkCapacity

	// 4. 计算Pod部署在节点上后，磁盘的实际利用率
	podDiskRequest := float64(0)
	// 这里其实本意想如果Pod没有要求disk资源的话，那么就将所有container的镜像大小作为disk请求值，如果有那就加上所有container的镜像大小之和
	if value, exist := p.GetLabels()["disk-request"]; exist { // 如果Pod的标签中有"disk-request"字段，则取出其值作为磁盘请求值，其单位为MB
		podDiskRequest, _ = strconv.ParseFloat(value, 64)
	}
	nodeDiskCapacity, err := GetNodeTotalDisk(neilats.config.PrometheusAddress, nodeName, neilats.config.StorageDevice[nodeName])
	if err != nil {
		log.Printf("get node %s disk capacity failed: %v\n", nodeName, err)
		return 0, fmt.Errorf("get node %s disk capacity failed: %v", nodeName, err)
	}
	NodeDiskAvailableRate, err := GetNodeDiskAvailableRate(neilats.config.PrometheusAddress, nodeName, neilats.config.StorageDevice[nodeName])
	if err != nil {
		log.Printf("get node %s disk available rate failed: %v\n", nodeName, err)
		return 0, fmt.Errorf("get node %s disk available rate failed: %v", nodeName, err)
	}
	nodeDiskUsed := (1 - NodeDiskAvailableRate) * nodeDiskCapacity
	realNodeDiskUseRate := (nodeDiskUsed + podDiskRequest) / nodeDiskCapacity

	// 5. 计算四资源的均值
	avgScore := (realNodeCpuUseRate + realNodeMemUseRate + realNodeNetworkUseRate + realNodeDiskUseRate) / 4
	// 6. 计算四资源的方差
	variance := math.Pow(realNodeCpuUseRate-avgScore, 2) + math.Pow(realNodeMemUseRate-avgScore, 2) + math.Pow(realNodeNetworkUseRate-avgScore, 2) + math.Pow(realNodeDiskUseRate-avgScore, 2)
	// 7. 计算最终负载均衡得分
	LBScore := int64(100 - 100*variance)
	log.Printf("Node Name: %s LB Score: %d\n", nodeName, LBScore)

	return LBScore, nil
}

func (neilats *NeilatsRefactorScheduler) ScoreExtensions() framework.ScoreExtensions {
	return neilats
}

func (neilats *NeilatsRefactorScheduler) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// 默认的排序方法
	//return helper.DefaultNormalizeScore(framework.MaxNodeScore, false, scores)

	// 将原始得分线性映射到[MinNodeScore, MaxNodeScore]，此处即0-100的范围内
	// 找出原始的分数最小值和最大值
	var maxScore int64 = -math.MaxInt64
	var minScore int64 = math.MaxInt64
	for _, nodeScore := range scores {
		if nodeScore.Score > maxScore {
			maxScore = nodeScore.Score
		}
		if nodeScore.Score < minScore {
			minScore = nodeScore.Score
		}
	}

	// 将原始得分映射到新的区间
	oldRange := maxScore - minScore
	newRange := framework.MaxNodeScore - framework.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - minScore) / oldRange * newRange) + framework.MinNodeScore
			log.Printf("Node %s Normalized Score is %d", i, scores[i].Score)
		}
	}
	return nil
}
