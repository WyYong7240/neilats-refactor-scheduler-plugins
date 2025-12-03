package neilats_refactor_go

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/config"
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
var _ framework.PreScorePlugin = &NeilatsRefactorScheduler{}

// 负载均衡得分、ADF网络平稳性得分、FutureScore未来链路得分存储
var LBScoreMap map[string]float64
var ADFScoreMap map[string]float64
var FutureScoreMap map[string]float64

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
		LstmAdfModuleAddress: "http://192.168.3.226:30080", // 默认LSTM预测服务与ADF分数计算服务地址
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

		}
		if args.LstmAdfModuleAddress != "" {
			defaultConfig.LstmAdfModuleAddress = args.LstmAdfModuleAddress
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

		// 构建一次RttMatrix
		rttMatrix, err := BuildRttWithOutFileOnRemote(neilats.config.KubeNodeAddressAndSecret)
		if err != nil {
			log.Printf("Failed Build RttMatrix:%v\n", err)
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Failed Build RttMatrix:%v", err))
		}

		// 根据RttMatrix，筛选满足Pod的SLA要求的Node
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

// 预先计算每个节点的三种得分
func (neilats *NeilatsRefactorScheduler) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	// 初始化各个节点不同得分Map
	nodeNum := len(nodes)
	LBScoreMap = make(map[string]float64, nodeNum)
	ADFScoreMap = make(map[string]float64, nodeNum)
	FutureScoreMap = make(map[string]float64, nodeNum)

	// 找出各个分数值的极值，用于归一化
	var LBMax, ADFMax, FutureMax float64 = float64(math.MinInt64), float64(math.MinInt64), float64(math.MinInt64)
	var LBMin, ADFMin, FutureMin float64 = math.MaxFloat64, math.MaxFloat64, math.MaxFloat64

	for _, node := range nodes {
		// 预先计算各个节点的负载均衡得分
		nodeLBscore, err := neilats.LBScore(pod, node.Name)
		if err != nil {
			log.Printf("Pod %s compute Node %s LBScore Failed: %v", pod.Name, node.Name, err)
		}
		LBScoreMap[node.Name] = float64(nodeLBscore)
		LBMin = math.Min(LBMin, nodeLBscore)
		LBMax = math.Max(LBMax, nodeLBscore)

		if neilats.config.EnableSLA {
			// 安全检查已经在PreFilter阶段通过了，不用检查了
			neiNodeValue, _ := pod.GetLabels()["nei_node"]
			SLAConstraintValue, _ := strconv.ParseFloat(pod.GetLabels()["sla"], 64)
			// 获取ADF分数
			ADFScore, err := neilats.ADFScore(neiNodeValue, node.Name)
			if err != nil {
				log.Printf("Get ADFScore Failed, PodName:%s, NodeFrom:%s, NodeTo:%s.", pod.Name, neiNodeValue, node.Name)
				ADFScore = 0
			}
			ADFScoreMap[node.Name] = ADFScore
			ADFMin = math.Min(ADFMin, ADFScore)
			ADFMax = math.Max(ADFMax, ADFScore)

			// 获取未来网络延迟预测分数
			FutureScore, err := neilats.FutureScore(neiNodeValue, node.Name, SLAConstraintValue)
			if err != nil {
				log.Printf("Get FutureScore Failed, PodName:%s, NodeFrom:%s, NodeTo:%s, SLAConstraint:%f.", pod.Name, neiNodeValue, node.Name, SLAConstraintValue)
				FutureScore = 0
			}
			FutureScoreMap[node.Name] = FutureScore
			FutureMin = math.Min(FutureMin, FutureScore)
			FutureMax = math.Max(FutureMax, FutureScore)
		}
	}

	// 将三种类型的分数分别归一化, 如果某一种分数的值都一样，统一设置为100
	for _, node := range nodes {
		LBScore := LBScoreMap[node.Name]
		if LBMax == LBMin {
			LBScoreMap[node.Name] = 100
		} else {
			LBScoreMap[node.Name] = ((LBScore-LBMin)/(LBMax-LBMin) + 1) * 100
		}

		if neilats.config.EnableSLA {
			ADFScore := ADFScoreMap[node.Name]
			if ADFMax == ADFMin {
				ADFScoreMap[node.Name] = 100
			} else {
				ADFScoreMap[node.Name] = ((ADFScore-ADFMin)/(ADFMax-ADFMin) + 1) * 100
			}

			FutureScore := FutureScoreMap[node.Name]
			if FutureMax == FutureMin {
				FutureScoreMap[node.Name] = 100
			} else {
				FutureScoreMap[node.Name] = ((FutureScore-FutureMin)/(FutureMax-FutureMin) + 1) * 100
			}
		}
	}
	return framework.NewStatus(framework.Success, "LBScore、ADFScore、FutureScore Compute Complete.")
}

func (neilats *NeilatsRefactorScheduler) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	if neilats.config.EnableSLA {
		// 归一化已经完成， 只需要求该节点各个分数的均方值
		lbscore := LBScoreMap[nodeName]
		adfscore := ADFScoreMap[nodeName]
		futurescore := FutureScoreMap[nodeName]
		return int64(math.Cbrt(lbscore * adfscore * futurescore)), nil
	} else {
		return int64(LBScoreMap[nodeName]), nil
	}
}

// 计算节点的负载均衡得分
func (neilats *NeilatsRefactorScheduler) LBScore(p *v1.Pod, nodeName string) (float64, error) {
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
	LBScore := float64(100 - 100*variance)
	log.Printf("Node Name: %s LB Score: %d\n", nodeName, LBScore)

	return LBScore, nil
}

func (neilats *NeilatsRefactorScheduler) ADFScore(nodeFrom, nodeTo string) (float64, error) {
	nodeFromUASMap := neilats.config.KubeNodeAddressAndSecret[nodeFrom]
	nodeToUASMap := neilats.config.KubeNodeAddressAndSecret[nodeTo]
	// 获取两个节点之间的最后30行延迟数据，依据此得到ADF分数
	lastNLatency, err := getLastLinesLatencyFromSSH(nodeFrom, nodeFromUASMap.NodeAddress, nodeFromUASMap.NodeSecret, nodeToUASMap.NodeAddress, 30)
	if err != nil {
		log.Printf("ADF Get Last N Lines Latency Failed, From Node %s To %s:%v", nodeFrom, nodeTo, err)
		return 0, err
	}
	// 定义获取ADF分数的结构体
	type ADFRequest struct {
		Latency []float64 `json:"latency"`
	}

	reqBody := ADFRequest{
		Latency: lastNLatency,
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		log.Printf("Error ADF Marshaling JSON: %v\n", err)
		return 0, err
	}
	ADFScore, err := neilats.sendHTTPRequest(jsonData, "/adf_score")
	if err != nil {
		log.Printf("Error ADF Get ADFScore from RemoteServer: %v", err)
		return 0, err
	}
	return ADFScore, nil
}

func (neilats *NeilatsRefactorScheduler) FutureScore(nodeFrom, nodeTo string, slaTime float64) (float64, error) {
	nodeFromUASMap := neilats.config.KubeNodeAddressAndSecret[nodeFrom]
	nodeToUASMap := neilats.config.KubeNodeAddressAndSecret[nodeTo]
	lastNLatency, err := getLastLinesLatencyFromSSH(nodeFrom, nodeFromUASMap.NodeAddress, nodeFromUASMap.NodeSecret, nodeToUASMap.NodeAddress, 30)
	if err != nil {
		log.Printf("ADF Get Last N Lines Latency Failed, From Node %s To %s:%v", nodeFrom, nodeTo, err)
		return 0, err
	}
	// 定义未来链路得分结构体
	type FutureRequest struct {
		Latency []float64 `json:"latency"`
		SLATime float64   `json:"sla_time"`
	}

	reqBody := FutureRequest{
		Latency: lastNLatency,
		SLATime: slaTime,
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		log.Printf("Error FutureScore Marshaling JSON: %v\n", err)
		return 0, err
	}
	FutureScore, err := neilats.sendHTTPRequest(jsonData, "/predict/"+nodeFrom+"2"+nodeTo)
	if err != nil {
		log.Printf("Error Future Get FutureScore from RemoteServer: %v", err)
		return 0, err
	}
	return FutureScore, nil
}

func (neilats *NeilatsRefactorScheduler) sendHTTPRequest(reqBodyData []byte, serverAddress string) (float64, error) {
	url := neilats.config.LstmAdfModuleAddress + serverAddress

	// 创建请求
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBodyData))
	if err != nil {
		log.Printf("Error Creating requests: %v\n", err)
		return 0, err
	}

	// 设置Header
	request.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		log.Printf("Error Sending Request: %v\n", err)
		return 0, err
	}
	defer response.Body.Close()

	// 定义响应体结构体，用于接收数据
	type ResponseType struct {
		Score float64
	}

	// 读取响应体
	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Printf("Error Read Response Body: %v\n", err)
	}
	// 解析响应体
	var result ResponseType
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Printf("Error Failed to pares JSON: %v\n", err)
		log.Printf("Raw response %s\n", string(body))
		return 0, err
	}
	return result.Score, nil
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
