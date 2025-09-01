package neilats_refactor_go

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

// 对于Prometheus的/api/v1/query端点，响应结构体统一的，这个结构体适用于所有PRomeQL查询的响应
type PrometheusQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		}
	}
}

// 获取Prometheus的查询结果
func queryPrometheus(promURL string, promQL string) (*PrometheusQueryResponse, error) {
	// 构造请求URL
	u, _ := url.Parse(promURL)
	u.Path = "/api/v1/query"
	params := url.Values{"query": []string{promQL}}
	u.RawQuery = params.Encode()

	// 发起请求
	resp, err := http.Get(u.String())
	if err != nil {
		log.Printf("Error querying Prometheus: %s \n", err)
		return nil, err
	}
	defer resp.Body.Close()

	// 解析响应
	var result PrometheusQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Println("Error decoding Prometheus response:", err)
		return nil, err
	}

	if result.Status != "success" {
		log.Printf("Prometheus query failed: %s \n", result.Status)
		return nil, fmt.Errorf("Prometheus query failed: %s", result.Status)
	}
	return &result, nil
}

// GetNodeCpuIdleRate 获取指定节点的CPU空闲率
func GetNodeCpuIdleRate(prometheusAddress, nodeName string) (float64, error) {
	// 构造查询语句
	promQL := fmt.Sprintf("sum(increase(node_cpu_seconds_total{mode='idle',instance='%s'}[1m]))/sum(increase(node_cpu_seconds_total{instance='%s'}[1m]))", nodeName, nodeName)
	result, err := queryPrometheus(
		prometheusAddress,
		promQL,
	)
	if err != nil {
		log.Printf("Error querying Prometheus Cpu idle rate: %s \n", err)
		return 0, err
	}

	// 检查结果是否为空，防止访问越界
	if len(result.Data.Result) == 0 {
		log.Printf("get node cpu idle rate response data length is 0!\n")
		return 0, fmt.Errorf("get node cpu idle rate response data length is 0!")
	}

	CpuIdleRate, _ := strconv.ParseFloat(result.Data.Result[0].Value[1].(string), 64)
	log.Printf("CPU idle rate of node: %s is %f\n", nodeName, CpuIdleRate)
	return CpuIdleRate, nil
}

func GetNodeMemoryAvailableRate(prometheusAddress, nodeName string) (float64, error) {
	// 构造查询语句
	promQL := fmt.Sprintf("node_memory_MemAvailable_bytes{instance='%s'}/node_memory_MemTotal_bytes{instance='%s'}", nodeName, nodeName)
	result, err := queryPrometheus(
		prometheusAddress,
		promQL,
	)
	if err != nil {
		log.Printf("Error querying Prometheus Memory available rate: %s \n", err)
		return 0, err
	}

	// 检查结果是否为空，防止访问越界
	if len(result.Data.Result) == 0 {
		log.Printf("get node memory available rate response data length is 0!\n")
		return 0, fmt.Errorf("get node memory available rate response data length is 0!")
	}

	MemoryAvailableRate, _ := strconv.ParseFloat(result.Data.Result[0].Value[1].(string), 64)
	log.Printf("Memory available of node: %s is %f \n", nodeName, MemoryAvailableRate)
	return MemoryAvailableRate, nil
}

func GetNodeNetworkAvailableRate(prometheusAddress, nodeName, networkDevice string) (float64, error) {
	const totalNetworkTraffic = 1000000 // 网络接口的最大传输速度经过测压得出，最大为100 0000字节/s
	receiveRatePromql := fmt.Sprintf("rate(node_network_receive_bytes_total{instance='%s',device='%s'}[1m])", nodeName, networkDevice)
	transmitRatePromql := fmt.Sprintf("rate(node_network_transmit_bytes_total{instance='%s',device='%s'}[1m])", nodeName, networkDevice)
	receiveRateResult, err := queryPrometheus(
		prometheusAddress,
		receiveRatePromql,
	)
	if err != nil {
		log.Printf("Error querying Prometheus network receive rate: %s \n", err)
		return 0, err
	}
	if len(receiveRateResult.Data.Result) == 0 {
		log.Printf("get node network receive rate response data length is 0!\n")
		return 0, fmt.Errorf("get network receive available rate response data length is 0!")
	}

	transmitRateResult, err := queryPrometheus(
		prometheusAddress,
		transmitRatePromql,
	)
	if err != nil {
		log.Printf("Error querying Prometheus network transmit rate: %s \n", err)
		return 0, err
	}
	if len(transmitRateResult.Data.Result) == 0 {
		log.Printf("get node network transmit rate response data length is 0!\n")
		return 0, fmt.Errorf("get node network transmit rate response data length is 0!")
	}

	// 分别获取每秒接受和发送的字节数
	receiveBytes, _ := strconv.ParseFloat(receiveRateResult.Data.Result[0].Value[1].(string), 64)
	transmitBytes, _ := strconv.ParseFloat(transmitRateResult.Data.Result[0].Value[1].(string), 64)
	// 以该节点的接收速率和发送速率均值作为该节点的网络利用率
	networkAvailableRate := (totalNetworkTraffic*2 - receiveBytes - transmitBytes) / totalNetworkTraffic * 2
	log.Printf("Network available of node: %s is %f \n", nodeName, networkAvailableRate)
	return networkAvailableRate, nil
}

func GetNodeDiskAvailableRate(prometheusAddress, nodeName, storageDevice string) (float64, error) {
	promQL := fmt.Sprintf("((node_filesystem_free_bytes{fstype!='',device='%s',instance='%s'} / node_filesystem_size_bytes{fstype!='',device='%s',instance='%s'})) * 100", storageDevice, nodeName, storageDevice, nodeName)
	result, err := queryPrometheus(
		prometheusAddress,
		promQL,
	)
	if err != nil {
		log.Printf("Error querying Prometheus disk available rate: %s \n", err)
		return 0, err
	}
	if len(result.Data.Result) == 0 {
		log.Printf("get node disk avaliable rate response data length is 0!\n")
		return 0, fmt.Errorf("get node disk avaliable rate response data length is 0!")
	}

	DiskAvailableRate, _ := strconv.ParseFloat(result.Data.Result[0].Value[1].(string), 64)
	log.Printf("Disk available of node: %s is %f \n", nodeName, DiskAvailableRate)
	return DiskAvailableRate, nil
}

// GetNodeTotalCpu 获取指定节点的总CPU核数,例如宿主机使用的是8核心，那么每秒提供的CPU总量是 8 * 1000 = 8000毫核
func GetNodeTotalCpu(prometheusAddress, nodeName string) (float64, error) {
	promQL := fmt.Sprintf("count(node_cpu_seconds_total{mode='user',instance='%s'})", nodeName)
	result, err := queryPrometheus(
		prometheusAddress,
		promQL,
	)
	if err != nil {
		log.Printf("Error querying Prometheus total CPU: %s \n", err)
		return 0, err
	}
	if len(result.Data.Result) == 0 {
		log.Printf("get node total cpu response data length is 0!\n")
		return 0, fmt.Errorf("get node total cpu response data length is 0!")
	}

	totalCpu, _ := strconv.ParseFloat(result.Data.Result[0].Value[1].(string), 64)
	totalCpu = totalCpu * 1000 // 单位转换为毫核
	log.Printf("Total CPU of node: %s is %f \n", nodeName, totalCpu)
	return totalCpu, nil
}

func GetNodeTotalMemory(prometheusAddress, nodeName string) (float64, error) {
	promQL := fmt.Sprintf("node_memory_MemTotal_bytes{instance='%s'}", nodeName)
	result, err := queryPrometheus(
		prometheusAddress,
		promQL,
	)
	if err != nil {
		log.Printf("Error querying Prometheus total memory: %s\n", err)
		return 0, err
	}
	if len(result.Data.Result) == 0 {
		log.Printf("get node total memory response data length is 0!\n")
		return 0, fmt.Errorf("get node total memory response data length is 0!")
	}

	totalMemory, _ := strconv.ParseFloat(result.Data.Result[0].Value[1].(string), 64)
	totalMemory = totalMemory / (1024 * 1024) // 单位转换为兆字节
	log.Printf("Total memory of node: %s is %f MB\n", nodeName, totalMemory)
	return totalMemory, nil
}

func GetNodeTotalNetwork() float64 {
	return 1000000 // 网络接口的最大传输速度经过测压得出，最大为100 0000字节/s
}

func GetNodeTotalDisk(prometheusAddress, nodeName, storageDevice string) (float64, error) {
	promQL := fmt.Sprintf("node_filesystem_size_bytes{fstype!='',device='%s',instance='%s'}", storageDevice, nodeName)
	result, err := queryPrometheus(
		prometheusAddress,
		promQL,
	)
	if err != nil {
		log.Printf("Error querying Prometheus total disk: %s\n", err)
		return 0, err
	}
	if len(result.Data.Result) == 0 {
		log.Printf("get node total disk response data length is 0!\n")
		return 0, fmt.Errorf("get node total disk response data length is 0!")
	}

	totalDisk, _ := strconv.ParseFloat(result.Data.Result[0].Value[1].(string), 64)
	totalDisk = totalDisk / (1024 * 1024) // 单位转换为MB
	log.Printf("Total disk of node: %s is %f MB\n", nodeName, totalDisk)
	return totalDisk, nil
}
