package neilats_refactor_go

import (
	"bufio"
	"fmt"
	"github.com/pkg/sftp"
	"log"
	"os"
	"sigs.k8s.io/scheduler-plugins/apis/config"
	"strconv"
	"strings"
	"time"
)

const MEASUREMENT_TIME = 10
const LAST_N_LINES = 10

func BuildRttWithOutFileOnRemote(KubeNodeAddressAndSecret map[string]config.UserAddressSecretMap) (map[string]map[string]float64, error) {
	nodeNum := len(KubeNodeAddressAndSecret)

	// 初始化rtt矩阵
	rttMatrix := make(map[string]map[string]float64, nodeNum)
	for k, _ := range KubeNodeAddressAndSecret {
		rttMatrix[k] = make(map[string]float64, nodeNum)
	}

	for fromNode, fromUAS := range KubeNodeAddressAndSecret {
		for toNode, toUAS := range KubeNodeAddressAndSecret {
			if fromNode == toNode {
				rttMatrix[fromNode][toNode] = 0.0
			} else {
				value, err := getMeanOfLatencyFromSSH(fromNode, fromUAS.NodeAddress, fromUAS.NodeSecret, toUAS.NodeAddress)
				if err != nil {
					log.Printf("get MeanOfLatencyFromTxt Failed:%v \n", err)
					return nil, err
				} else if value >= 99 {
					rttMatrix[fromNode][toNode] = rttMatrix[toNode][fromNode]
				} else {
					rttMatrix[fromNode][toNode] = value
				}
			}
		}
	}

	// rttMatrix构建完成，返回给调度插件
	return rttMatrix, nil
}

func BuildRttWithOutFile(KubeNodeAddressAndSecret map[string]config.UserAddressSecretMap) (map[string]map[string]float64, error) {
	nodeNum := len(KubeNodeAddressAndSecret)

	// 初始化rtt矩阵
	rttMatrix := make(map[string]map[string]float64, nodeNum)
	for k, _ := range KubeNodeAddressAndSecret {
		rttMatrix[k] = make(map[string]float64, nodeNum)
	}

	for fromNode, _ := range KubeNodeAddressAndSecret {
		for toNode, _ := range KubeNodeAddressAndSecret {
			if fromNode == toNode {
				rttMatrix[fromNode][toNode] = 0.0
			} else {
				value, err := getMeanOfLatencyFromTxt(fromNode, toNode)
				if err != nil {
					log.Printf("get MeanOfLatencyFromTxt Failed:%v \n", err)
					return nil, err
				} else if value >= 99 {
					rttMatrix[fromNode][toNode] = rttMatrix[toNode][fromNode]
				} else {
					rttMatrix[fromNode][toNode] = value
				}
			}
		}
	}

	// rttMatrix构建完成，返回给调度插件
	return rttMatrix, nil
}

func BuildRtt(KubeNodeAddressAndSecret map[string]config.UserAddressSecretMap) error {
	resultFile := "./latency/rtt_matrix.txt"
	timeOut := MEASUREMENT_TIME
	nodeNum := len(KubeNodeAddressAndSecret)

	// 获取需要构建RTT矩阵的节点名称
	nodes := make([]string, nodeNum)
	for k, _ := range KubeNodeAddressAndSecret {
		nodes = append(nodes, k)
	}

	// 初始化rtt矩阵
	rttMatrix := make([][]float64, nodeNum)
	for i := range rttMatrix {
		rttMatrix[i] = make([]float64, nodeNum)
	}

	for true {
		if timeOut > 0 {
			timeOut -= 1
			time.Sleep(1)
		} else {
			// 开始刷新RTT矩阵
			for i := range rttMatrix {
				for j := range rttMatrix {
					if i == j {
						rttMatrix[i][j] = 0.0
					} else {
						// 获取延迟结果文件的文件锁
						LatencyFileLock.Lock()
						value, err := getMeanOfLatencyFromTxt(nodes[i], nodes[j])
						// 延迟结果文件锁解锁
						LatencyFileLock.Unlock()
						if err != nil {
							log.Printf("get MeanOfLatencyFromTxt Failed:%v\n", err)
							return err
						} else if value >= 99 {
							rttMatrix[i][j] = rttMatrix[j][i]
						} else {
							rttMatrix[i][j] = value
						}
					}
				}
			}
			// 要写回RTT矩阵了,在获取rttMatrix文件锁之前，先构造好rttMatrix矩阵的新内容
			// 遍历二维数组并构建rttMatrix文件内容
			var lines []string
			for _, row := range rttMatrix {
				var line []string
				for _, value := range row {
					// 将所有值字符串化
					line = append(line, fmt.Sprintf("%f", value))
				}
				// 将一行中所有值变为一个字符串，中间用","间隔
				lines = append(lines, strings.Join(line, ","))
			}
			// 将所有行变为一个字符串，中间用换行符间隔
			content := strings.Join(lines, "\n")
			// 获取RTT矩阵文件的文件锁
			RttMatrixFileLock.Lock()
			// 由于后面的操作都涉及到rttMatrix文件的写入，所以推迟文件锁的解锁操作到函数结束
			defer RttMatrixFileLock.Unlock()
			rttFile, err := os.OpenFile(resultFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Printf("open RTT File Failed!:%v\n", err)
				return err
			}
			defer rttFile.Close()

			// 写入rttMatrix
			_, err = rttFile.WriteString(content)
			if err != nil {
				log.Printf("failed to write to rtt-matrix file:%v\n", err)
				return err
			}
		}
	}
	return nil
}

func getMeanOfLatencyFromSSH(nodeFromName, nodeFromAddress, nodeFromSecret, nodeToAddress string) (float64, error) {
	resultFilePath := fmt.Sprintf("/root/ws/network-latency-test/latency_results_%s.txt", nodeToAddress)

	// 连接到远程服务器
	client, err := getSshClient(nodeFromName, nodeFromAddress, nodeFromSecret)
	if err != nil {
		log.Printf("failed to login Node %q: %v\n", nodeFromName, err)
		return 0, err
	}
	defer client.Close()

	// 创建SFTP客户端
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		log.Printf("after Login Node %q, Failed Create Sftp Client: %v\n", nodeFromName, err)
		return 0, err
	}
	defer sftpClient.Close()

	// 打开远程主机上的文件
	resultFile, err := sftpClient.Open(resultFilePath)
	if err != nil {
		log.Printf("after Create Sftp Client on Node %q, Failed Open remoteFile:%v\n", nodeFromName, err)
		return 0, err
	}
	defer resultFile.Close()

	// 用于对所有延迟数据求和后求平均
	var latencySum float64 = 0.0
	// 保存每行数据字符串
	var lines []string
	scanner := bufio.NewScanner(resultFile)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > LAST_N_LINES {
			// 仅保留最后10行数据，代码含义为切片从下标为1的元素开始到最后一个元素作为一个新切片
			lines = lines[1:]
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("get Last N Lines in ResultFile Failed:%v\n", err)
		return 0, err
	}

	for _, line := range lines {
		splitedLine := strings.Split(line, " ")
		dataStr := splitedLine[len(splitedLine)-2]
		dataStr, _ = strings.CutPrefix(dataStr, "(")
		latencyData, err := strconv.ParseFloat(dataStr, 64)
		if err != nil {
			log.Printf("convert Data String to Float Failed:%v\n", err)
			return 0, err
		}
		latencySum += latencyData
	}
	// 返回延迟平均值
	return latencySum / float64(len(lines)), nil
}

func getMeanOfLatencyFromTxt(nodeFrom, nodeTo string) (float64, error) {
	resultFilePath := fmt.Sprintf("./latency/%s2%s", nodeFrom, nodeTo)
	// 用于对所有延迟数据求和后求平均
	var latencySum float64 = 0.0
	resultFile, err := os.Open(resultFilePath)
	if err != nil {
		log.Printf("Failed Open File %s2%s.txt\n", nodeFrom, nodeTo)
		return 0.0, fmt.Errorf("Failed Open File %s2%s.txt", nodeFrom, nodeTo)
	}
	defer resultFile.Close()

	// 保存每行数据字符串
	var lines []string
	scanner := bufio.NewScanner(resultFile)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > LAST_N_LINES {
			// 仅保留最后10行数据，代码含义为切片从下标为1的元素开始到最后一个元素作为一个新切片
			lines = lines[1:]
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("get Last N Lines in ResultFile Failed:%v\n", err)
		return 0, err
	}

	for _, line := range lines {
		splitedLine := strings.Split(line, " ")
		dataStr := splitedLine[len(splitedLine)-2]
		dataStr, _ = strings.CutPrefix(dataStr, "(")
		latencyData, err := strconv.ParseFloat(dataStr, 64)
		if err != nil {
			log.Printf("convert Data String to Float Failed:%v\n", err)
			return 0, err
		}
		latencySum += latencyData
	}
	// 返回延迟平均值
	return latencySum / float64(len(lines)), nil
}
