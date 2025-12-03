package neilats_refactor_go

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/sftp"
	"sigs.k8s.io/scheduler-plugins/apis/config"
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

func getMeanOfLatencyFromSSH(nodeFromName, nodeFromAddress, nodeFromSecret, nodeToAddress string) (float64, error) {
	lastNLatency, err := getLastLinesLatencyFromSSH(nodeFromName, nodeFromAddress, nodeFromSecret, nodeToAddress, 30)
	if err != nil {
		log.Printf("Get last N Lines Latency Data Failed:%v\n", err)
	}

	var latencySum float64 = 0
	for _, line := range lastNLatency {
		latencySum += line
	}

	// 返回延迟平均值
	return latencySum / float64(len(lastNLatency)), nil
}

func getLastLinesLatencyFromSSH(nodeFromName, nodeFromAddress, nodeFromSecret, nodeToAddress string, lastNLines int) ([]float64, error) {
	resultFilePath := fmt.Sprintf("/root/ws/network-latency-test/latency_results_%s.txt", nodeToAddress)

	// 连接到远程服务器
	client, err := getSshClient(nodeFromName, nodeFromAddress, nodeFromSecret)
	if err != nil {
		log.Printf("failed to login Node %q: %v\n", nodeFromName, err)
		return nil, err
	}
	defer client.Close()

	// 创建SFTP客户端
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		log.Printf("after Login Node %q, Failed Create Sftp Client: %v\n", nodeFromName, err)
		return nil, err
	}
	defer sftpClient.Close()

	// 打开远程主机上的文件
	resultFile, err := sftpClient.Open(resultFilePath)
	if err != nil {
		log.Printf("after Create Sftp Client on Node %q, Failed Open remoteFile:%v\n", nodeFromName, err)
		return nil, err
	}
	defer resultFile.Close()

	// 保存每行数据字符串
	var lines []string
	scanner := bufio.NewScanner(resultFile)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > lastNLines {
			// 仅保留最后lastN行数据，代码含义为切片从下标为1的元素开始到最后一个元素作为一个新切片
			lines = lines[1:]
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("get Last N Lines in ResultFile Failed:%v\n", err)
		return nil, err
	}

	// 保存最后N行的延迟数据值
	var lastNLatency []float64

	for _, line := range lines {
		splitedLine := strings.Split(line, " ")
		dataStr := splitedLine[len(splitedLine)-2]
		dataStr, _ = strings.CutPrefix(dataStr, "(")
		latencyData, err := strconv.ParseFloat(dataStr, 64)
		if err != nil {
			log.Printf("convert Data String to Float Failed:%v\n", err)
			return nil, err
		}
		lastNLatency = append(lastNLatency, latencyData)
	}
	// 返回最后N行的延迟值
	return lastNLatency, nil
}

func getMeanOfLatencyFromTxt(nodeFrom, nodeTo string) (float64, error) {
	resultFilePath := fmt.Sprintf("./latency/%s2%s", nodeFrom, nodeTo)
	// 用于对所有延迟数据求和后求平均
	var latencySum float64 = 0.0
	resultFile, err := os.Open(resultFilePath)
	if err != nil {
		log.Printf("Failed Open File %s2%s.txt\n", nodeFrom, nodeTo)
		return 0.0, fmt.Errorf("failed open file %s2%s.txt", nodeFrom, nodeTo)
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
