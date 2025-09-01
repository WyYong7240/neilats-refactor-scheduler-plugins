package neilats_refactor_go

import (
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"log"
	"os"
	"sigs.k8s.io/scheduler-plugins/apis/config"
	"time"
)

func getSshClient(nodeName, nodeAddress, nodeSecret string) (*ssh.Client, error) {
	// 配置SSH客户端配置
	config := &ssh.ClientConfig{
		// 默认以root登录各个节点
		User: "root",
		Auth: []ssh.AuthMethod{
			// 设置SSH密码
			ssh.Password(nodeSecret),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 这里为了简便忽略了host key检查，在生产环境中不推荐这样做
	}

	// 连接到远程服务器
	client, err := ssh.Dial("tcp", nodeAddress+":22", config)
	if err != nil {
		log.Printf("failed to login Node %q: %v\n", nodeName, err)
		return nil, err
	}
	return client, nil
}

func openFileOnRemote(nodeName, nodeAddress, nodeSecret, remotePath string) (*sftp.File, error) {
	// 连接到远程服务器
	client, err := getSshClient(nodeName, nodeAddress, nodeSecret)
	if err != nil {
		log.Printf("failed to login Node %q: %v\n", nodeName, err)
		return nil, err
	}
	defer client.Close()

	// 创建SFTP客户端
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		log.Printf("after Login Node %q, Failed Create Sftp Client: %v\n", nodeName, err)
		return nil, err
	}
	defer sftpClient.Close()

	// 打开远程主机上的文件
	remoteFile, err := sftpClient.Open(remotePath)
	if err != nil {
		log.Printf("after Create Sftp Client on Node %q, Failed Open remoteFile:%v\n", nodeName, err)
		return nil, err
	}
	return remoteFile, nil
}

func getFileOnRemote(nodeName, nodeAddress, nodeSecret, remotePath, localPath string) error {
	// 连接到远程服务器
	client, err := getSshClient(nodeName, nodeAddress, nodeSecret)
	if err != nil {
		log.Printf("failed to login Node %q: %v\n", nodeName, err)
		return err
	}
	defer client.Close()

	// 创建SFTP客户端
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		log.Printf("after Login Node %q, Failed Create Sftp Client: %v\n", nodeName, err)
		return err
	}
	defer sftpClient.Close()

	// 打开远程主机上的文件
	remoteFile, err := sftpClient.Open(remotePath)
	if err != nil {
		log.Printf("after Create Sftp Client on Node %q, Failed Open remoteFile:%v\n", nodeName, err)
		return err
	}
	defer remoteFile.Close()

	// 创建本地文件
	localFile, err := os.Create(localPath)
	if err != nil {
		log.Printf("failed Create Local file:%v\n", err)
		return err
	}
	defer localFile.Close()

	// 将远程主机上的文件内容复制到本地文件中
	if _, err := io.Copy(localFile, remoteFile); err != nil {
		log.Printf("failed Copy Remote file to Local:%v\n", err)
		return err
	}
	log.Printf("Down Load Node %q File Success!\n", nodeName)
	return nil
}

func writeFileOnRemote(nodeName, nodeAddress, nodeSecret, remotePath, content string) error {
	// 连接到远程服务器
	client, err := getSshClient(nodeName, nodeAddress, nodeSecret)
	if err != nil {
		log.Printf("failed to login Node %q: %v\n", nodeName, err)
		return err
	}
	defer client.Close()

	// 创建SFTP客户端
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		log.Printf("after Login Node %q, Failed Create Sftp Client: %v\n", nodeName, err)
		return err
	}
	defer sftpClient.Close()

	// 在远程主机创建文件
	file, err := sftpClient.Create(remotePath)
	if err != nil {
		log.Printf("after Create SftpClient on Node %q, Failed Create File: %v\n", nodeName, err)
		return err
	}

	// 向创建的文件中写入内容
	if _, err := file.Write([]byte(content)); err != nil {
		log.Printf("after Create File on Node %q, Failed Write File: %v\n", nodeName, err)
		return err
	}

	// 将创建的文件设置为可执行类型
	if err := sftpClient.Chmod(remotePath, 0755); err != nil {
		log.Printf("after Write File on Node %q, Failed Chmod File: %v\n", nodeName, err)
		return err
	}
	return nil
}

func executeCommandOnRemote(nodeName, nodeAddress, nodeSecret, command string) error {
	// 连接到远程服务器
	conn, err := getSshClient(nodeName, nodeAddress, nodeSecret)
	if err != nil {
		log.Printf("Failed to login Node %q: %v\n", nodeName, err)
		return err
	}
	defer conn.Close()

	// 创建一个新的会话
	session, err := conn.NewSession()
	if err != nil {
		log.Printf("After Login Node %q, Failed to Create Session: %v\n", nodeName, err)
		return err
	}
	defer session.Close()

	// 设置输出流
	//session.Stdout = bufio.NewWriter(log.Writer())
	//session.Stderr = bufio.NewWriter(log.Writer())

	// 在远程服务器上执行命令
	if err := session.Run(command); err != nil {
		log.Printf("After Create Session, Failed to Execute command\n")
		return fmt.Errorf("After Create Session, Failed to Execute command")
	}
	log.Printf("Command executed successfully\n")
	return nil
}

func InitNodeLatencyTestTool(KubeNodeAddressAndSecret map[string]config.UserAddressSecretMap) error {
	for k, v := range KubeNodeAddressAndSecret {
		log.Printf("Init Node %q Latency Test Tool\n", k)
		// 先下载测试工具
		command := "apt-get install fping -y"
		if err := executeCommandOnRemote(k, v.NodeAddress, v.NodeSecret, command); err != nil {
			log.Printf("node %q install Latency Test Tool Failed\n", k)
			return fmt.Errorf("node %q install Latency Test Tool Failed", k)
		}
		log.Printf("Install Latency Test Tool Success\n")

		// 创建测试目录并写入延迟测试脚本
		var remoteDirectoryPath string = "/root/ws/network-latency-test"
		createDirectoryCommand := "mkdir -p " + remoteDirectoryPath
		if err := executeCommandOnRemote(k, v.NodeAddress, v.NodeSecret, createDirectoryCommand); err != nil {
			log.Printf("node %q install Latency Test Tool Failed\n", k)
			return fmt.Errorf("node %q install Latency Test Tool Failed", k)
		}
		log.Printf("Node %q Create Latency Test Directory Success\n", k)

		// 定义当前节点要发送的延迟测试目标IP地址，去除当前主机的IP
		var ips = "ips=("
		for node, value := range KubeNodeAddressAndSecret {
			if node != k {
				ips = ips + "\"" + value.NodeAddress + "\" "
			}
		}
		ips = ips + ")\n"
		var scriptContent = `
# 定义存储数据的文件路径

while true; do
    # 获取当前时间戳，格式为YYYY-MM-DD HH:MM:SS
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    for ip in "${ips[@]}"; do
        output_file="latency_results_${ip}.txt"
        # 调用fping命令，-c 1表示只发送一个数据包
        result=$(fping -e $ip 2>/dev/null)
        # 将时间戳和fping结果追加到文件中
        echo "$timestamp, $result" >> $output_file
    done
    # 每隔一段时间（这里是1秒）执行一次，可按需调整
    sleep 10
done
`
		// 生成延迟测量脚本文件内容
		content := "#!/bin/bash\n" + ips + scriptContent
		// 将脚本写入远程主机中
		if err := writeFileOnRemote(k, v.NodeAddress, v.NodeSecret, remoteDirectoryPath+"/latency_test.sh", content); err != nil {
			log.Printf("write script file to remote node %q failed: %v\n", k, err)
			return err
		}
	}
	return nil
}

func StartLatencyTest(KubeNodeAddressAndSecret map[string]config.UserAddressSecretMap) error {
	for k, v := range KubeNodeAddressAndSecret {
		command := "cd /root/ws/network-latency-test && screen -dmS latency_test ./latency_test.sh"
		if err := executeCommandOnRemote(k, v.NodeAddress, v.NodeSecret, command); err != nil {
			log.Printf("start Node %q Latency Test Failed: %v\n", k, err)
			return err
		}
		log.Printf("Success Start Node %q Latency Test!\n", k)
	}
	return nil
}

func StopLatencyTest(KubeNodeAddressAndSecret map[string]config.UserAddressSecretMap) error {
	for k, v := range KubeNodeAddressAndSecret {
		command := "pkill -f latency_test.sh"
		if err := executeCommandOnRemote(k, v.NodeAddress, v.NodeSecret, command); err != nil {
			log.Printf("start Node %q Latency Test Failed: %v\n", k, err)
			return err
		}
		log.Printf("Success Stop Node %q Latency Test!\n", k)
	}
	return nil
}

func DownloadLatencyTestResult(KubeNodeAddressAndSecret map[string]config.UserAddressSecretMap) error {
	localPath := "latency/"
	remoteDirectoryPath := "/root/ws/network-latency-test/"
	for k, v := range KubeNodeAddressAndSecret {
		for nodeName, value := range KubeNodeAddressAndSecret {
			if nodeName != k {
				localFileName := fmt.Sprintf("%s2%s", k, nodeName)
				remoteFileName := fmt.Sprintf("latency_results_%s.txt", value.NodeAddress)
				if err := getFileOnRemote(k, v.NodeAddress, v.NodeSecret, remoteDirectoryPath+remoteFileName, localPath+localFileName); err != nil {
					log.Printf("failed get Latency Test On Node %q\n", k)
					return fmt.Errorf("failed get Latency Test On Node %q", k)
				}
			}
		}
	}
	return nil
}

func GetLatencyTestResultInterval(KubeNodeAddressAndSecret map[string]config.UserAddressSecretMap) error {
	for true {
		time.Sleep(10)
		LatencyFileLock.Lock()
		if err := DownloadLatencyTestResult(KubeNodeAddressAndSecret); err != nil {
			log.Printf("failed Get Latency Test Result Interval:%v\n", err)
			return fmt.Errorf("failed Get Latency Test Result Interval:%v", err)
		}
	}
	return nil
}
