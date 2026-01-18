/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// TransportType 定義傳輸類型
type TransportType string

const (
	TransportUDP  TransportType = "udp"
	TransportGRPC TransportType = "grpc"
)

// GatewayOptions is used to configure the gateway settings.
type Options struct {
	// GatewayEnabled is used to enable the gateway service.
	Enabled bool
	// EndorsementTimeout is used to specify the maximum time to wait for endorsement responses from external peers.
	EndorsementTimeout time.Duration
	// BroadcastTimeout is used to specify the maximum time to wait for responses from ordering nodes.
	BroadcastTimeout time.Duration
	// DialTimeout is used to specify the maximum time to wait for connecting to external peers and orderer nodes.
	DialTimeout time.Duration
	// SequencerTransport 指定連接到 sequencer 的傳輸方式：udp 或 grpc
	SequencerTransport TransportType
	// SequencerAddress sequencer 的地址（UDP 或 gRPC）
	SequencerAddress string
}

var defaultOptions = Options{
	Enabled:            true,
	EndorsementTimeout: 10 * time.Second,
	BroadcastTimeout:   10 * time.Second,
	DialTimeout:        30 * time.Second,
	SequencerTransport: TransportGRPC, // 默認使用 UDP
	SequencerAddress:   "172.20.10.5:7073",
	// SequencerAddress: "10.140.0.10:7073",
}

// DefaultOptions gets the default Gateway configuration Options
func GetOptions(v *viper.Viper) Options {
	options := defaultOptions

	// 🔍 調試：檢查 viper 配置
	fmt.Printf("🔍 [Config Debug] Viper 配置文件: %s\n", v.ConfigFileUsed())
	fmt.Printf("🔍 [Config Debug] 檢查所有 gateway 相關配置鍵:\n")

	// 檢查所有可能的配置鍵
	allKeys := v.AllKeys()
	gatewayKeys := []string{}
	for _, key := range allKeys {
		if len(key) >= 13 && key[:13] == "peer.gateway." {
			gatewayKeys = append(gatewayKeys, key)
		}
	}
	fmt.Printf("   找到的 gateway 配置鍵: %v\n", gatewayKeys)

	// 檢查特定鍵是否存在
	fmt.Printf("   peer.gateway.sequencerTransport IsSet: %v\n", v.IsSet("peer.gateway.sequencerTransport"))
	fmt.Printf("   peer.gateway.sequencerAddress IsSet: %v\n", v.IsSet("peer.gateway.sequencerAddress"))

	// 嘗試直接讀取值（即使 IsSet 為 false）
	transportStr := v.GetString("peer.gateway.sequencerTransport")
	addressStr := v.GetString("peer.gateway.sequencerAddress")
	fmt.Printf("   直接讀取 sequencerTransport: '%s'\n", transportStr)
	fmt.Printf("   直接讀取 sequencerAddress: '%s'\n", addressStr)

	if v.IsSet("peer.gateway.enabled") {
		options.Enabled = v.GetBool("peer.gateway.enabled")
	}
	if v.IsSet("peer.gateway.endorsementTimeout") {
		options.EndorsementTimeout = v.GetDuration("peer.gateway.endorsementTimeout")
	}
	if v.IsSet("peer.gateway.broadcastTimeout") {
		options.BroadcastTimeout = v.GetDuration("peer.gateway.broadcastTimeout")
	}
	if v.IsSet("peer.gateway.dialTimeout") {
		options.DialTimeout = v.GetDuration("peer.gateway.dialTimeout")
	}

	// 使用直接讀取的值，即使 IsSet 為 false（可能是配置格式問題）
	if transportStr != "" {
		fmt.Printf("🔍 [Config] 讀取 sequencerTransport: %s\n", transportStr)
		if transportStr == "grpc" {
			options.SequencerTransport = TransportGRPC
			fmt.Printf("✅ [Config] 設置為 gRPC 傳輸\n")
		} else {
			options.SequencerTransport = TransportUDP
			fmt.Printf("✅ [Config] 設置為 UDP 傳輸\n")
		}
	} else if v.IsSet("peer.gateway.sequencerTransport") {
		transportStr := v.GetString("peer.gateway.sequencerTransport")
		fmt.Printf("🔍 [Config] 讀取 sequencerTransport: %s\n", transportStr)
		if transportStr == "grpc" {
			options.SequencerTransport = TransportGRPC
			fmt.Printf("✅ [Config] 設置為 gRPC 傳輸\n")
		} else {
			options.SequencerTransport = TransportUDP
			fmt.Printf("✅ [Config] 設置為 UDP 傳輸\n")
		}
	} else {
		fmt.Printf("⚠️  [Config] sequencerTransport 未設置，使用默認值: %s\n", options.SequencerTransport)
	}

	if addressStr != "" {
		options.SequencerAddress = addressStr
		fmt.Printf("🔍 [Config] 讀取 sequencerAddress: %s\n", options.SequencerAddress)
	} else if v.IsSet("peer.gateway.sequencerAddress") {
		options.SequencerAddress = v.GetString("peer.gateway.sequencerAddress")
		fmt.Printf("🔍 [Config] 讀取 sequencerAddress: %s\n", options.SequencerAddress)
	} else {
		fmt.Printf("⚠️  [Config] sequencerAddress 未設置，使用默認值: %s\n", options.SequencerAddress)
	}

	return options
}
