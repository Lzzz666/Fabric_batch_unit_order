# Sequencer gRPC 支持

## 概述

Sequencer 現在同時支持 UDP 和 gRPC 兩種傳輸方式。gRPC 適合處理大型 batch，避免 UDP MTU 限制導致的分片問題。

**重要**：Sequencer 會**同時啟動** UDP 和 gRPC 服務端，無需額外配置即可使用。

## 生成 gRPC 代碼（如果尚未生成）

如果 `sequencerpb.pb.go` 和 `sequencerpb_grpc.pb.go` 文件不存在，需要從 proto 文件生成：

```bash
cd sequencer
protoc --go_out=. --go-grpc_out=. sequencer.proto

# 生成後需要將文件移動並修改包名（已自動完成）
# mv github.com/hyperledger/fabric/sequencer/sequencer.pb.go sequencerpb.pb.go
# mv github.com/hyperledger/fabric/sequencer/sequencer_grpc.pb.go sequencerpb_grpc.pb.go
# 並將包名改為 main
```

## 執行 Sequencer

### 基本執行

```bash
cd sequencer
go run . <broadcastCount>
```

或編譯後執行：

```bash
cd sequencer
go build -o sequencer .
./sequencer <broadcastCount>
```

**參數說明**：
- `<broadcastCount>`：需要廣播到的 orderer 數量（例如：8 表示廣播到所有 8 個 orderer）

### 執行示例

```bash
# 廣播到所有 8 個 orderer
go run . 8

# 或編譯後執行
go build -o sequencer .
./sequencer 8
```

### 環境變量配置（可選）

```bash
# 自定義 gRPC 端口（默認 7073）
export SEQUENCER_GRPC_PORT=7073

# 執行
go run . 8
```

### 執行後會看到

```
✅ [Sequencer] 已連接到 orderer localhost:3073
✅ [Sequencer] 已連接到 orderer localhost:4073
...
✅ [Sequencer gRPC] 服務端啟動在端口 7073
📥 [Sequencer] 收到封包: ...
```

**注意**：Sequencer 會同時監聽：
- **UDP 端口 7072**：處理 UDP 請求
- **gRPC 端口 7073**：處理 gRPC 請求（自動啟動）

## 配置

### Gateway 配置

在 `core.yaml` 或環境變量中配置：

```yaml
peer:
  gateway:
    sequencerTransport: "grpc"  # 或 "udp"（默認）
    sequencerAddress: "172.20.10.5:7073"  # gRPC 端口（UDP 使用 7072）
```

### Sequencer 配置

Sequencer 會同時監聽：
- UDP: 端口 7072（默認）
- gRPC: 端口 7073（默認，可通過環境變量 `SEQUENCER_GRPC_PORT` 配置）

## 使用方式

### 使用 UDP（默認）

```yaml
peer:
  gateway:
    sequencerTransport: "udp"
    sequencerAddress: "172.20.10.5:7072"
```

### 使用 gRPC

```yaml
peer:
  gateway:
    sequencerTransport: "grpc"
    sequencerAddress: "172.20.10.5:7073"
```

## 優勢對比

| 特性 | UDP | gRPC |
|------|-----|------|
| 傳輸可靠性 | 無保證 | 可靠（TCP） |
| 大封包支持 | 受 MTU 限制（~1472 bytes） | 無限制 |
| 分片問題 | 可能丟失 | 自動處理 |
| 延遲 | 低 | 稍高 |
| 適用場景 | 小 batch、低延遲 | 大 batch、可靠性要求高 |

## 完整執行流程示例

### 1. 啟動 Sequencer

```bash
cd fabricWithEbpfSequencer/sequencer
go run . 8
```

### 2. 配置 Gateway 使用 gRPC

在 `core.yaml` 中配置：

```yaml
peer:
  gateway:
    sequencerTransport: "grpc"
    sequencerAddress: "172.20.10.5:7073"  # 替換為實際的 sequencer IP
```

### 3. 重啟 Peer

重啟 peer 節點以應用新配置。

## 驗證 gRPC 服務

可以使用 `grpcurl` 工具測試 gRPC 服務是否正常：

```bash
# 安裝 grpcurl（如果沒有）
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# 列出服務
grpcurl -plaintext localhost:7073 list

# 測試 SubmitBatch（需要提供正確的 batch_data）
grpcurl -plaintext -d '{"batch_data": "..."}' localhost:7073 sequencer.SequencerService/SubmitBatch
```

## 注意事項

1. **端口配置**：確保 UDP (7072) 和 gRPC (7073) 端口未被占用
2. **防火牆**：確保端口 7072 和 7073 開放
3. **生產環境**：建議使用 TLS 加密（當前使用 insecure 連接）
4. **同時運行**：UDP 和 gRPC 會同時運行，可以根據需要選擇使用哪種方式

