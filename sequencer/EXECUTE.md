# Sequencer 執行指南

## 快速開始

### 方法 1：直接運行（開發/測試）

```bash
cd fabricWithEbpfSequencer/sequencer
go run . 8
```

### 方法 2：編譯後執行（生產環境）

```bash
cd fabricWithEbpfSequencer/sequencer

# 編譯
go build -o sequencer .

# 執行
./sequencer 8
```

## 參數說明

- **第一個參數**：`broadcastCount` - 需要廣播到的 orderer 數量
  - `8` = 廣播到所有 8 個 orderer
  - `4` = 只廣播到最後 4 個 orderer
  - 依此類推

## 服務端口

Sequencer 啟動後會同時監聽兩個端口：

| 服務 | 端口 | 說明 |
|------|------|------|
| UDP | 7072 | 處理 UDP 批次請求 |
| gRPC | 7073 | 處理 gRPC 批次請求 |

## 環境變量

可選的環境變量配置：

```bash
# 自定義 gRPC 端口（默認 7073）
export SEQUENCER_GRPC_PORT=7073

# 執行
go run . 8
```

## 執行示例

### 示例 1：基本執行

```bash
cd fabricWithEbpfSequencer/sequencer
go run . 8
```

**預期輸出**：
```
✅ [Sequencer] 已連接到 orderer localhost:3073
✅ [Sequencer] 已連接到 orderer localhost:4073
...
✅ [Sequencer gRPC] 服務端啟動在端口 7073
📥 [Sequencer] 收到封包: ...
```

### 示例 2：編譯後執行

```bash
cd fabricWithEbpfSequencer/sequencer
go build -o sequencer .
./sequencer 8
```

### 示例 3：自定義 gRPC 端口

```bash
cd fabricWithEbpfSequencer/sequencer
export SEQUENCER_GRPC_PORT=8080
go run . 8
```

## 配置 Gateway 使用 gRPC

在 `core.yaml` 中配置：

```yaml
peer:
  gateway:
    sequencerTransport: "grpc"  # 使用 gRPC
    sequencerAddress: "172.20.10.5:7073"  # Sequencer 的 IP 和 gRPC 端口
```

或使用 UDP（默認）：

```yaml
peer:
  gateway:
    sequencerTransport: "udp"  # 使用 UDP
    sequencerAddress: "172.20.10.5:7072"  # Sequencer 的 IP 和 UDP 端口
```

## 驗證服務運行

### 檢查端口是否監聽

```bash
# 檢查 UDP 端口
netstat -an | grep 7072

# 檢查 gRPC 端口
netstat -an | grep 7073
```

### 使用 grpcurl 測試 gRPC（可選）

```bash
# 安裝 grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# 列出服務
grpcurl -plaintext localhost:7073 list

# 應該看到：sequencer.SequencerService
```

## 故障排除

### 問題 1：端口已被占用

**錯誤**：`bind: address already in use`

**解決**：
```bash
# 查找占用端口的進程
lsof -i :7072
lsof -i :7073

# 終止進程或使用其他端口
export SEQUENCER_GRPC_PORT=8080
```

### 問題 2：gRPC 服務未啟動

**檢查**：查看是否有錯誤信息

**解決**：確保 `sequencerpb.pb.go` 和 `sequencerpb_grpc.pb.go` 文件存在

### 問題 3：無法連接到 orderer

**檢查**：確認 orderer 地址和端口正確

**解決**：修改 `sequencer.go` 中的 `addrs` 和 `ports` 數組

## 注意事項

1. **同時運行**：UDP 和 gRPC 會同時運行，無需額外配置
2. **端口衝突**：確保 7072 和 7073 端口未被占用
3. **防火牆**：確保端口開放
4. **生產環境**：建議使用 TLS 加密（當前為 insecure 連接）

