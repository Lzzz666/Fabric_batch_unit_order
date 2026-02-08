/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package gateway

import (
	"context"
	"net"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	gproto "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	peerproto "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/deliverclient/orderers"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/core/scc"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	gdiscovery "github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/gateway/commit"
	"github.com/hyperledger/fabric/internal/pkg/gateway/config"
	"github.com/hyperledger/fabric/internal/pkg/gateway/ledger"
	"google.golang.org/grpc"
)

var logger = flogging.MustGetLogger("gateway")

// TxnPoolInterface defines the interface for transaction pool operations
type TxnPoolInterface interface {
	Put(hash []byte, envelope interface{}, ttl interface{}) error
	Get(hash []byte) (interface{}, bool)
}

// GossipBroadcaster defines the interface for gossip broadcasting
type GossipBroadcaster interface {
	// Gossip sends a message to other peers in the network
	Gossip(msg *gproto.GossipMessage)
	// PeersOfChannel returns the NetworkMembers subscribed to the channel
	PeersOfChannel(gcommon.ChannelID) []gdiscovery.NetworkMember
}

// Server represents the GRPC server for the Gateway.
type Server struct {
	registry         *registry
	commitFinder     CommitFinder
	policy           ACLChecker
	options          config.Options
	logger           *flogging.FabricLogger
	ledgerProvider   ledger.Provider
	getChannelConfig channelConfigGetter
	UdpGateway       *net.UDPConn
	GrpcGateway      grpc.ClientConnInterface // gRPC 客戶端連接
	batchCollector   *SimpleBatchCollector    // 批次收集器
	txnPool          TxnPoolInterface         // TxnPool for hash-only mode
	gossipService    GossipBroadcaster        // Gossip service for broadcasting transactions
}

type EndorserServerAdapter struct {
	Server peerproto.EndorserServer
}

func (e *EndorserServerAdapter) ProcessProposal(ctx context.Context, req *peerproto.SignedProposal, _ ...grpc.CallOption) (*peerproto.ProposalResponse, error) {
	return e.Server.ProcessProposal(ctx, req)
}

type CommitFinder interface {
	TransactionStatus(ctx context.Context, channelName string, transactionID string) (*commit.Status, error)
}

type ACLChecker interface {
	CheckACL(policyName string, channelName string, data interface{}) error
}

type channelConfigGetter func(cid string) channelconfig.Resources

// CreateServer creates an embedded instance of the Gateway.
func CreateServer(
	localEndorser peerproto.EndorserServer,
	discovery Discovery,
	peerInstance *peer.Peer,
	secureOptions *comm.SecureOptions,
	policy ACLChecker,
	localMSPID string,
	options config.Options,
	systemChaincodes scc.BuiltinSCCs,
) *Server {
	adapter := &ledger.PeerAdapter{
		Peer: peerInstance,
	}
	notifier := commit.NewNotifier(adapter)

	server := newServer(
		&EndorserServerAdapter{
			Server: localEndorser,
		},
		discovery,
		commit.NewFinder(adapter, notifier),
		policy,
		adapter,
		peerInstance.GossipService.SelfMembershipInfo(),
		localMSPID,
		secureOptions,
		options,
		systemChaincodes,
		peerInstance.OrdererEndpointOverrides,
		peerInstance.GetChannelConfig,
		peerInstance.GossipService, // Pass GossipService for transaction broadcasting
	)

	peerInstance.AddConfigCallbacks(server.registry.configUpdate)

	return server
}

// SetTxnPool sets the TxnPool for storing transactions in hash-only mode
func (s *Server) SetTxnPool(pool TxnPoolInterface) {
	s.txnPool = pool
	logger.Infof("TxnPool set for Gateway hash-only mode")
}

func newServer(localEndorser peerproto.EndorserClient,
	discovery Discovery,
	finder CommitFinder,
	policy ACLChecker,
	ledgerProvider ledger.Provider,
	localInfo gdiscovery.NetworkMember,
	localMSPID string,
	secureOptions *comm.SecureOptions,
	options config.Options,
	systemChaincodes scc.BuiltinSCCs,
	ordererEndpointOverrides map[string]*orderers.Endpoint,
	getChannelConfig channelConfigGetter,
	gossipService GossipBroadcaster,
) *Server {

	s := &Server{
		registry: &registry{
			localEndorser: &endorser{
				client:         localEndorser,
				endpointConfig: &endpointConfig{pkiid: localInfo.PKIid, address: localInfo.Endpoint, logAddress: localInfo.Endpoint, mspid: localMSPID},
			},
			discovery: discovery,
			logger:    logger,
			endpointFactory: &endpointFactory{
				timeout:                  options.DialTimeout,
				clientCert:               secureOptions.Certificate,
				clientKey:                secureOptions.Key,
				ordererEndpointOverrides: ordererEndpointOverrides,
			},
			remoteEndorsers:    map[string]*endorser{},
			channelInitialized: map[string]bool{},
			systemChaincodes:   systemChaincodes,
			localProvider:      ledgerProvider,
		},
		commitFinder:     finder,
		policy:           policy,
		options:          options,
		logger:           logger,
		ledgerProvider:   ledgerProvider,
		getChannelConfig: getChannelConfig,
		gossipService:    gossipService,
	}

	// 嘗試連接到 sequencer，但不阻塞啟動（連接失敗時會在首次使用時重試）
	if err := s.connect(); err != nil {
		// 只記錄警告，不影響 peer 啟動
		// sequencer 連接將在首次使用時自動重試
		s.logger.Warnw("Failed to connect to sequencer during startup", "error", err)
	}

	return s
}
