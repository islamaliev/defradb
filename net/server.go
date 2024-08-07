// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package net

import (
	"context"
	"crypto/ecdh"
	"fmt"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	libpeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/sourcenetwork/corelog"
	rpc "github.com/sourcenetwork/go-libp2p-pubsub-rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/errors"
	"github.com/sourcenetwork/defradb/event"
	coreblock "github.com/sourcenetwork/defradb/internal/core/block"
	pb "github.com/sourcenetwork/defradb/net/pb"
)

// Server is the request/response instance for all P2P RPC communication.
// Implements gRPC server. See net/pb/net.proto for corresponding service definitions.
//
// Specifically, server handles the push/get request/response aspects of the RPC service
// but not the API calls.
type server struct {
	peer *Peer
	opts []grpc.DialOption

	topics map[string]pubsubTopic
	// replicators is a map from collectionName => peerId
	replicators map[string]map[peer.ID]struct{}
	mu          sync.Mutex

	conns map[libpeer.ID]*grpc.ClientConn

	pb.UnimplementedServiceServer

	sessions []session
}

const sessionTimeout = 5 * time.Second

type session struct {
	id         string
	privateKey *ecdh.PrivateKey
	t          time.Time
}

func newSession(id string, privateKey *ecdh.PrivateKey) session {
	return session{id: id, privateKey: privateKey, t: time.Now()}
}

// pubsubTopic is a wrapper of rpc.Topic to be able to track if the topic has
// been subscribed to.
type pubsubTopic struct {
	*rpc.Topic
	subscribed bool
}

// newServer creates a new network server that handle/directs RPC requests to the
// underlying DB instance.
func newServer(p *Peer, opts ...grpc.DialOption) (*server, error) {
	s := &server{
		peer:        p,
		conns:       make(map[libpeer.ID]*grpc.ClientConn),
		topics:      make(map[string]pubsubTopic),
		replicators: make(map[string]map[peer.ID]struct{}),
	}

	cred := insecure.NewCredentials()
	defaultOpts := []grpc.DialOption{
		s.getLibp2pDialer(),
		grpc.WithTransportCredentials(cred),
	}

	s.opts = append(defaultOpts, opts...)

	return s, nil
}

func (s *server) extractSessionAndRemoveOldOnes(id string) *session {
	var result *session
	swapLast := func(i int) {
		s.sessions[i] = s.sessions[len(s.sessions)-1]
		s.sessions = s.sessions[:len(s.sessions)-1]
	}
	for i, session := range s.sessions {
		if session.id == id {
			tmpSession := session
			result = &tmpSession
			swapLast(i)
		} else if time.Since(session.t) > sessionTimeout {
			swapLast(i)
		}
	}
	return result
}

// GetDocGraph receives a get graph request
func (s *server) GetDocGraph(
	ctx context.Context,
	req *pb.GetDocGraphRequest,
) (*pb.GetDocGraphReply, error) {
	return nil, nil
}

// PushDocGraph receives a push graph request
func (s *server) PushDocGraph(
	ctx context.Context,
	req *pb.PushDocGraphRequest,
) (*pb.PushDocGraphReply, error) {
	return nil, nil
}

// GetLog receives a get log request
func (s *server) GetLog(ctx context.Context, req *pb.GetLogRequest) (*pb.GetLogReply, error) {
	return nil, nil
}

// PushLog receives a push log request
func (s *server) PushLog(ctx context.Context, req *pb.PushLogRequest) (*pb.PushLogReply, error) {
	pid, err := peerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	headCID, err := cid.Cast(req.Body.Cid)
	if err != nil {
		return nil, err
	}
	docID, err := client.NewDocIDFromString(string(req.Body.DocID))
	if err != nil {
		return nil, err
	}
	byPeer, err := libpeer.Decode(req.Body.Creator)
	if err != nil {
		return nil, err
	}
	block, err := coreblock.GetFromBytes(req.Body.Log.Block)
	if err != nil {
		return nil, err
	}

	log.InfoContext(ctx, "Received pushlog",
		corelog.Any("PeerID", pid.String()),
		corelog.Any("Creator", byPeer.String()),
		corelog.Any("DocID", docID.String()))

	log.InfoContext(ctx, "Starting DAG sync",
		corelog.Any("PeerID", pid.String()),
		corelog.Any("DocID", docID.String()))

	err = syncDAG(ctx, s.peer.bserv, block)
	if err != nil {
		return nil, err
	}

	log.InfoContext(ctx, "DAG sync complete",
		corelog.Any("PeerID", pid.String()),
		corelog.Any("DocID", docID.String()))

	// Once processed, subscribe to the DocID topic on the pubsub network unless we already
	// subscribed to the collection.
	if !s.hasPubSubTopic(string(req.Body.SchemaRoot)) {
		err = s.addPubSubTopic(docID.String(), true)
		if err != nil {
			return nil, err
		}
	}

	s.peer.bus.Publish(event.NewMessage(event.MergeName, event.Merge{
		DocID:      docID.String(),
		ByPeer:     byPeer,
		FromPeer:   pid,
		Cid:        headCID,
		SchemaRoot: string(req.Body.SchemaRoot),
	}))

	return &pb.PushLogReply{}, nil
}

// GetHeadLog receives a get head log request
func (s *server) GetHeadLog(
	ctx context.Context,
	req *pb.GetHeadLogRequest,
) (*pb.GetHeadLogReply, error) {
	return nil, nil
}

// addPubSubTopic subscribes to a topic on the pubsub network
func (s *server) addPubSubTopic(topic string, subscribe bool) error {
	if s.peer.ps == nil {
		return nil
	}

	log.InfoContext(s.peer.ctx, "Adding pubsub topic",
		corelog.String("PeerID", s.peer.PeerID().String()),
		corelog.String("Topic", topic))

	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.topics[topic]; ok {
		// When the topic was previously set to publish only and we now want to subscribe,
		// we need to close the existing topic and create a new one.
		if !t.subscribed && subscribe {
			if err := t.Close(); err != nil {
				return err
			}
		} else {
			return nil
		}
	}

	t, err := rpc.NewTopic(s.peer.ctx, s.peer.ps, s.peer.host.ID(), topic, subscribe)
	if err != nil {
		return err
	}

	t.SetEventHandler(s.pubSubEventHandler)
	t.SetMessageHandler(s.pubSubMessageHandler)
	s.topics[topic] = pubsubTopic{
		Topic:      t,
		subscribed: subscribe,
	}
	return nil
}

// hasPubSubTopic checks if we are subscribed to a topic.
func (s *server) hasPubSubTopic(topic string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.topics[topic]
	return ok
}

// removePubSubTopic unsubscribes to a topic
func (s *server) removePubSubTopic(topic string) error {
	if s.peer.ps == nil {
		return nil
	}

	log.InfoContext(s.peer.ctx, "Removing pubsub topic",
		corelog.String("PeerID", s.peer.PeerID().String()),
		corelog.String("Topic", topic))

	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.topics[topic]; ok {
		delete(s.topics, topic)
		return t.Close()
	}
	return nil
}

func (s *server) removeAllPubsubTopics() error {
	if s.peer.ps == nil {
		return nil
	}

	log.InfoContext(s.peer.ctx, "Removing all pubsub topics",
		corelog.String("PeerID", s.peer.PeerID().String()))

	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.topics {
		delete(s.topics, id)
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}

// publishLog publishes the given PushLogRequest object on the PubSub network via the
// corresponding topic
func (s *server) publishLog(ctx context.Context, topic string, req *pb.PushLogRequest) error {
	log.InfoContext(ctx, "Publish log",
		corelog.String("PeerID", s.peer.PeerID().String()),
		corelog.String("Topic", topic))

	if s.peer.ps == nil { // skip if we aren't running with a pubsub net
		return nil
	}
	s.mu.Lock()
	t, ok := s.topics[topic]
	s.mu.Unlock()
	if !ok {
		err := s.addPubSubTopic(topic, false)
		if err != nil {
			return errors.Wrap(fmt.Sprintf("failed to created single use topic %s", topic), err)
		}
		return s.publishLog(ctx, topic, req)
	}

	data, err := req.MarshalVT()
	if err != nil {
		return errors.Wrap("failed to marshal pubsub message", err)
	}

	if _, err := t.Publish(ctx, data, rpc.WithIgnoreResponse(true)); err != nil {
		return errors.Wrap(fmt.Sprintf("failed publishing to thread %s", topic), err)
	}
	return nil
}

// pubSubMessageHandler handles incoming PushLog messages from the pubsub network.
func (s *server) pubSubMessageHandler(from libpeer.ID, topic string, msg []byte) ([]byte, error) {
	log.InfoContext(s.peer.ctx, "Received new pubsub event",
		corelog.String("PeerID", s.peer.PeerID().String()),
		corelog.Any("SenderId", from),
		corelog.String("Topic", topic))

	req := new(pb.PushLogRequest)
	if err := proto.Unmarshal(msg, req); err != nil {
		log.ErrorContextE(s.peer.ctx, "Failed to unmarshal pubsub message %s", err)
		return nil, err
	}
	ctx := grpcpeer.NewContext(s.peer.ctx, &grpcpeer.Peer{
		Addr: addr{from},
	})
	if _, err := s.PushLog(ctx, req); err != nil {
		return nil, errors.Wrap(fmt.Sprintf("Failed pushing log for doc %s", topic), err)
	}
	return nil, nil
}

// pubSubEventHandler logs events from the subscribed DocID topics.
func (s *server) pubSubEventHandler(from libpeer.ID, topic string, msg []byte) {
	log.InfoContext(s.peer.ctx, "Received new pubsub event",
		corelog.String("PeerID", s.peer.PeerID().String()),
		corelog.Any("SenderId", from),
		corelog.String("Topic", topic),
		corelog.String("Message", string(msg)),
	)
	evt := event.NewMessage(event.PubSubName, event.PubSub{
		Peer: from,
	})
	s.peer.bus.Publish(evt)
}

// addr implements net.Addr and holds a libp2p peer ID.
type addr struct{ id libpeer.ID }

// Network returns the name of the network that this address belongs to (libp2p).
func (a addr) Network() string { return "libp2p" }

// String returns the peer ID of this address in string form (B58-encoded).
func (a addr) String() string { return a.id.String() }

// peerIDFromContext returns peer ID from the GRPC context
func peerIDFromContext(ctx context.Context) (libpeer.ID, error) {
	ctxPeer, ok := grpcpeer.FromContext(ctx)
	if !ok {
		return "", errors.New("unable to identify stream peer")
	}
	pid, err := libpeer.Decode(ctxPeer.Addr.String())
	if err != nil {
		return "", errors.Wrap("parsing stream PeerID", err)
	}
	return pid, nil
}

func (s *server) updatePubSubTopics(evt event.P2PTopic) {
	for _, topic := range evt.ToAdd {
		err := s.addPubSubTopic(topic, true)
		if err != nil {
			log.ErrorContextE(s.peer.ctx, "Failed to add pubsub topic.", err)
		}
	}

	for _, topic := range evt.ToRemove {
		err := s.removePubSubTopic(topic)
		if err != nil {
			log.ErrorContextE(s.peer.ctx, "Failed to remove pubsub topic.", err)
		}
	}
	s.peer.bus.Publish(event.NewMessage(event.P2PTopicCompletedName, nil))
}

func (s *server) updateReplicators(evt event.Replicator) {
	if len(evt.Schemas) == 0 {
		// remove peer from store
		s.peer.host.Peerstore().ClearAddrs(evt.Info.ID)
	} else {
		// add peer to store
		s.peer.host.Peerstore().AddAddrs(evt.Info.ID, evt.Info.Addrs, peerstore.PermanentAddrTTL)
		// connect to the peer
		if err := s.peer.Connect(s.peer.ctx, evt.Info); err != nil {
			log.ErrorContextE(s.peer.ctx, "Failed to connect to replicator peer", err)
		}
	}

	// update the cached replicators
	s.mu.Lock()
	for schema, peers := range s.replicators {
		if _, hasSchema := evt.Schemas[schema]; hasSchema {
			s.replicators[schema][evt.Info.ID] = struct{}{}
			delete(evt.Schemas, schema)
		} else {
			if _, exists := peers[evt.Info.ID]; exists {
				delete(s.replicators[schema], evt.Info.ID)
			}
		}
	}
	for schema := range evt.Schemas {
		if _, exists := s.replicators[schema]; !exists {
			s.replicators[schema] = make(map[peer.ID]struct{})
		}
		s.replicators[schema][evt.Info.ID] = struct{}{}
	}
	s.mu.Unlock()

	if evt.Docs != nil {
		for update := range evt.Docs {
			if err := s.pushLog(s.peer.ctx, update, evt.Info.ID); err != nil {
				log.ErrorContextE(
					s.peer.ctx,
					"Failed to replicate log",
					err,
					corelog.Any("CID", update.Cid),
					corelog.Any("PeerID", evt.Info.ID),
				)
			}
		}
	}
	s.peer.bus.Publish(event.NewMessage(event.ReplicatorCompletedName, nil))
}
