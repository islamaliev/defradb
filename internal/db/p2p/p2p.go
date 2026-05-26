// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package p2p

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/sourcenetwork/corekv"

	"github.com/sourcenetwork/corelog"
	"github.com/sourcenetwork/immutable"
	lens "github.com/sourcenetwork/lens/host-go/node"

	"github.com/sourcenetwork/defradb/acp/dac"
	"github.com/sourcenetwork/defradb/acp/identity"
	acpTypes "github.com/sourcenetwork/defradb/acp/types"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/errors"
	"github.com/sourcenetwork/defradb/event"
	coreblock "github.com/sourcenetwork/defradb/internal/core/block"
	"github.com/sourcenetwork/defradb/internal/datastore"
	acpDB "github.com/sourcenetwork/defradb/internal/db/acp"
	"github.com/sourcenetwork/defradb/internal/db/description"
	"github.com/sourcenetwork/defradb/internal/db/p2p/protocol"
	"github.com/sourcenetwork/defradb/internal/debug"
	"github.com/sourcenetwork/defradb/internal/kms"
	"github.com/sourcenetwork/defradb/internal/se"
	"github.com/sourcenetwork/defradb/internal/telemetry"
)

var (
	log    = corelog.NewLogger("p2p")
	tracer = telemetry.NewTracer()
)

type (
	peerID        = string
	collectionID  = string
	addresses     = []string
	peerAddresses = map[peerID]addresses
)

const networkRequestTimeout = 10 * time.Second

// PushToReplicatorsHandler is called when documents are pushed to replicators.
// Implementations can perform additional actions like generating SE artifacts.
type PushToReplicatorsHandler interface {
	HandlePushToReplicators(ctx context.Context, evt event.Update) error
}

// DB hold the database related methods that are required by P2P.
type DB interface {
	// NewTxn returns a new transaction on the root store that may be managed externally.
	NewTxn(readOnly bool) (client.Txn, error)
	// GetNodeIdentity returns the current node identity.
	GetNodeIdentity(ctx context.Context) (immutable.Option[identity.PublicRawIdentity], error)
	// GetNodeIdentityToken returns an identity token for the given audience.
	GetNodeIdentityToken(ctx context.Context, audience immutable.Option[string]) ([]byte, error)
	// GetCollections returns all collections and their descriptions matching the given options
	// that currently exist within this [Store].
	GetCollections(
		ctx context.Context,
		opts ...options.Enumerable[options.GetCollectionsOptions],
	) ([]client.Collection, error)
	// Merge initiates a merge of the DAG and caches the resulting values into the datastore.
	Merge(ctx context.Context, evt event.Merge) error
	// Events returns the event bus for the database.
	Events() event.Bus
	// RetryIntervals returns the replicator retry configuration.
	RetryIntervals() []time.Duration
	// NodeACP returns the NodeACP implementation configured on the database.
	NodeACP() acpDB.NACInfo
	// DocumentACP returns the DocumentACP implementation configured on the database.
	DocumentACP() immutable.Option[dac.DocumentACP]
	// Rootstore returns the rootstore
	Rootstore() corekv.TxnStore
	// Multistore returns the multistore
	Multistore() *datastore.Multistore
	// P2PBlockSyncTimeout is the timeout duration for syncing block links.
	P2PBlockSyncTimeout() time.Duration
	// SearchableEncryptionKey returns the searchable encryption key if configured.
	SearchableEncryptionKey() []byte
	// MaxTxnRetries returns the maximum number of transaction retries.
	MaxTxnRetries() int
}

type P2P struct {
	identityProtocol   *protocol.IdentityProtocol
	replicatorProtocol protocol.CommChannel[protocol.PushLogRequest, protocol.PushLogReply]

	ctx                  context.Context
	db                   DB
	lens                 *lens.Node
	collectionRepository *description.CollectionRepository
	host                 client.Host
	kms                  kms.Service

	// replicators is a map from collection CollectionID => peerId => list of addresses.
	// This is a cached in-memory copy of the persisted replicators in the database.
	// It is used to quickly find the replicators for a given collection when sending updates.
	// The map is protected by repMu.
	replicators map[collectionID]peerAddresses
	repMu       sync.Mutex

	peerIdentities map[peerID]identity.Identity
	piMu           sync.RWMutex

	// The intervals at which to retry replicator failures.
	// For example, this can define an exponential backoff strategy.
	retryIntervals   []time.Duration
	handleRetryMutex sync.Mutex

	// a cid queue for the processing of Pushlogs
	processQueue *processQueue

	// timeout duration for syncing block links.
	syncBlockLinkTimeout time.Duration

	// seCoordinator manages searchable encryption artifact replication
	seCoordinator *se.Coordinator

	// pushHandlers are called when documents are pushed to replicators
	pushHandlers []PushToReplicatorsHandler

	// trc is a per-instance debug tracer prefixed with the node's short peer ID
	// so sender vs receiver lines are visually distinguishable.
	trc *debug.Tracer

	// actor is a short tag used in the global Timeline (e.g. "NAwNJ").
	actor string
}

// pushLogCommProcessor implements CommProcessor for push log functionality
type pushLogCommProcessor struct {
	p2p *P2P
}

func (proc *pushLogCommProcessor) ProcessRequest(
	ctx context.Context,
	req protocol.PushLogRequest,
) (protocol.PushLogReply, error) {
	return protocol.PushLogReply{}, proc.p2p.processPushlogRequest(ctx, &req, true)
}

// peerEventHandlingHost wraps a Host to add a PeerEventHandler to pubsub topics.
// It's added so that KMS doesn't need to bother with event handling and keeps it independent
// from the event bus.
type peerEventHandlingHost struct {
	client.Host
	eventHandler client.PeerEventHandler
}

func (h *peerEventHandlingHost) AddPubSubTopic(
	topicName string,
	subscribe bool,
	handler client.PubsubMessageHandler,
) error {
	return h.Host.AddPubSubTopic(topicName, subscribe, handler, h.eventHandler)
}

// New returns a new configured P2P instance.
func New(
	ctx context.Context,
	db DB,
	lens *lens.Node,
	host client.Host,
	nodeIdentity immutable.Option[identity.Identity],
	collectionRetriever kms.CollectionRetriever,
	collectionRepository *description.CollectionRepository,
) (*P2P, error) {
	// Build a per-instance tracer prefix from a short suffix of the node's peer ID
	// (last 4 chars of the libp2p PeerID base58). This lets sender/receiver lines
	// be told apart visually in interleaved output.
	pidStr := host.ID()
	suffix := pidStr
	if len(suffix) > 4 {
		suffix = suffix[len(suffix)-4:]
	}

	p := P2P{
		ctx:                  ctx,
		db:                   db,
		lens:                 lens,
		collectionRepository: collectionRepository,
		host:                 host,
		identityProtocol:     protocol.NewIdentityProtocol(host, db.GetNodeIdentityToken),
		replicators:          make(map[string]map[string][]string),
		peerIdentities:       make(map[string]identity.Identity),
		retryIntervals:       db.RetryIntervals(),
		processQueue:         newProcessQueue(),
		syncBlockLinkTimeout: db.P2PBlockSyncTimeout(),
		trc:                  debug.NewTracer("p2p-" + suffix),
		actor:                "N-" + suffix,
	}
	p.replicatorProtocol = protocol.NewCommChannel(host, "rep", &pushLogCommProcessor{p2p: &p})

	host.SetBlockAccessFunc(p.hasAccess)

	err := p.host.AddPubSubTopic(docSyncTopic, true, p.docSyncMessageHandler, p.peerEventHandler)
	if err != nil {
		return nil, err
	}

	err = p.host.AddPubSubTopic(syncBranchableCollectionTopic, true, p.syncBranchableCollectionMessageHandler,
		p.peerEventHandler)
	if err != nil {
		return nil, err
	}

	go p.handleReplicatorRetries(ctx)
	err = p.loadAndPublishReplicators(ctx)
	if err != nil {
		return nil, err
	}
	err = p.loadAndPublishP2PCollections(ctx)
	if err != nil {
		return nil, err
	}
	err = p.loadAndPublishP2PDocuments(ctx)
	if err != nil {
		return nil, err
	}

	if nodeIdentity.HasValue() {
		p.kms, err = kms.NewPubSubService(
			ctx,
			host.ID(),
			&peerEventHandlingHost{
				Host:         host,
				eventHandler: p.peerEventHandler,
			},
			datastore.EncstoreFrom(db.Rootstore()),
			db.NodeACP,
			db.DocumentACP(),
			collectionRetriever,
			nodeIdentity,
		)
		if err != nil {
			return nil, err
		}
	}

	if len(db.SearchableEncryptionKey()) > 0 {
		coord, err := se.NewCoordinator(&p, host, db, db.SearchableEncryptionKey(), nodeIdentity)
		if err != nil {
			return nil, err
		}
		p.seCoordinator = coord
		p.AddPushToReplicatorsHandler(coord)
	}

	return &p, nil
}

func (p *P2P) KMS() kms.Service {
	return p.kms
}

func (p *P2P) SECoordinator() *se.Coordinator {
	return p.seCoordinator
}

// AddPushToReplicatorsHandler registers a handler that will be called when documents are pushed to replicators.
func (p *P2P) AddPushToReplicatorsHandler(handler PushToReplicatorsHandler) {
	p.pushHandlers = append(p.pushHandlers, handler)
}

func (p *P2P) PeerInfo() ([]string, error) {
	return p.host.Addresses()
}

func (p *P2P) ActivePeers(ctx context.Context) ([]string, error) {
	return p.host.ActivePeers()
}

// Connect initiates a connection to the peer with the given addresses.
func (p *P2P) Connect(ctx context.Context, addresses []string) error {
	return p.host.Connect(ctx, addresses)
}

func (p *P2P) updateReplicators(ctx context.Context, id string, addresses []string, collectionIDs map[string]struct{}) {
	if len(collectionIDs) == 0 {
		// remove peer from store
		if err := p.host.Disconnect(ctx, id); err != nil {
			log.ErrorE("Failed to disconnect from replicator peer", err)
		}
	} else {
		if err := p.host.Connect(ctx, addresses); err != nil {
			log.ErrorE("Failed to connect to replicator peer", err, corelog.Any("Addresses", addresses))
		}
	}

	// update the cached replicators
	p.repMu.Lock()
	for collectionID, peers := range p.replicators {
		if _, hasID := collectionIDs[collectionID]; hasID {
			p.replicators[collectionID][id] = addresses
			delete(collectionIDs, collectionID)
		} else {
			if _, exists := peers[id]; exists {
				delete(p.replicators[collectionID], id)
			}
		}
	}
	for collectionID := range collectionIDs {
		if _, exists := p.replicators[collectionID]; !exists {
			p.replicators[collectionID] = make(map[string][]string)
		}
		p.replicators[collectionID][id] = addresses
	}
	p.repMu.Unlock()
}

// hasAccess checks if the requesting peer has access to the given cid.
//
// This is used as a filter in bitswap to determine if we should send the block to the requesting peer.
func (p *P2P) hasAccess(ctx context.Context, pid string, c cid.Cid) bool {
	defer p.trc.Enter("hasAccess (sender filter)")()
	p.trc.Println("from peer=%s cid=%s", pid, c.String())
	debug.DefaultTimeline.Log(p.actor, "[send] hasAccess request cid=%s", shortCID(c.String()))
	if !p.db.DocumentACP().HasValue() {
		p.trc.Println("no document ACP -> allow")
		return true
	}

	rawblock, err := p.db.Multistore().Blockstore().Get(ctx, c)
	if err != nil {
		p.trc.Println("blockstore Get FAILED err=%v -> deny", err)
		if !ipld.IsNotFound(err) {
			log.ErrorE("Failed to get block", err)
		}
		return false
	}

	_, err = coreblock.GetSignatureBlockFromBytes(rawblock.RawData())
	if err == nil {
		// If the block is a signature block, we can safely send it to the requesting peer.
		p.trc.Println("block is signature block -> allow (no ACP check)")
		debug.DefaultTimeline.Log(p.actor, "[send] hasAccess: signature block -> allow (no ACP)")
		return true
	}

	block, err := coreblock.GetFromBytes(rawblock.RawData())
	if err != nil {
		if strings.Contains(err.Error(), "invalid key: \"modules\" is not a field in type Block") ||
			strings.Contains(err.Error(), "invalid key: \"lens\" is not a field in type Block") ||
			strings.Contains(err.Error(), "invalid key: \"wasmBytes\" is not a field in type Block") ||
			strings.Contains(err.Error(), "invalid key: \"chunks\" is not a field in type Block") {
			// There are currently 3 kinds of Lens blocks that may be synced, these three error checks
			// are for those blocks.  If the block is a Lens block, we can safely send it to the
			// requesting peer.
			return true
		}
		log.ErrorE("Failed to get doc from block", err)
		return false
	}

	if block.Delta.IsDefinition() {
		p.trc.Println("block is a definition delta -> allow")
		return true
	}

	ident, err := p.db.GetNodeIdentity(p.ctx)
	if err != nil {
		p.trc.Println("GetNodeIdentity FAILED err=%v -> deny", err)
		log.ErrorE("Failed to get node identity", err)
		return false
	}
	getColOpts := options.GetCollections().SetCollectionID(block.Delta.GetCollectionVersionID())
	if ident.HasValue() {
		getColOpts = getColOpts.SetIdentity(identity.FromDID(ident.Value().DID))
	}

	cols, err := p.db.GetCollections(ctx, getColOpts)
	if err != nil {
		p.trc.Println("GetCollections FAILED err=%v -> deny", err)
		log.ErrorE("Failed to get collections", err)
		return false
	}
	if len(cols) == 0 {
		p.trc.Println("no collections found -> deny")
		log.Info("No collections found",
			corelog.Any("Collection Version ID", block.Delta.GetCollectionVersionID()))
		return false
	}

	// If the requesting peer is in the replicators list for that collection, then they have accesp.
	p.repMu.Lock()
	if peerList, ok := p.replicators[cols[0].CollectionID()]; ok {
		_, exists := peerList[pid]
		if exists {
			p.repMu.Unlock()
			p.trc.Println("peer is in replicators list -> allow (no ACP check)")
			debug.DefaultTimeline.Log(p.actor, "[send] hasAccess: peer in replicators list -> allow (no ACP)")
			return true
		}
	}
	p.repMu.Unlock()
	p.trc.Println("peer NOT in replicators list -> will check ACP")
	debug.DefaultTimeline.Log(p.actor, "[send] hasAccess: peer NOT in replicators list -> SourceHub check")

	identFunc := func() immutable.Option[identity.Identity] {
		p.piMu.RLock()
		ident, ok := p.peerIdentities[pid]
		p.piMu.RUnlock()
		if !ok {
			ctx, cancel := context.WithTimeout(ctx, networkRequestTimeout)
			defer cancel()
			resp, err := p.identityProtocol.GetIdentity(ctx, pid)
			if err != nil {
				log.ErrorE("Failed to get identity", err)
				return immutable.None[identity.Identity]()
			}
			ident, err = identity.FromToken(resp.IdentityToken)
			if err != nil {
				log.ErrorE("Failed to parse identity token", err)
				return immutable.None[identity.Identity]()
			}
			tokenIdent, ok := ident.(identity.TokenIdentity)
			if !ok {
				log.ErrorE("Identity is not of type TokenIdentity", nil, corelog.String("Actual", fmt.Sprintf("%T", ident)))
				return immutable.None[identity.Identity]()
			}
			err = identity.VerifyAuthToken(tokenIdent, p.host.ID())
			if err != nil {
				log.ErrorE("Failed to verify auth token", err)
				return immutable.None[identity.Identity]()
			}
			p.piMu.Lock()
			p.peerIdentities[pid] = ident
			p.piMu.Unlock()
		}
		return immutable.Some(ident)
	}

	p.trc.Println("calling SourceHub CheckDocAccessWithIdentityFunc (sender side)")
	debug.DefaultTimeline.Log(p.actor, "[send] -> SourceHub CheckDocAccess (start)")
	peerHasAccess, err := acpDB.CheckDocAccessWithIdentityFunc(
		ctx,
		identFunc,
		p.db.NodeACP(),
		p.db.DocumentACP().Value(),
		cols[0], // For now we assume there is only one collection.
		acpTypes.DocumentReadPerm,
		string(block.Delta.GetDocID()),
	)
	if err != nil {
		p.trc.Println("SourceHub check ERROR: %v", err)
		debug.DefaultTimeline.Log(p.actor, "[send] <- SourceHub ERROR: %v", err)
		log.ErrorE("Failed to check access", err)
		return false
	}
	p.trc.Println("SourceHub answer: peerHasAccess=%v", peerHasAccess)
	debug.DefaultTimeline.Log(p.actor, "[send] <- SourceHub answer: peerHasAccess=%v (serve block)", peerHasAccess)

	return peerHasAccess
}

// trySelfHasAccess checks if the local node has access to the given block.
//
// This is a best-effort check and returns true unless we explicitly find that the local node
// doesn't have access or if we get an error. The node sending is ultimately responsible for
// ensuring that the recipient has access.
func (p *P2P) trySelfHasAccess(ctx context.Context, block *coreblock.Block, collectionID string) (bool, error) {
	defer p.trc.Enter("trySelfHasAccess (receiver gate)")()
	p.trc.Println("docID=%s collectionID=%s", string(block.Delta.GetDocID()), collectionID)
	if !p.db.DocumentACP().HasValue() {
		p.trc.Println("no document ACP -> allow")
		return true, nil
	}

	ident, err := p.db.GetNodeIdentity(ctx)
	if err != nil {
		return false, err
	}

	// The collection lookup is a local operation on this node — authorise it
	// as the node itself so NAC sees a known identity rather than "anonymous".
	getColOpts := options.GetCollections().SetCollectionID(collectionID)
	if ident.HasValue() {
		getColOpts = getColOpts.SetIdentity(identity.FromDID(ident.Value().DID))
	}
	cols, err := p.db.GetCollections(ctx, getColOpts)
	if err != nil {
		return false, err
	}
	if len(cols) == 0 {
		return false, client.ErrCollectionNotFound
	}
	if !ident.HasValue() {
		return true, nil
	}

	p.trc.Println("calling SourceHub CheckDocAccessWithIdentityFunc (receiver side, identity=%s)", ident.Value().DID)
	debug.DefaultTimeline.Log(p.actor, "[recv] -> SourceHub CheckDocAccess (start, identity=node-self)")
	peerHasAccess, err := acpDB.CheckDocAccessWithIdentityFunc(
		ctx,
		func() immutable.Option[identity.Identity] {
			return immutable.Some(identity.FromDID(ident.Value().DID))
		},
		p.db.NodeACP(),
		p.db.DocumentACP().Value(),
		cols[0], // For now we assume there is only one collection.
		acpTypes.DocumentReadPerm,
		string(block.Delta.GetDocID()),
	)
	if err != nil {
		p.trc.Println("SourceHub check ERROR: %v", err)
		debug.DefaultTimeline.Log(p.actor, "[recv] <- SourceHub ERROR: %v", err)
		return false, err
	}
	p.trc.Println("SourceHub answer: peerHasAccess=%v", peerHasAccess)
	debug.DefaultTimeline.Log(p.actor, "[recv] <- SourceHub answer: peerHasAccess=%v", peerHasAccess)

	return peerHasAccess, nil
}

// pubSubMessageHandler handles incoming PushLog messages from the pubsub network.
func (p *P2P) pubSubMessageHandler(from string, topic string, msg []byte) ([]byte, error) {
	req := &protocol.PushLogRequest{}
	if err := cbor.Unmarshal(msg, req); err != nil {
		return nil, err
	}
	req.SenderID = from

	if err := p.processPushlogRequest(p.ctx, req, false); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			log.Info("Context done during pushlog request processing", corelog.Any("Error", err))
			return nil, nil
		}
		return nil, errors.Wrap(fmt.Sprintf("Failed to process pushlog request %s", topic), err)
	}

	return nil, nil
}

func (p *P2P) peerEventHandler(peerID string, topic string, eventType string) {
	p.db.Events().Publish(event.NewMessage(event.TopicPeerEventName, event.TopicPeerEvent{
		PeerID:    peerID,
		Topic:     topic,
		EventType: eventType,
	}))
}

// processPushlogRequest processes a push log request
func (p *P2P) processPushlogRequest(
	ctx context.Context,
	req *protocol.PushLogRequest,
	isReplicator bool,
) error {
	defer p.trc.Enter("processPushlogRequest (receiver)")()
	p.trc.Println("docID=%s collectionID=%s sender=%s isReplicator=%v",
		req.DocID, req.CollectionID, req.SenderID, isReplicator)
	debug.DefaultTimeline.Log(p.actor, "[recv] pushlog arrived (isReplicator=%v)", isReplicator)
	if ctx.Err() != nil {
		p.trc.Println("ctx already errored: %v", ctx.Err())
		return ctx.Err()
	}
	block, err := coreblock.GetFromBytes(req.Block)
	if err != nil {
		p.trc.Println("decode block ERROR: %v", err)
		return err
	}

	headCID, err := cid.Cast(req.CID)
	if err != nil {
		p.trc.Println("cast CID ERROR: %v", err)
		return err
	}
	p.trc.Println("headCID=%s", headCID.String())

	// Calls to syncDAG should not overlap for a given CID. If they do, they will use the same
	// underlying pubsub topic and this brings along potential pitfalls. One of them being that
	// if this initial sync call had a negative response for a given link, the subsequent calls will
	// assume a negative response for that same link without retrying.
	p.processQueue.add(headCID)
	done := p.processQueue.doneOnce(headCID)
	defer done()

	// Check if we've already merged this block. If so, skip the sink process.
	isMerged, err := p.db.Multistore().Blockstore().IsMerged(ctx, headCID)
	if err != nil {
		p.trc.Println("IsMerged ERROR: %v", err)
		return err
	}
	if isMerged {
		p.trc.Println("block already merged -> skip")
		return nil
	}

	// No need to check access if the message is for replication as the node sending
	// will have done so deliberately.
	if !isReplicator {
		mightHaveAccess, err := p.trySelfHasAccess(ctx, block, req.CollectionID)
		if err != nil {
			p.trc.Println("trySelfHasAccess ERROR: %v", err)
			return err
		}
		if !mightHaveAccess {
			p.trc.Println("trySelfHasAccess said NO -> drop (silently)")
			debug.DefaultTimeline.Log(p.actor, "[recv] gate=NO  (silent drop, no log, no retry)")
			// If we know we don't have access, we can skip the rest of the processing.
			return nil
		}
		p.trc.Println("trySelfHasAccess said YES -> proceed to syncDAG")
		debug.DefaultTimeline.Log(p.actor, "[recv] gate=YES -> syncDAG")
	} else {
		p.trc.Println("isReplicator=true -> skip trySelfHasAccess")
	}

	p.trc.Println("entering syncDAG")
	err = p.syncDAG(ctx, block)
	if err != nil {
		p.trc.Println("syncDAG ERROR: %v", err)
		return err
	}
	p.trc.Println("syncDAG OK -> entering Merge")

	mergeEvt := event.Merge{
		DocID:        req.DocID,
		ByPeer:       req.SenderID,
		FromPeer:     req.Creator,
		Cid:          headCID,
		CollectionID: req.CollectionID,
	}
	err = p.db.Merge(ctx, mergeEvt)
	if err != nil {
		return err
	}

	// Notify bus subscribers and the network of peers that we have a new document available.
	updateEvt := event.Update{
		DocID:        req.DocID,
		Cid:          headCID,
		CollectionID: req.CollectionID,
		Block:        req.Block,
		IsRelay:      true,
	}
	p.db.Events().Publish(event.NewMessage(event.UpdateName, updateEvt))
	if err := p.SendUpdate(updateEvt); err != nil {
		// We don't need to return the error for this side-effect-function call.
		// It's a bonus action that shouldn't affect the caller of `processPuslogRequest`.
		log.ErrorE("Failed to send update after sync", err, slog.Any("PeerID", p.host.ID()))
	}

	return nil
}

func (p *P2P) SendUpdate(evt event.Update) error {
	// push to each peer (replicator)
	p.pushLogToReplicators(evt)

	// Retries are for replicators only and should not pollute the pubsub network.
	if !evt.IsRetry {
		req := &protocol.PushLogRequest{
			DocID:        evt.DocID,
			CID:          evt.Cid.Bytes(),
			CollectionID: evt.CollectionID,
			Creator:      p.host.ID(),
			Block:        evt.Block,
		}

		b, err := cbor.Marshal(req)
		if err != nil {
			return err
		}

		if evt.DocID != "" {
			if err := p.host.PublishToTopicAsync(p.ctx, evt.DocID, b); err != nil {
				return NewErrPublishingToDocIDTopic(err, evt.Cid.String(), evt.DocID)
			}
		}

		if err := p.host.PublishToTopicAsync(p.ctx, evt.CollectionID, b); err != nil {
			return NewErrPublishingToCollectionTopic(err, evt.Cid.String(), evt.CollectionID)
		}
	}

	return nil
}

// processQueue is synchronization source to ensure that concurrent
// document merges do not cause transaction conflicts.
type processQueue struct {
	cids  map[cid.Cid]chan struct{}
	mutex sync.Mutex
}

func newProcessQueue() *processQueue {
	return &processQueue{
		cids: make(map[cid.Cid]chan struct{}),
	}
}

// add adds a cid to the queue. If the cid is already in the queue, it will
// wait for the cid to be removed from the queue. For every add call, done must
// be called to remove the cid from the queue. Otherwise, subsequent add calls will
// block forever.
func (m *processQueue) add(cid cid.Cid) {
	for {
		m.mutex.Lock()
		done, ok := m.cids[cid]
		if !ok {
			m.cids[cid] = make(chan struct{})
			m.mutex.Unlock()
			return
		}
		m.mutex.Unlock()
		<-done
	}
}

func (m *processQueue) done(cid cid.Cid) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	done, ok := m.cids[cid]
	if ok {
		delete(m.cids, cid)
		close(done)
	}
}

// doneOnce returns a function that invokes done only once.
func (m *processQueue) doneOnce(cid cid.Cid) func() {
	return sync.OnceFunc(func() {
		m.done(cid)
	})
}

// QueryDocIDsWithSETags queries SE artifacts from replicators based on field values.
func (p *P2P) QueryDocIDsWithSETags(
	ctx context.Context,
	collectionID string,
	fieldValues []se.FieldValueQuery,
) ([]string, error) {
	if p.seCoordinator == nil {
		return []string{}, nil
	}

	return p.seCoordinator.QueryDocIDsByValues(ctx, collectionID, fieldValues)
}

// shortCID returns a 6-char suffix of a CID string for readable timeline output.
func shortCID(c string) string {
	if len(c) <= 6 {
		return c
	}
	return "…" + c[len(c)-6:]
}
