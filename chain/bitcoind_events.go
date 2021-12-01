package chain

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/gozmq"
)

const (
	// defaultBlockPollInterval is the default interval used for querying
	// for new blocks.
	defaultBlockPollInterval = time.Second * 10

	// defaultTxPollInterval is the default interval used for querying
	// for new mempool transactions.
	defaultTxPollInterval = time.Second * 10

	// defaultMempoolEvictionInterval is the default interval after which we
	// will evict a transaction from our local mempool.
	defaultMempoolEvictionInterval = time.Hour * 24
)

// BitcoindEvents is the interface that must be satisfied by any type that
// serves bitcoind block and transactions events.
type BitcoindEvents interface {
	Start() error
	Stop() error
}

// NewBitcoindEventSubscriber initialises a new BitcoinEvents object impl
// depending on the config passed.
func NewBitcoindEventSubscriber(cfg *BitcoindConfig, client *rpcclient.Client,
	notifyBlock func(block *wire.MsgBlock),
	notifyTx func(tx *wire.MsgTx)) (BitcoindEvents, error) {

	if cfg.ZMQConfig != nil && cfg.PollingConfig != nil {
		return nil, fmt.Errorf("cannot specify both zmq config and " +
			"rpc polling config")
	}

	if cfg.ZMQConfig != nil {
		return newBitcoindZMQEvents(
			cfg.ZMQConfig, notifyBlock, notifyTx,
		)
	}

	if client == nil {
		return nil, fmt.Errorf("rpc client must be given if rpc " +
			"polling is to be used for event subscriptions")
	}

	if cfg.PollingConfig == nil {
		cfg.PollingConfig = &PollingConfig{Enable: true}
	}

	pollingEvents := newBitcoindRPCPollingEvents(
		cfg.PollingConfig, client, notifyBlock, notifyTx,
	)

	return pollingEvents, nil
}

// ZMQConfig holds all the config values needed to set up a ZMQ connection to
// bitcoind.
type ZMQConfig struct {
	// ZMQBlockHost is the IP address and port of the bitcoind's rawblock
	// listener.
	ZMQBlockHost string

	// ZMQTxHost is the IP address and port of the bitcoind's rawtx
	// listener.
	ZMQTxHost string

	// ZMQReadDeadline represents the read deadline we'll apply when reading
	// ZMQ messages from either subscription.
	ZMQReadDeadline time.Duration
}

// bitcoindZMQEvents delivers block and transaction notifications that it gets
// from ZMQ connections to bitcoind.
type bitcoindZMQEvents struct {
	cfg *ZMQConfig

	// blockConn is the ZMQ connection we'll use to read raw block events.
	blockConn *gozmq.Conn

	// txConn is the ZMQ connection we'll use to read raw transaction
	// events.
	txConn *gozmq.Conn

	// notifyBlock will be called each time a new block is received on the
	// block ZMQ connection.
	notifyBlock func(block *wire.MsgBlock)

	// notifyTx will be called each time a new transaction is received on
	// the tx ZMQ connection.
	notifyTx func(tx *wire.MsgTx)

	wg   sync.WaitGroup
	quit chan struct{}
}

// newBitcoindZMQEvents initialises the necessary zmq connections to bitcoind.
func newBitcoindZMQEvents(cfg *ZMQConfig, notifyBlock func(*wire.MsgBlock),
	notifyTx func(tx *wire.MsgTx)) (*bitcoindZMQEvents, error) {

	// Establish two different ZMQ connections to bitcoind to retrieve block
	// and transaction event notifications. We'll use two as a separation of
	// concern to ensure one type of event isn't dropped from the connection
	// queue due to another type of event filling it up.
	zmqBlockConn, err := gozmq.Subscribe(
		cfg.ZMQBlockHost, []string{rawBlockZMQCommand},
		cfg.ZMQReadDeadline,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to subscribe for zmq block "+
			"events: %v", err)
	}

	zmqTxConn, err := gozmq.Subscribe(
		cfg.ZMQTxHost, []string{rawTxZMQCommand}, cfg.ZMQReadDeadline,
	)
	if err != nil {
		// Ensure that the block zmq connection is closed in the case
		// that it succeeded but the tx zmq connection failed.
		if err := zmqBlockConn.Close(); err != nil {
			log.Errorf("could not close zmq block conn: %v", err)
		}

		return nil, fmt.Errorf("unable to subscribe for zmq tx "+
			"events: %v", err)
	}

	return &bitcoindZMQEvents{
		cfg:         cfg,
		blockConn:   zmqBlockConn,
		txConn:      zmqTxConn,
		notifyBlock: notifyBlock,
		notifyTx:    notifyTx,
		quit:        make(chan struct{}),
	}, nil
}

// Start spins off the bitcoindZMQEvent goroutines.
func (b *bitcoindZMQEvents) Start() error {
	b.wg.Add(2)
	go b.blockEventHandler()
	go b.txEventHandler()
	return nil
}

// blockEventHandler reads raw blocks events from the ZMQ block socket and
// forwards them along to the current rescan clients.
//
// NOTE: This must be run as a goroutine.
func (b *bitcoindZMQEvents) blockEventHandler() {
	defer b.wg.Done()

	log.Info("Started listening for bitcoind block notifications via ZMQ "+
		"on", b.blockConn.RemoteAddr())

	// Set up the buffers we expect our messages to consume. ZMQ
	// messages from bitcoind include three parts: the command, the
	// data, and the sequence number.
	//
	// We'll allocate a fixed data slice that we'll reuse when reading
	// blocks from bitcoind through ZMQ. There's no need to recycle this
	// slice (zero out) after using it, as further reads will overwrite the
	// slice and we'll only be deserializing the bytes needed.
	var (
		command [len(rawBlockZMQCommand)]byte
		seqNum  [seqNumLen]byte
		data    = make([]byte, maxRawBlockSize)
	)

	for {
		// Before attempting to read from the ZMQ socket, we'll make
		// sure to check if we've been requested to shut down.
		select {
		case <-b.quit:
			return
		default:
		}

		// Poll an event from the ZMQ socket.
		var (
			bufs = [][]byte{command[:], data, seqNum[:]}
			err  error
		)
		bufs, err = b.blockConn.Receive(bufs)
		if err != nil {
			// EOF should only be returned if the connection was
			// explicitly closed, so we can exit at this point.
			if err == io.EOF {
				return
			}

			// It's possible that the connection to the socket
			// continuously times out, so we'll prevent logging this
			// error to prevent spamming the logs.
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				log.Trace("Re-establishing timed out ZMQ " +
					"block connection")
				continue
			}

			log.Errorf("Unable to receive ZMQ %v message: %v",
				rawBlockZMQCommand, err)
			continue
		}

		// We have an event! We'll now ensure it is a block event,
		// deserialize it, and report it to the different rescan
		// clients.
		eventType := string(bufs[0])
		switch eventType {
		case rawBlockZMQCommand:
			block := &wire.MsgBlock{}
			r := bytes.NewReader(bufs[1])
			if err := block.Deserialize(r); err != nil {
				log.Errorf("Unable to deserialize block: %v",
					err)
				continue
			}

			b.notifyBlock(block)

		default:
			// It's possible that the message wasn't fully read if
			// bitcoind shuts down, which will produce an unreadable
			// event type. To prevent from logging it, we'll make
			// sure it conforms to the ASCII standard.
			if eventType == "" || !isASCII(eventType) {
				continue
			}

			log.Warnf("Received unexpected event type from %v "+
				"subscription: %v", rawBlockZMQCommand,
				eventType)
		}
	}
}

// txEventHandler reads raw blocks events from the ZMQ block socket and forwards
// them along to the current rescan clients.
//
// NOTE: This must be run as a goroutine.
func (b *bitcoindZMQEvents) txEventHandler() {
	defer b.wg.Done()

	log.Info("Started listening for bitcoind transaction notifications "+
		"via ZMQ on", b.txConn.RemoteAddr())

	// Set up the buffers we expect our messages to consume. ZMQ
	// messages from bitcoind include three parts: the command, the
	// data, and the sequence number.
	//
	// We'll allocate a fixed data slice that we'll reuse when reading
	// transactions from bitcoind through ZMQ. There's no need to recycle
	// this slice (zero out) after using it, as further reads will overwrite
	// the slice and we'll only be deserializing the bytes needed.
	var (
		command [len(rawTxZMQCommand)]byte
		seqNum  [seqNumLen]byte
		data    = make([]byte, maxRawTxSize)
	)

	for {
		// Before attempting to read from the ZMQ socket, we'll make
		// sure to check if we've been requested to shut down.
		select {
		case <-b.quit:
			return
		default:
		}

		// Poll an event from the ZMQ socket.
		var (
			bufs = [][]byte{command[:], data, seqNum[:]}
			err  error
		)
		bufs, err = b.txConn.Receive(bufs)
		if err != nil {
			// EOF should only be returned if the connection was
			// explicitly closed, so we can exit at this point.
			if err == io.EOF {
				return
			}

			// It's possible that the connection to the socket
			// continuously times out, so we'll prevent logging this
			// error to prevent spamming the logs.
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				log.Trace("Re-establishing timed out ZMQ " +
					"transaction connection")
				continue
			}

			log.Errorf("Unable to receive ZMQ %v message: %v",
				rawTxZMQCommand, err)
			continue
		}

		// We have an event! We'll now ensure it is a transaction event,
		// deserialize it, and report it to the different rescan
		// clients.
		eventType := string(bufs[0])
		switch eventType {
		case rawTxZMQCommand:
			tx := &wire.MsgTx{}
			r := bytes.NewReader(bufs[1])
			if err := tx.Deserialize(r); err != nil {
				log.Errorf("Unable to deserialize "+
					"transaction: %v", err)
				continue
			}

			b.notifyTx(tx)

		default:
			// It's possible that the message wasn't fully read if
			// bitcoind shuts down, which will produce an unreadable
			// event type. To prevent from logging it, we'll make
			// sure it conforms to the ASCII standard.
			if eventType == "" || !isASCII(eventType) {
				continue
			}

			log.Warnf("Received unexpected event type from %v "+
				"subscription: %v", rawTxZMQCommand, eventType)
		}
	}
}

// Stop cleans up any of the resources and goroutines held by bitcoindZMQEvents.
func (b *bitcoindZMQEvents) Stop() error {
	var returnErr error
	if err := b.txConn.Close(); err != nil {
		returnErr = err
	}

	if err := b.blockConn.Close(); err != nil {
		returnErr = err
	}

	close(b.quit)
	b.wg.Wait()
	return returnErr
}

// PollingConfig holds all the config options used for setting up
// bitcoindRPCPollingEvents.
type PollingConfig struct {
	// Enable is true if we should use RPC polling for block and tx
	// notifications instead of ZMQ.
	Enable bool

	// BlockPollingInterval is the interval that will be used to poll
	// bitcoind for new blocks.
	BlockPollingInterval time.Duration

	// TxPollingInterval is the interval that will be used to poll bitcoind
	// for new transactions.
	TxPollingInterval time.Duration

	// MempoolEvictionAge is the amount of time after which we will manually
	// remove a transaction from our in-memory mempool if it has still not
	// been confirmed.
	MempoolEvictionAge time.Duration
}

// bitcoindRPCPollingEvents delivers block and transaction notifications that
// it gets by polling bitcoind's rpc interface at regular intervals.
type bitcoindRPCPollingEvents struct {
	cfg *PollingConfig

	client *rpcclient.Client

	// mempool holds all the transactions that we currently see as being in
	// the mempool. This is used so that we know which transactions we have
	// already sent notifications for.
	mempool *mempool

	// notifyBlock will be called each time a new block is received on the
	// block ZMQ connection.
	notifyBlock func(block *wire.MsgBlock)

	// notifyTx will be called each time a new transaction is received on
	// the tx ZMQ connection.
	notifyTx func(tx *wire.MsgTx)

	wg   sync.WaitGroup
	quit chan struct{}
}

// newBitcoindRPCPollingEvents instantiates a new bitcoindRPCPollingEvents
// object.
func newBitcoindRPCPollingEvents(cfg *PollingConfig, client *rpcclient.Client,
	notifyBlock func(block *wire.MsgBlock),
	notifyTx func(tx *wire.MsgTx)) *bitcoindRPCPollingEvents {

	if cfg.BlockPollingInterval == 0 {
		cfg.BlockPollingInterval = defaultBlockPollInterval
	}

	if cfg.TxPollingInterval == 0 {
		cfg.TxPollingInterval = defaultTxPollInterval
	}

	if cfg.MempoolEvictionAge == 0 {
		cfg.MempoolEvictionAge = defaultMempoolEvictionInterval
	}

	return &bitcoindRPCPollingEvents{
		cfg:         cfg,
		client:      client,
		notifyTx:    notifyTx,
		notifyBlock: notifyBlock,
		mempool:     newMempool(),
		quit:        make(chan struct{}),
	}
}

// Start kicks off all the bitcoindRPCPollingEvents goroutines.
func (b *bitcoindRPCPollingEvents) Start() error {
	info, err := b.client.GetBlockChainInfo()
	if err != nil {
		return err
	}

	b.wg.Add(3)
	go b.blockEventHandlerRPC(info.Blocks)
	go b.txEventHandlerRPC()
	go b.evictOldTransactions()
	return nil
}

// Stop cleans up all the bitcoindRPCPollingEvents resources and goroutines.
func (b *bitcoindRPCPollingEvents) Stop() error {
	close(b.quit)
	b.wg.Wait()
	return nil
}

// blockEventHandlerRPC is a goroutine that uses the rpc client to check if we
// have a new block every so often.
func (b *bitcoindRPCPollingEvents) blockEventHandlerRPC(startHeight int32) {
	defer b.wg.Done()

	ticker := time.NewTicker(b.cfg.BlockPollingInterval)
	defer ticker.Stop()

	height := startHeight
	log.Infof("Started polling for new bitcoind blocks via RPC at "+
		"height %d", height)

	for {
		select {
		case <-ticker.C:
			// At every interval, we poll to see if there's a block
			// with a height that exceeds the height that we
			// previously recorded.
			info, err := b.client.GetBlockChainInfo()
			if err != nil {
				log.Errorf("Unable to retrieve best block: "+
					"%v", err)
				continue
			}

			// If the block isn't new, we continue and wait for the
			// next interval tick. In order to replicate the
			// behaviour of the zmq block subscription, we only do
			// a height based check here. We only deliver
			// notifications if the new block has a height above the
			// one we previously saw. The caller is left to
			// determine if there has been a reorg.
			if info.Blocks <= height {
				continue
			}

			// Since we do a height based check, we send
			// notifications for each block with a height between
			// the last height we recorded and the new height.
			for i := height + 1; i <= info.Blocks; i++ {
				newHash, err := b.client.GetBlockHash(int64(i))
				if err != nil {
					log.Errorf("Unable to retrieve "+
						"block hash: %v", err)
					continue
				}

				newBlock, err := b.client.GetBlock(newHash)
				if err != nil {
					log.Errorf("Unable to retrieve "+
						"block: %v", err)
					continue
				}

				// notify the client of the new block.
				b.notifyBlock(newBlock)

				// From our local mempool map, let's remove each
				// of the transactions that are confirmed in
				// this new block, since they are no longer in
				// the mempool.
				b.mempool.clean(newBlock.Transactions)

				height++
			}

		case <-b.quit:
			return
		}
	}
}

// txEventHandlerRPC is a goroutine that uses the RPC client to check the
// mempool for new transactions.
func (b *bitcoindRPCPollingEvents) txEventHandlerRPC() {
	defer b.wg.Done()

	log.Info("Started polling for new bitcoind transactions via RPC.")
	ticker := time.NewTicker(b.cfg.TxPollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// After each ticker interval, we poll the mempool to
			// check for transactions we haven't seen yet.
			txs, err := b.client.GetRawMempool()
			if err != nil {
				log.Errorf("Unable to retrieve mempool txs: "+
					"%v", err)
				continue
			}

			// We'll scan through the most recent txs in the
			// mempool to see whether there are new txs that we
			// need to send to the client.
			for _, txHash := range txs {
				// If the transaction isn't in the local
				// mempool, we'll send it to the client.
				if b.mempool.contains(txHash) {
					continue
				}

				// Grab full mempool transaction from hash.
				tx, err := b.client.GetRawTransaction(txHash)
				if err != nil {
					log.Errorf("unable to fetch "+
						"transaction %s from "+
						"mempool: %v", txHash, err)
					continue
				}

				// Add the transaction to our local mempool.
				// Note that we only do this after fetching
				// the full raw transaction from bitcoind.
				// We do this so that if that call happens to
				// initially fail, then we will retry it on the
				// next interval since it is still not in our
				// local mempool.
				b.mempool.add(txHash)

				b.notifyTx(tx.MsgTx())
			}

		case <-b.quit:
			return
		}
	}
}

// evictOldTransactions periodically removes any old transactions from our local
// mempool.
func (b *bitcoindRPCPollingEvents) evictOldTransactions() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.cfg.MempoolEvictionAge)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.mempool.removeOldTxs(b.cfg.MempoolEvictionAge)

		case <-b.quit:
			return
		}
	}
}

// mempool represents our view of the mempool and helps to keep track of which
// mempool transactions we already know about and also what time we saw them.
type mempool struct {
	sync.RWMutex
	txs map[chainhash.Hash]time.Time
}

// newMempool creates a new mempool object.
func newMempool() *mempool {
	return &mempool{
		txs: make(map[chainhash.Hash]time.Time),
	}
}

// clean removes any of the given transactions from the mempool if they are
// found there.
func (m *mempool) clean(txs []*wire.MsgTx) {
	m.Lock()
	defer m.Unlock()

	for _, tx := range txs {
		// If the transaction is in our mempool
		// map, we need to delete it.
		if _, ok := m.txs[tx.TxHash()]; ok {
			delete(m.txs, tx.TxHash())
		}
	}
}

// contains returns true if the given transaction hash is already in our
// mempool.
func (m *mempool) contains(hash *chainhash.Hash) bool {
	m.RLock()
	defer m.RUnlock()

	_, ok := m.txs[*hash]
	return ok
}

// add inserts the given hash into our mempool.
func (m *mempool) add(hash *chainhash.Hash) {
	m.Lock()
	defer m.Unlock()

	m.txs[*hash] = time.Now()
}

// removeOldTxs removes all transactions older than the given age from our
// mempool.
func (m *mempool) removeOldTxs(age time.Duration) {
	m.Lock()
	defer m.Unlock()

	for txHash, timeAdded := range m.txs {
		if time.Since(timeAdded) > age {
			delete(m.txs, txHash)
		}
	}
}
