package chain

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/require"
)

// TestBitcoindEvents ensures that the BitcoindClient correctly delivers tx and
// block notifications for both the case where a ZMQ subscription is used and
// for the case where RPC polling is used.
func TestBitcoindEvents(t *testing.T) {
	tests := []struct {
		name       string
		rpcPolling bool
	}{
		{
			name:       "Events via ZMQ subscriptions",
			rpcPolling: true,
		},
		{
			name:       "Events via RPC Polling",
			rpcPolling: true,
		},
	}

	// Set up 2 btcd miners.
	miner1, miner2 := setupMiners(t)

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			// Set up a bitcoind node and connect it to miner 1.
			btcClient := setupBitcoind(
				t, miner1.P2PAddress(), test.rpcPolling,
			)

			// Test that the correct block connect and disconnect
			// notifications are received during a re-org.
			testReorg(t, miner1, miner2, btcClient)

			// Test that the expected block and transaction
			// notifications are received.
			testNotifications(t, miner1, btcClient)
		})
	}
}

// TestMempool tests that each method of the mempool struct works as expected.
func TestMempool(t *testing.T) {
	m := newMempool()

	// Create a transaction.
	tx1 := &wire.MsgTx{LockTime: 1}
	tx1Hash := tx1.TxHash()

	// Check that mempool doesn't have the tx yet.
	require.False(t, m.contains(&tx1Hash))

	// Now add the tx.
	m.add(&tx1Hash)

	// Mempool should now contain the tx.
	require.True(t, m.contains(&tx1Hash))

	// Add another tx to the mempool.
	tx2 := &wire.MsgTx{LockTime: 2}
	tx2Hash := tx2.TxHash()
	m.add(&tx2Hash)
	require.True(t, m.contains(&tx2Hash))

	// Clean the mempool of tx1 (this simulates a block being confirmed
	// with tx1 in the block).
	m.clean([]*wire.MsgTx{tx1})

	// Ensure that tx1 is no longer in the mempool but that tx2 still is.
	require.False(t, m.contains(&tx1Hash))
	require.True(t, m.contains(&tx2Hash))

	// Lastly, we test that the removeOldTxs function does in fact only
	// remove transactions older than a certain age. We do this by first
	// aging tx2 a bit and then by adding a brand new tx3 and then we test
	// that if we call removeOldTxs, only tx3 will be left in the mempool.
	time.Sleep(time.Millisecond * 20)

	// Add a brand new tx to the mempool.
	tx3 := &wire.MsgTx{LockTime: 3}
	tx3Hash := tx3.TxHash()
	m.add(&tx3Hash)
	require.True(t, m.contains(&tx3Hash))

	m.removeOldTxs(time.Millisecond * 10)

	require.False(t, m.contains(&tx2Hash))
	require.True(t, m.contains(&tx3Hash))
}

// testNotifications tests that the correct notifications are received for
// blocks and transactions in the simple non-reorg case.
func testNotifications(t *testing.T, miner *rpctest.Harness,
	client *BitcoindClient) {

	script, _, err := randPubKeyHashScript()
	require.NoError(t, err)

	tx, err := miner.CreateTransaction(
		[]*wire.TxOut{{Value: 1000, PkScript: script}}, 5, false,
	)
	if err != nil {
		t.Fatalf("could not create tx: %v", err)
	}
	hash := tx.TxHash()

	err = client.NotifyTx([]chainhash.Hash{hash})
	if err != nil {
		t.Fatalf("could not subscribe to tx notifications: %v", err)
	}

	_, err = client.SendRawTransaction(tx, true)
	if err != nil {
		t.Fatalf("could not broadcast transaction: %v", err)
	}

	ntfns := client.Notifications()

	miner.Client.Generate(1)

	// We cant be sure what order the notifications will arrive in, but
	// we know that we expect three of them. We expect one of each of the
	// following.
	var (
		relevantTx             int
		filteredBlockConnected int
		blockConnected         int
	)
	for i := 0; i < 3; i++ {
		select {
		case ntfn := <-ntfns:
			switch ntfnType := ntfn.(type) {
			case RelevantTx:
				require.True(
					t, ntfnType.TxRecord.Hash.IsEqual(
						&hash,
					),
				)
				relevantTx++
				if relevantTx != 1 {
					t.Fatalf("received more RelevantTx " +
						"notifications than expected")
				}

			case FilteredBlockConnected:
				filteredBlockConnected++
				if filteredBlockConnected != 1 {
					t.Fatalf("received more " +
						"FilterBlockRequest " +
						"notifications than expected")
				}

			case BlockConnected:
				blockConnected++
				if blockConnected != 1 {
					t.Fatalf("received more " +
						"BlockConnected " +
						"notifications than expected")
				}

			default:
				t.Fatalf("reveived unexpected %T "+
					"notification", ntfn)
			}

		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for block notification")
		}
	}
}

// testReorg tests that the given BitcoindClient correctly responds to a chain
// re-org.
func testReorg(t *testing.T, miner1, miner2 *rpctest.Harness,
	client *BitcoindClient) {

	ntfns := client.Notifications()
	miner1Hash, commonHeight, err := miner1.Client.GetBestBlock()
	if err != nil {
		t.Fatalf("could not fetch best block from miner1")
	}

	miner2Hash, miner2Height, err := miner2.Client.GetBestBlock()
	if err != nil {
		t.Fatalf("could not fetch best block from miner2")
	}

	require.Equal(t, commonHeight, miner2Height)
	require.Equal(t, miner1Hash, miner2Hash)

	// Let miner2 generate a few blocks and ensure that our bitcoind client
	// is notified of this block.
	hashes, err := miner2.Client.Generate(5)
	if err != nil {
		t.Fatalf("could not generate block with miner2: %v", err)
	}
	require.Len(t, hashes, 5)

	for i := 0; i < 5; i++ {
		commonHeight++
		ntfnHeight, ntfnHash := waitForBlockNtfn(t, ntfns)
		require.Equal(t, commonHeight, ntfnHeight)
		require.True(t, ntfnHash.IsEqual(hashes[i]))
	}

	// Now disconnect the two miners.
	err = miner1.Client.AddNode(miner2.P2PAddress(), rpcclient.ANRemove)
	if err != nil {
		t.Fatalf("unable to remove node: %v", err)
	}

	// Generate 5 blocks on miner2.
	_, err = miner2.Client.Generate(5)
	if err != nil {
		t.Fatalf("could not generate block with miner2: %v", err)
	}

	// Since the miners have been disconnected, we expect not to get any
	// notifications from our client since our client is connected to
	// miner1.
	select {
	case ntfn := <-ntfns:
		t.Fatalf("received a notification of type %T but expected,"+
			" none", ntfn)
	case <-time.After(time.Millisecond * 10):
	}

	// Now generate 3 blocks on miner1. Note that to force our client to
	// experience a re-org, miner1 must generate fewer blocks here than
	// miner2 so that when they reconnect, miner1 does a re-org to switch
	// to the longer chain.
	_, err = miner1.Client.Generate(3)
	if err != nil {
		t.Fatalf("could not generate block with miner2: %v", err)
	}

	// Read the notifications for the new blocks
	for i := 0; i < 3; i++ {
		_, _ = waitForBlockNtfn(t, ntfns)
	}

	// Ensure that the two miners have different ideas of what the best
	// block is.
	hash1, height1, err := miner1.Client.GetBestBlock()
	if err != nil {
		t.Fatalf("could not get best block from miner1")
	}
	require.Equal(t, commonHeight+3, height1)

	hash2, height2, err := miner2.Client.GetBestBlock()
	if err != nil {
		t.Fatalf("could not get best block from miner2")
	}
	require.Equal(t, commonHeight+5, height2)

	require.False(t, hash1.IsEqual(hash2))

	// Reconnect the miners. This should result in miner1 reorging to match
	// miner2. Since our client is connected to a node connected to miner1,
	// we should get the expected disconnected and connected notifications.
	if err := rpctest.ConnectNode(miner1, miner2); err != nil {
		t.Fatalf("could not connect miner1 and miner2: %v", err)
	}
	if err := rpctest.JoinNodes(
		[]*rpctest.Harness{miner1, miner2}, rpctest.Blocks,
	); err != nil {
		t.Fatalf("unable to join node on blocks: %v", err)
	}

	// Check that the miners are now on the same page.
	hash1, height1, err = miner1.Client.GetBestBlock()
	if err != nil {
		t.Fatalf("could not get best block from miner1")
	}
	hash2, height2, err = miner2.Client.GetBestBlock()
	if err != nil {
		t.Fatalf("could not get best block from miner2")
	}

	require.Equal(t, commonHeight+5, height2)
	require.Equal(t, commonHeight+5, height1)
	require.True(t, hash1.IsEqual(hash2))

	// We expect our client to get 3 BlockDisconnected notifications first
	// signaling the unwinding of its top 3 blocks.
	for i := 0; i < 3; i++ {
		ntfnHeight, _ := waitForBlockNtfn(t, ntfns)
		require.Equal(t, ntfnHeight, commonHeight+int32(3-i))
	}

	// Now we expect 5 BlockConnected notifications.
	for i := 0; i < 5; i++ {
		ntfnHeight, _ := waitForBlockNtfn(t, ntfns)
		require.Equal(t, ntfnHeight, commonHeight+int32(i+1))
	}
}

// waitForBlockNtfn waits on the passed channel for a BlockConnected
// or BlockDisconnected notification and returns the height and hash of the
// notification if received. If nothing is received on the channel after a
// second, the test is failed.
func waitForBlockNtfn(t *testing.T, ntfns <-chan interface{}) (int32,
	chainhash.Hash) {

	timer := time.NewTimer(time.Second)
	for {

		select {
		case nftn := <-ntfns:
			switch ntfnType := nftn.(type) {
			case BlockConnected:
				return ntfnType.Height, ntfnType.Hash

			case BlockDisconnected:
				return ntfnType.Height, ntfnType.Hash

			default:
			}

		case <-timer.C:
			t.Fatalf("timed out waiting for block notification")
		}
	}
}

// setUpMiners sets up two miners that can be used for a re-org test.
func setupMiners(t *testing.T) (*rpctest.Harness, *rpctest.Harness) {
	trickle := fmt.Sprintf("--trickleinterval=%v", 10*time.Millisecond)
	args := []string{trickle}

	miner1, err := rpctest.New(
		&chaincfg.RegressionNetParams, nil, args, "",
	)
	if err != nil {
		t.Fatalf("could not set up miner: %v", err)
	}

	t.Cleanup(func() {
		miner1.TearDown()
	})

	if err := miner1.SetUp(true, 1); err != nil {
		t.Fatalf("could not set up miner: %v", err)
	}

	miner2, err := rpctest.New(
		&chaincfg.RegressionNetParams, nil, args, "",
	)
	if err != nil {
		t.Fatalf("could not set up miner: %v", err)
	}

	t.Cleanup(func() {
		miner2.TearDown()
	})

	if err := miner2.SetUp(false, 0); err != nil {
		t.Fatalf("could not set up miner: %v", err)
	}

	// Connect the miners.
	if err := rpctest.ConnectNode(miner1, miner2); err != nil {
		t.Fatalf("could not connect miner1 and miner2: %v", err)
	}

	if err := rpctest.JoinNodes(
		[]*rpctest.Harness{miner1, miner2}, rpctest.Blocks,
	); err != nil {
		t.Fatalf("unable to join node on blocks: %v", err)
	}

	return miner1, miner2
}

// setupBitcoind starts up a bitcoind node with either a zmq connection or
// rpc polling connection and returns a client wrapper of this connection.
func setupBitcoind(t *testing.T, minerAddr string,
	rpcPolling bool) *BitcoindClient {

	// Start a bitcoind instance and connect it to miner1.
	tempBitcoindDir, err := ioutil.TempDir("", "bitcoind")
	if err != nil {
		t.Fatalf("could not create directory: %v", err)
	}

	zmqBlockHost := "ipc:///" + tempBitcoindDir + "/blocks.socket"
	zmqTxHost := "ipc:///" + tempBitcoindDir + "/tx.socket"
	t.Cleanup(func() {
		os.RemoveAll(tempBitcoindDir)
	})

	rpcPort := rand.Int()%(65536-1024) + 1024
	bitcoind := exec.Command(
		"bitcoind",
		"-datadir="+tempBitcoindDir,
		"-regtest",
		"-connect="+minerAddr,
		"-txindex",
		"-rpcauth=weks:469e9bb14ab2360f8e226efed5ca6f"+
			"d$507c670e800a95284294edb5773b05544b"+
			"220110063096c221be9933c82d38e1",
		fmt.Sprintf("-rpcport=%d", rpcPort),
		"-disablewallet",
		"-zmqpubrawblock="+zmqBlockHost,
		"-zmqpubrawtx="+zmqTxHost,
	)

	if err = bitcoind.Start(); err != nil {
		t.Fatalf("could not start bitcoind: %v", err)
	}

	t.Cleanup(func() {
		bitcoind.Process.Kill()
		bitcoind.Wait()
	})

	// Wait for the bitcoind instance to start up.
	time.Sleep(time.Second)

	host := fmt.Sprintf("127.0.0.1:%d", rpcPort)
	cfg := &BitcoindConfig{
		ChainParams: &chaincfg.RegressionNetParams,
		Host:        host,
		User:        "weks",
		Pass:        "weks",
		// Fields only required for pruned nodes, not
		// needed for these tests.
		Dialer:             nil,
		PrunedModeMaxPeers: 0,
	}

	if rpcPolling {
		cfg.PollingConfig = &PollingConfig{
			Enable:               true,
			BlockPollingInterval: time.Millisecond * 100,
			TxPollingInterval:    time.Millisecond * 100,
			MempoolEvictionAge:   time.Minute,
		}
	} else {
		cfg.ZMQConfig = &ZMQConfig{
			ZMQBlockHost:    zmqBlockHost,
			ZMQTxHost:       zmqTxHost,
			ZMQReadDeadline: 5 * time.Second,
		}
	}

	chainConn, err := NewBitcoindConn(cfg)
	if err != nil {
		t.Fatalf("unable to establish connection to bitcoind: %v", err)
	}
	if err := chainConn.Start(); err != nil {
		t.Fatalf("unable to establish connection to bitcoind: %v", err)
	}

	t.Cleanup(func() {
		chainConn.Stop()
	})

	// Create a bitcoind client.
	btcClient := chainConn.NewBitcoindClient()
	if err := btcClient.Start(); err != nil {
		t.Fatalf("could not start bitcoind client: %v", err)
	}

	t.Cleanup(func() {
		btcClient.Stop()
	})

	if err := btcClient.NotifyBlocks(); err != nil {
		t.Fatalf("could not start NotifyBlocks: %v", err)
	}

	return btcClient
}

// randPubKeyHashScript generates a P2PKH script that pays to the public key of
// a randomly-generated private key.
func randPubKeyHashScript() ([]byte, *btcec.PrivateKey, error) {
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, nil, err
	}

	pubKeyHash := btcutil.Hash160(privKey.PubKey().SerializeCompressed())
	addrScript, err := btcutil.NewAddressPubKeyHash(
		pubKeyHash, &chaincfg.RegressionNetParams,
	)
	if err != nil {
		return nil, nil, err
	}

	pkScript, err := txscript.PayToAddrScript(addrScript)
	if err != nil {
		return nil, nil, err
	}

	return pkScript, privKey, nil
}
