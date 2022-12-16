// Copyright (c) 2017-2018 The qitmeer developers

package blkmgr

import (
	"container/list"
	"fmt"
	"github.com/Qitmeer/qng/common/hash"
	"github.com/Qitmeer/qng/common/roughtime"
	"github.com/Qitmeer/qng/config"
	"github.com/Qitmeer/qng/consensus/model"
	"github.com/Qitmeer/qng/core/blockchain"
	"github.com/Qitmeer/qng/core/types"
	"github.com/Qitmeer/qng/node/service"
	"github.com/Qitmeer/qng/params"
	"github.com/Qitmeer/qng/services/common/progresslog"
	"github.com/Qitmeer/qng/services/zmq"
	vmconsensus "github.com/Qitmeer/qng/vm/consensus"
	"sync"
	"time"
)

// BlockManager provides a concurrency safe block manager for handling all
// incoming blocks.
type BlockManager struct {
	service.Service

	config *config.Config
	params *params.Params

	notify vmconsensus.Notify

	chain *blockchain.BlockChain

	progressLogger *progresslog.BlockProgressLogger

	msgChan chan interface{}

	wg   sync.WaitGroup
	quit chan struct{}

	// The following fields are used for headers-first mode.
	headersFirstMode bool
	headerList       *list.List
	startHeader      *list.Element
	nextCheckpoint   *params.Checkpoint

	lastProgressTime time.Time

	// zmq notification
	zmqNotify zmq.IZMQNotification

	sync.Mutex

	//tx manager
	txManager TxManager

	// network server
	peerServer P2PService
}

// NewBlockManager returns a new block manager.
// Use Start to begin processing asynchronous block and inv updates.
func NewBlockManager(ntmgr vmconsensus.Notify, consensus model.Consensus, peerServer P2PService) (*BlockManager, error) {
	cfg := consensus.Config()
	par := params.ActiveNetParams.Params
	bm := BlockManager{
		config:         cfg,
		params:         par,
		notify:         ntmgr,
		progressLogger: progresslog.NewBlockProgressLogger("Processed", log),
		msgChan:        make(chan interface{}, cfg.MaxPeers*3),
		headerList:     list.New(),
		quit:           make(chan struct{}),
		peerServer:     peerServer,
		chain:          consensus.BlockChain().(*blockchain.BlockChain), // TODO:Future optimized interface
	}

	best := bm.chain.BestSnapshot()
	bm.chain.DisableCheckpoints(cfg.DisableCheckpoints)
	if !cfg.DisableCheckpoints {
		// Initialize the next checkpoint based on the current height.
		bm.nextCheckpoint = bm.findNextHeaderCheckpoint(uint64(best.GraphState.GetMainHeight()))
		if bm.nextCheckpoint != nil {
			bm.resetHeaderState(&best.Hash, uint64(best.GraphState.GetMainHeight()))
		}
	} else {
		log.Info("Checkpoints are disabled")
	}

	bm.zmqNotify = zmq.NewZMQNotification(cfg)

	bm.chain.Subscribe(bm.handleNotifyMsg)

	bm.InitServices()
	bm.Services().RegisterService(bm.chain.BlockDAG())
	return &bm, nil
}

// handleNotifyMsg handles notifications from blockchain.  It does things such
// as request orphan block parents and relay accepted blocks to connected peers.
func (b *BlockManager) handleNotifyMsg(notification *blockchain.Notification) {
	switch notification.Type {
	// A block has been accepted into the block chain.  Relay it to other peers
	// and possibly notify RPC clients with the winning tickets.
	case blockchain.BlockAccepted:
		band, ok := notification.Data.(*blockchain.BlockAcceptedNotifyData)
		if !ok {
			log.Warn("Chain accepted notification is not " +
				"BlockAcceptedNotifyData.")
			break
		}
		block := band.Block
		if band.Flags&blockchain.BFP2PAdd == blockchain.BFP2PAdd {
			b.progressLogger.LogBlockHeight(block)
			// reset last progress time
			b.lastProgressTime = roughtime.Now()
		}
		b.zmqNotify.BlockAccepted(block)
		// Don't relay if we are not current. Other peers that are current
		// should already know about it
		if !b.peerServer.IsCurrent() {
			log.Trace("we are not current")
			return
		}
		log.Trace("we are current, can do relay")

		// Send a winning tickets notification as needed.  The notification will
		// only be sent when the following conditions hold:
		//
		// - The RPC server is running
		// - The block that would build on this one is at or after the height
		//   voting begins
		// - The block that would build on this one would not cause a reorg
		//   larger than the max reorg notify depth
		// - This block is after the final checkpoint height
		// - A notification for this block has not already been sent
		//
		// To help visualize the math here, consider the following two competing
		// branches:
		//
		// 100 -> 101  -> 102  -> 103 -> 104 -> 105 -> 106
		//    \-> 101' -> 102'
		//
		// Further, assume that this is a notification for block 103', or in
		// other words, it is extending the shorter side chain.  The reorg depth
		// would be 106 - (103 - 3) = 6.  This should intuitively make sense,
		// because if the side chain were to be extended enough to become the
		// best chain, it would result in a a reorg that would remove 6 blocks,
		// namely blocks 101, 102, 103, 104, 105, and 106.
		b.notify.RelayInventory(block.Block().Header, nil)

	// A block has been connected to the main block chain.
	case blockchain.BlockConnected:
		log.Trace("Chain connected notification.")
		blockSlice, ok := notification.Data.([]interface{})
		if !ok {
			log.Warn("Chain connected notification is not a block slice.")
			break
		}

		if len(blockSlice) != 2 {
			log.Warn("Chain connected notification is wrong size slice.")
			break
		}

		block := blockSlice[0].(*types.SerializedBlock)
		// Remove all of the transactions (except the coinbase) in the
		// connected block from the transaction pool.  Secondly, remove any
		// transactions which are now double spends as a result of these
		// new transactions.  Finally, remove any transaction that is
		// no longer an orphan. Transactions which depend on a confirmed
		// transaction are NOT removed recursively because they are still
		// valid.
		txds := []*types.TxDesc{}
		for _, tx := range block.Transactions()[1:] {
			b.GetTxManager().MemPool().RemoveTransaction(tx, false)
			b.GetTxManager().MemPool().RemoveDoubleSpends(tx)
			b.GetTxManager().MemPool().RemoveOrphan(tx.Hash())
			b.notify.TransactionConfirmed(tx)
			acceptedTxs := b.GetTxManager().MemPool().ProcessOrphans(tx.Hash())
			txds = append(txds, acceptedTxs...)
		}
		b.notify.AnnounceNewTransactions(txds, nil)

		/*
			if r := b.server.rpcServer; r != nil {
				// Notify registered websocket clients of incoming block.
				r.ntfnMgr.NotifyBlockConnected(block)
			}
		*/

		// Register block with the fee estimator, if it exists.
		if b.txManager.FeeEstimator() != nil && blockSlice[1].(bool) {
			err := b.txManager.FeeEstimator().RegisterBlock(block)

			// If an error is somehow generated then the fee estimator
			// has entered an invalid state. Since it doesn't know how
			// to recover, create a new one.
			if err != nil {
				b.txManager.InitDefaultFeeEstimator()
			}
		}

		b.zmqNotify.BlockConnected(block)

	// A block has been disconnected from the main block chain.
	case blockchain.BlockDisconnected:
		log.Trace("Chain disconnected notification.")
		block, ok := notification.Data.(*types.SerializedBlock)
		if !ok {
			log.Warn("Chain disconnected notification is not a block slice.")
			break
		}
		// Rollback previous block recorded by the fee estimator.
		if b.txManager.FeeEstimator() != nil {
			b.txManager.FeeEstimator().Rollback(block.Hash())
		}
		b.zmqNotify.BlockDisconnected(block)
	// The blockchain is reorganizing.
	case blockchain.Reorganization:
		log.Trace("Chain reorganization notification")
		/*
			rd, ok := notification.Data.(*blockchain.ReorganizationNotifyData)
			if !ok {
				log.Warn("Chain reorganization notification is malformed")
				break
			}

			// Notify registered websocket clients.
			if r := b.server.rpcServer; r != nil {
				r.ntfnMgr.NotifyReorganization(rd)
			}

			// Drop the associated mining template from the old chain, since it
			// will be no longer valid.
			b.cachedCurrentTemplate = nil
		*/
	}
}

func (b *BlockManager) IsCurrent() bool {
	return b.peerServer.IsCurrent()
}

// Start begins the core block handler which processes block and inv messages.
func (b *BlockManager) Start() error {
	if err := b.Service.Start(); err != nil {
		return err
	}

	log.Trace("Starting block manager")
	b.wg.Add(1)
	go b.blockHandler()
	return nil
}

func (b *BlockManager) Stop() error {
	log.Info("try stop bm")
	if err := b.Service.Stop(); err != nil {
		return err
	}
	log.Info("Block manager shutting down")
	close(b.quit)

	// shutdown zmq
	b.zmqNotify.Shutdown()

	b.WaitForStop()
	return nil
}

func (b *BlockManager) WaitForStop() {
	log.Info("Wait For Block manager stop ...")
	b.wg.Wait()
	log.Info("Block manager stopped")
}

// findNextHeaderCheckpoint returns the next checkpoint after the passed layer.
// It returns nil when there is not one either because the height is already
// later than the final checkpoint or some other reason such as disabled
// checkpoints.
func (b *BlockManager) findNextHeaderCheckpoint(layer uint64) *params.Checkpoint {
	// There is no next checkpoint if checkpoints are disabled or there are
	// none for this current network.
	if b.config.DisableCheckpoints {
		return nil
	}
	checkpoints := b.params.Checkpoints
	if len(checkpoints) == 0 {
		return nil
	}

	// There is no next checkpoint if the height is already after the final
	// checkpoint.
	finalCheckpoint := &checkpoints[len(checkpoints)-1]
	if layer >= finalCheckpoint.Layer {
		return nil
	}

	// Find the next checkpoint.
	nextCheckpoint := finalCheckpoint
	for i := len(checkpoints) - 2; i >= 0; i-- {
		if layer >= checkpoints[i].Layer {
			break
		}
		nextCheckpoint = &checkpoints[i]
	}
	return nextCheckpoint
}

// resetHeaderState sets the headers-first mode state to values appropriate for
// syncing from a new peer.
func (b *BlockManager) resetHeaderState(newestHash *hash.Hash, newestHeight uint64) {
	b.headersFirstMode = false
	b.headerList.Init()
	b.startHeader = nil

	// When there is a next checkpoint, add an entry for the latest known
	// block into the header pool.  This allows the next downloaded header
	// to prove it links to the chain properly.
	if b.nextCheckpoint != nil {
		node := headerNode{height: newestHeight, hash: newestHash}
		b.headerList.PushBack(&node)
	}
}

func (b *BlockManager) blockHandler() {
out:
	for {
		select {
		case m := <-b.msgChan:
			log.Trace("blkmgr msgChan received ...", "msg", m)
			switch msg := m.(type) {
			case processBlockMsg:
				log.Trace("blkmgr msgChan processBlockMsg", "msg", msg)

				if msg.flags.Has(blockchain.BFRPCAdd) {
					err := b.chain.BlockDAG().CheckSubMainChainTip(msg.block.Block().Parents)
					if err != nil {
						msg.reply <- ProcessBlockResponse{
							IsOrphan:      false,
							Err:           fmt.Errorf("The tips of block is expired:%s (error:%s)\n", msg.block.Hash().String(), err.Error()),
							IsTipsExpired: true,
						}
						continue
					}
				}

				isOrphan, err := b.chain.ProcessBlock(
					msg.block, msg.flags)
				if err != nil {
					msg.reply <- ProcessBlockResponse{
						IsOrphan:      isOrphan,
						Err:           err,
						IsTipsExpired: false,
					}
					continue
				}

				// If the block added to the dag chain, then we need to
				// update the tip locally on block manager.
				if !isOrphan {
					// TODO, decoupling mempool with bm
					b.GetTxManager().MemPool().PruneExpiredTx()
				}

				// Allow any clients performing long polling via the
				// getblocktemplate RPC to be notified when the new block causes
				// their old block template to become stale.
				// TODO, re-impl the client notify by subscript/publish
				/*
					rpcServer := b.rpcServer
					if rpcServer != nil {
						rpcServer.gbtWorkState.NotifyBlockConnected(msg.block.Hash())
					}
				*/

				msg.reply <- ProcessBlockResponse{
					IsOrphan:      isOrphan,
					Err:           nil,
					IsTipsExpired: false,
				}

			case processTransactionMsg:
				log.Trace("blkmgr msgChan processTransactionMsg", "msg", msg)
				acceptedTxs, err := b.GetTxManager().MemPool().ProcessTransaction(msg.tx,
					msg.allowOrphans, msg.rateLimit, msg.allowHighFees)
				msg.reply <- processTransactionResponse{
					acceptedTxs: acceptedTxs,
					err:         err,
				}
			case isCurrentMsg:
				log.Trace("blkmgr msgChan isCurrentMsg", "msg", msg)
				msg.isCurrentReply <- b.IsCurrent()

			default:
				log.Error("Unknown message type", "msg", msg)
			}

		case <-b.quit:
			log.Trace("blkmgr quit received, break out")
			break out
		}
	}
	b.wg.Done()
	log.Trace("Block handler done")
}

// processBlockResponse is a response sent to the reply channel of a
// processBlockMsg.
type ProcessBlockResponse struct {
	IsOrphan      bool
	Err           error
	IsTipsExpired bool
}

// processBlockMsg is a message type to be sent across the message channel
// for requested a block is processed.  Note this call differs from blockMsg
// above in that blockMsg is intended for blocks that came from peers and have
// extra handling whereas this message essentially is just a concurrent safe
// way to call ProcessBlock on the internal block chain instance.
type processBlockMsg struct {
	block *types.SerializedBlock
	flags blockchain.BehaviorFlags
	reply chan ProcessBlockResponse
}

// ProcessBlock makes use of ProcessBlock on an internal instance of a block
// chain.  It is funneled through the block manager since blockchain is not safe
// for concurrent access.
func (b *BlockManager) ProcessBlock(block *types.SerializedBlock, flags blockchain.BehaviorFlags) ProcessBlockResponse {
	reply := make(chan ProcessBlockResponse, 1)
	b.msgChan <- processBlockMsg{block: block, flags: flags, reply: reply}
	response := <-reply
	return response
}

// processTransactionResponse is a response sent to the reply channel of a
// processTransactionMsg.
type processTransactionResponse struct {
	acceptedTxs []*types.TxDesc
	err         error
}

// processTransactionMsg is a message type to be sent across the message
// channel for requesting a transaction to be processed through the block
// manager.
type processTransactionMsg struct {
	tx            *types.Tx
	allowOrphans  bool
	rateLimit     bool
	allowHighFees bool
	reply         chan processTransactionResponse
}

// ProcessTransaction makes use of ProcessTransaction on an internal instance of
// a block chain.  It is funneled through the block manager since blockchain is
// not safe for concurrent access.
func (b *BlockManager) ProcessTransaction(tx *types.Tx, allowOrphans bool,
	rateLimit bool, allowHighFees bool) ([]*types.TxDesc, error) {
	reply := make(chan processTransactionResponse, 1)
	b.msgChan <- processTransactionMsg{tx, allowOrphans, rateLimit,
		allowHighFees, reply}
	response := <-reply
	return response.acceptedTxs, response.err
}

// isCurrentMsg is a message type to be sent across the message channel for
// requesting whether or not the block manager believes it is synced with
// the currently connected peers.
type isCurrentMsg struct {
	isCurrentReply chan bool
}

// IsCurrent returns whether or not the block manager believes it is synced with
// the connected peers.
func (b *BlockManager) Current() bool {
	reply := make(chan bool)
	log.Trace("send isCurrentMsg to blkmgr msgChan")
	b.msgChan <- isCurrentMsg{isCurrentReply: reply}
	return <-reply
}

// Return chain params
func (b *BlockManager) ChainParams() *params.Params {
	return b.params
}

func (b *BlockManager) SetTxManager(txManager TxManager) {
	b.txManager = txManager
}

func (b *BlockManager) GetTxManager() TxManager {
	return b.txManager
}

// headerNode is used as a node in a list of headers that are linked together
// between checkpoints.
type headerNode struct {
	height uint64
	hash   *hash.Hash
}
