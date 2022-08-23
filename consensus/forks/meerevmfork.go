package forks

import (
	"github.com/Qitmeer/qng/core/protocol"
	"github.com/Qitmeer/qng/core/types"
	"github.com/Qitmeer/qng/engine/txscript"
	"github.com/Qitmeer/qng/params"
)

const (
	// What main height qng fork
	MeerEVMForkMainHeight = 959000

	// What main height can transfer the locked utxo in genesis to MeerVM
	MeerEVMValidMainHeight = 959000

	// 21024000000000000 (Total)-5051813000000000 (locked genesis)-1215912000000000 (meerevm genesis) = 14756275000000000
	MeerEVMForkTotalSubsidy = 14756275000000000

	// subsidy reduction interval
	SubsidyReductionInterval = 7358400

	// Subsidy reduction multiplier.
	MulSubsidy = 100
	// Subsidy reduction divisor.
	DivSubsidy = 101
)

func IsMeerEVMValid(tx *types.Transaction, ip *types.TxInput, mainHeight int64) bool {
	if params.ActiveNetParams.Net != protocol.MainNet {
		return false
	}
	if mainHeight < MeerEVMForkMainHeight ||
		mainHeight < MeerEVMValidMainHeight {
		return false
	}
	if !types.IsCrossChainExportTx(tx) {
		return false
	}
	return IsMaxLockUTXOInGenesis(&ip.PreviousOut)
}

func IsMaxLockUTXOInGenesis(op *types.TxOutPoint) bool {
	gblock := params.ActiveNetParams.GenesisBlock
	for _, tx := range gblock.Transactions {
		if tx.CachedTxHash().IsEqual(&op.Hash) {
			if op.OutIndex >= uint32(len(tx.TxOut)) {
				return false
			}
			ops, err := txscript.ParseScript(tx.TxOut[op.OutIndex].PkScript)
			if err != nil {
				return false
			}
			if ops[1].GetOpcode().GetValue() != txscript.OP_CHECKLOCKTIMEVERIFY {
				return false
			}
			lockTime := txscript.GetInt64FromOpcode(ops[0])
			if lockTime == params.ActiveNetParams.LedgerParams.MaxLockHeight {
				return true
			}
			return false
		}
	}
	return false
}

func IsMeerEVMForkHeight(mainHeight int64) bool {
	if params.ActiveNetParams.Net != protocol.MainNet {
		return false
	}
	return mainHeight >= MeerEVMForkMainHeight
}

func IsMeerEVMValidHeight(mainHeight int64) bool {
	if params.ActiveNetParams.Net != protocol.MainNet {
		return false
	}
	return mainHeight >= MeerEVMValidMainHeight
}
