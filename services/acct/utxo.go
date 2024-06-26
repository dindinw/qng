package acct

import (
	"fmt"
	s "github.com/Qitmeer/qng/core/serialization"
	"io"
)

const (
	AddressUTXOsSuffix = "-utxos"

	NormalUTXOType   = 0
	CoinbaseUTXOType = 1
	CLTVUTXOType     = 2
	TokenUTXOType    = 3
)

type AcctUTXO struct {
	typ     byte
	balance uint64
}

func (au *AcctUTXO) Encode(w io.Writer) error {
	err := s.WriteElements(w, au.typ)
	if err != nil {
		return err
	}
	err = s.WriteElements(w, au.balance)
	if err != nil {
		return err
	}
	return nil
}

func (au *AcctUTXO) Decode(r io.Reader) error {
	err := s.ReadElements(r, &au.typ)
	if err != nil {
		return err
	}

	err = s.ReadElements(r, &au.balance)
	if err != nil {
		return err
	}
	return nil
}

func (au *AcctUTXO) String() string {
	return fmt.Sprintf("type=%s balance=%d", au.TypeStr(), au.balance)
}

func (au *AcctUTXO) TypeStr() string {
	switch au.typ {
	case NormalUTXOType:
		return "normal"
	case CoinbaseUTXOType:
		return "coinbase"
	case CLTVUTXOType:
		return "CLTV"
	case TokenUTXOType:
		return "token"
	}
	return "unknown"
}

func (au *AcctUTXO) SetCoinbase() {
	au.typ = CoinbaseUTXOType
}

func (au *AcctUTXO) IsCoinbase() bool {
	return au.typ == CoinbaseUTXOType
}

func (au *AcctUTXO) SetCLTV() {
	au.typ = CLTVUTXOType
}

func (au *AcctUTXO) IsCLTV() bool {
	return au.typ == CLTVUTXOType
}

func (au *AcctUTXO) SetBalance(balance uint64) {
	au.balance = balance
}

func NewAcctUTXO() *AcctUTXO {
	au := AcctUTXO{
		typ: NormalUTXOType,
	}

	return &au
}
