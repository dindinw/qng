package qx

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/Qitmeer/qng/core/types"
	"math"
	"strconv"
	"strings"
)

type TxVersionFlag uint32
type TxLockTimeFlag uint32

func (ver TxVersionFlag) String() string {
	return fmt.Sprintf("%d", ver)
}
func (ver *TxVersionFlag) Set(s string) error {
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return err
	}
	*ver = TxVersionFlag(uint32(v))
	return nil
}

func (lt TxLockTimeFlag) String() string {
	return fmt.Sprintf("%d", lt)
}
func (lt *TxLockTimeFlag) Set(s string) error {
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return err
	}
	*lt = TxLockTimeFlag(uint32(v))
	return nil
}

type LockAddress struct {
	Address  string
	SignType int64
	Args     []byte
}

type TxInputsFlag struct {
	inputs []txInput
}
type TxOutputsFlag struct {
	outputs []txOutput
}

type txInput struct {
	txhash   []byte
	index    uint32
	sequence uint32
	txtype   string
	address  string
}
type txOutput struct {
	target string
	amount float64
	coinid int64
	txtype string
}

func (i LockAddress) String() string {
	return fmt.Sprintf("%s:%d:%s", i.Address, i.SignType, string(i.Args))
}
func (i txInput) String() string {
	return fmt.Sprintf("%x:%d:%d:%s:%s", i.txhash[:], i.index, i.sequence, i.txtype, i.address)
}
func (o txOutput) String() string {
	return fmt.Sprintf("%s:%f:%d:%s", o.target, o.amount, o.coinid, o.txtype)
}

func (v TxInputsFlag) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for _, input := range v.inputs {
		buffer.WriteString(input.String())
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (of TxOutputsFlag) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for _, o := range of.outputs {
		buffer.WriteString(o.String())
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (v *TxInputsFlag) Set(s string) error {
	input := strings.Split(s, ":")
	if len(input) < 2 {
		return fmt.Errorf("error to parse tx input : %s", s)
	}
	data, err := hex.DecodeString(input[0])
	if err != nil {
		return err
	}
	if len(data) != 32 {
		return fmt.Errorf("tx hash should be 32 bytes")
	}

	index, err := strconv.ParseUint(input[1], 10, 32)
	if err != nil {
		return err
	}
	var seq = uint32(math.MaxUint32)
	if len(input) >= 3 {
		s, err := strconv.ParseUint(input[2], 10, 32)
		if err != nil {
			return err
		}
		seq = uint32(s)
	}
	txtype := types.TxTypeRegular.String()
	if len(input) >= 4 {
		txtype = input[3]
	}
	addr := ""
	if len(input) == 5 {
		addr = input[4]
	}
	i := txInput{
		data,
		uint32(index),
		uint32(seq),
		txtype,
		addr,
	}
	v.inputs = append(v.inputs, i)
	return nil
}

func (of *TxOutputsFlag) Set(s string) error {
	output := strings.Split(s, ":")
	if len(output) < 3 {
		return fmt.Errorf("error to parse tx output : %s", s)
	}
	target := output[0]
	amount, err := strconv.ParseFloat(output[1], 64)
	if err != nil {
		return err
	}
	coinid, err := strconv.ParseInt(output[2], 10, 64)
	if err != nil {
		return err
	}
	txtype := types.TxTypeRegular.String()
	if len(output) == 4 {
		txtype = output[3]
	}
	of.outputs = append(of.outputs, txOutput{
		target, amount, coinid, txtype})
	return nil
}
