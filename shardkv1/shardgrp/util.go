package shardgrp

import (
	"bytes"
	"log"

	"6.5840/labgob"
)

// Debugging
// const Debug = false

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func EncodeEmptyState() []byte {
	data := make(map[string]DataValue)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(data)
	return w.Bytes()
}

func EncodeShardState(data *map[string]DataValue) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*data)
	return w.Bytes()
}

func DecodeShardState(buffer []byte, data *map[string]DataValue) {
	r := bytes.NewBuffer(buffer)
	d := labgob.NewDecoder(r)
	if d.Decode(data) != nil {
		log.Fatalf("decoder couldn't decode data")
	}
}
