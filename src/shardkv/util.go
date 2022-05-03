package shardkv

import "log"

// Debugging
const Debug = false
const Debug2 = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func D2Printf(format string, a ...interface{}) (n int, err error) {
	if Debug2 {
		log.Printf(format, a...)
	}
	return
}
