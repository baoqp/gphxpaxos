package gphxkv

import (
	"errors"
	"math"
)

const (
	// enum KVOperatorType
	KVOperatorType_READ   uint32 = 1
	KVOperatorType_WRITE         = 2
	KVOperatorType_DELETE        = 3

	NULLVERSION = math.MaxUint64 // TODO
)

// enum PhxKVStatus
var (
	KVStatus_SUCC             error = nil
	KVStatus_FAIL                   = errors.New("FAIL")
	KVStatus_KEY_NOTEXIST           = errors.New("KEY_NOTEXIST")
	KVStatus_VERSION_CONFLICT       = errors.New("VERSION_CONFLICT")
	KVStatus_VERSION_NOTEXIST       = errors.New("VERSION_NOTEXIST")
	KVStatus_MASTER_REDIRECT        = errors.New("MASTER_REDIRECT")
	KVStatus_NO_MASTER              = errors.New("NO_MASTER")

	KVStatusCode_SUCC             int32 = 0
	KVStatusCode_FAIL             int32 = -1
	KVStatusCode_KEY_NOTEXIST     int32 = 1
	KVStatusCode_VERSION_CONFLICT int32 = -11
	KVStatusCode_VERSION_NOTEXIST int32 = -12
	KVStatusCode_MASTER_REDIRECT  int32 = 10
	KVStatusCode_NO_MASTER        int32 = 101
)

func KVStatusToCode(status error) int32 {

	if status == KVStatus_SUCC {
		return KVStatusCode_SUCC
	} else if status == KVStatus_KEY_NOTEXIST {
		return KVStatusCode_KEY_NOTEXIST
	} else if status == KVStatus_VERSION_CONFLICT {
		return KVStatusCode_VERSION_CONFLICT
	} else if status == KVStatus_VERSION_NOTEXIST {
		return KVStatusCode_VERSION_NOTEXIST
	} else if status == KVStatus_MASTER_REDIRECT {
		return KVStatusCode_MASTER_REDIRECT
	} else if status == KVStatus_NO_MASTER {
		return KVStatusCode_NO_MASTER
	}

	return KVStatusCode_FAIL
}

func CodeToKVStatus(code int32) error {
	if code == KVStatusCode_SUCC {
		return KVStatus_SUCC
	} else if code == KVStatusCode_KEY_NOTEXIST {
		return KVStatus_KEY_NOTEXIST
	} else if code == KVStatusCode_VERSION_CONFLICT {
		return KVStatus_VERSION_CONFLICT
	} else if code == KVStatusCode_VERSION_NOTEXIST {
		return KVStatus_VERSION_NOTEXIST
	} else if code == KVStatusCode_MASTER_REDIRECT {
		return KVStatus_MASTER_REDIRECT
	} else if code == KVStatusCode_NO_MASTER {
		return KVStatus_NO_MASTER
	}
	return KVStatus_FAIL
}
