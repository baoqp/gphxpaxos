package gphxkv

import (
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
	"github.com/syndtr/goleveldb/leveldb/opt"

	log "github.com/sirupsen/logrus"
	"errors"
	"github.com/gogo/protobuf/proto"
	"math"
	"strconv"
	"gphxpaxos/util"
)

var KV_OK error = nil
var KV_SYS_FAIL = errors.New("SYS_FAIL")
var KV_KEY_NOTEXIST = errors.New("KEY_NOTEXIST")
var KV_KEY_VERSION_CONFLICT = errors.New("KEY_VERSION_CONFLICT")

var KV_CHECKPOINT_KEY = strconv.FormatUint(math.MaxUint64, 10)

// 和leveldb交互的客户端
type LevelDBClient struct {
	leveldb *leveldb.DB
	hasInit bool
	mutex   sync.Mutex
}

var once sync.Once
var kvClient *LevelDBClient

//单例
func GetKVClient() *LevelDBClient {

	once.Do(func() {
		kvClient = &LevelDBClient{
			hasInit: false,
			leveldb: nil,
		}

	})

	return kvClient
}

func (c *LevelDBClient) Init(dbPath string) error {

	if c.hasInit {
		return nil
	}

	options := &opt.Options{
		ErrorIfMissing: false,
	}

	var err error
	c.leveldb, err = leveldb.OpenFile(dbPath, options)
	if err != nil {
		log.Errorf("open leveldb fail, db path:%s", dbPath)
		return err
	}

	c.hasInit = true

	return nil
}

func (c *LevelDBClient) Get(key []byte) (value []byte, version uint64, err error) {
	if !c.hasInit {
		log.Error("LevelDBClient no init yet")
		err = KV_SYS_FAIL
		return
	}

	data, err := c.getFromLevelDb(key)
	if err != nil {
		if err == KV_KEY_NOTEXIST {
			version = 0
			err = KV_KEY_NOTEXIST
			return
		}
		err = KV_SYS_FAIL
		return
	}

	var kvData KVData
	err = proto.Unmarshal(data, &kvData)
	if err != nil {
		log.Errorf("DB DATA wrong, key %s", string(key))
		err = KV_SYS_FAIL
		return
	}

	version = kvData.GetVersion()

	if kvData.GetIsdeleted() {
		log.Errorf("LevelDB.Get key already deleted, key %s", string(key))
		err = KV_KEY_NOTEXIST
		return
	}

	value = kvData.GetValue()
	err = KV_OK
	return
}

func (c *LevelDBClient) Set(key []byte, value []byte, version uint64) error {
	if !c.hasInit {
		log.Error("LevelDBClient no init yet")
		return KV_SYS_FAIL
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, serverVersion, err := c.Get(key)

	if err != nil && err != KV_KEY_NOTEXIST {
		return KV_SYS_FAIL
	}

	// TODO ???
	if serverVersion != version {
		return KV_KEY_VERSION_CONFLICT
	}

	serverVersion += 1
	data := &KVData{
		Value:     value,
		Version:   serverVersion,
		Isdeleted: false,
	}

	buffer, err := proto.Marshal(data)
	if err != nil {
		log.Error("Data.Marshal fail")
		return KV_SYS_FAIL
	}

	err = c.putToLevelDB(true, key, buffer)
	if err != nil {
		log.Errorf("LevelDB.Put fail, key %s", string(key))
		return KV_SYS_FAIL
	}

	return KV_OK

}

// 删除操作是把对应的删除标识记为true
func (c *LevelDBClient) Del(key []byte, version uint64) error {

	if !c.hasInit {
		log.Error("LevelDBClient no init yet")
		return KV_SYS_FAIL
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	serverValue, serverVersion, err := c.Get(key)

	if err != nil && err != KV_KEY_NOTEXIST {
		return KV_SYS_FAIL
	}

	if serverVersion != version {
		return KV_KEY_VERSION_CONFLICT
	}

	serverVersion += 1

	data := &KVData{
		Value:     serverValue,
		Version:   serverVersion,
		Isdeleted: false,
	}

	buffer, err := proto.Marshal(data)
	if err != nil {
		log.Error("Data.Marshal fail")
		return KV_SYS_FAIL
	}

	err = c.putToLevelDB(true, key, buffer)
	if err != nil {
		log.Errorf("LevelDB.Put fail, key %s", string(key))
		return KV_SYS_FAIL
	}

	return KV_OK
}

func (c *LevelDBClient) GetCheckpointInstanceId() (uint64, error) {
	if !c.hasInit {
		log.Error("LevelDBClient no init yet")
		return 0, KV_SYS_FAIL
	}

	data, err := c.getFromLevelDb([]byte(KV_CHECKPOINT_KEY))
	if err != nil {
		if err == KV_KEY_NOTEXIST {
			return 0, KV_KEY_NOTEXIST
		}
		return 0, KV_SYS_FAIL
	}
	var instanceId uint64
	util.DecodeUint64(data, 0, &instanceId)

	return instanceId, KV_OK
}

func (c *LevelDBClient) SetCheckpointInstanceId(instanceId uint64) (error) {

	if !c.hasInit {
		log.Error("LevelDBClient no init yet")
		return KV_SYS_FAIL
	}

	var buffer []byte
	util.EncodeUint64(buffer, 0, instanceId)

	err := c.putToLevelDB(true, []byte(KV_CHECKPOINT_KEY), buffer)
	if err != nil {
		return KV_SYS_FAIL
	}

	return KV_OK
}

func (c *LevelDBClient) getFromLevelDb(key []byte) ([]byte, error) {

	ret, err := c.leveldb.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Debug("leveldb.get not found, key %s ", string(key))
			return nil, KV_KEY_NOTEXIST
		}

		log.Errorf("leveldb.get fail, key %s ", string(key))
		return nil, err
	}

	return ret, nil
}

func (c *LevelDBClient) putToLevelDB(sync bool, key []byte, value []byte) error {

	options := opt.WriteOptions{
		Sync: sync,
	}

	err := c.leveldb.Put(key, value, &options)
	if err != nil {
		log.Errorf("leveldb put fail, key %s", string(key))
		return err
	}

	return nil
}
