package gphxpaxos

import (
	"os"
	"sync"
	log "github.com/sirupsen/logrus"
	"fmt"
	"gphxpaxos/util"
	"bytes"
	"strconv"
	"github.com/golang/protobuf/proto"
)

// 存储value
type LogStore struct {
	path             string
	metaFile         *os.File
	fileId           int32
	nowFileOffset    uint64
	nowFileSize      uint64
	file             *os.File
	readMutex        sync.Mutex
	mutex            sync.Mutex
	deletedMaxFileId int32 // 已删除的最大的fielId
}

func NewLogStore() *LogStore {
	return &LogStore{file: nil,
		metaFile: nil,
		fileId: -1,
		deletedMaxFileId: -1,
		nowFileSize: 0,
		nowFileOffset: 0}
}

func (logStore *LogStore) Init(path string, db *Database) error {
	logStore.deletedMaxFileId = -1
	logStore.path = path + "/vfile" // value file 所在目录
	logStore.file = nil
	logStore.fileId = -1

	//这里用exists判断，linux中使用 syscall.Access(logStore.path, syscall.F_OK)
	exists, _ := util.Exists(logStore.path)
	if !exists {
		err := os.MkdirAll(logStore.path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("create dir %s error: %v", logStore.path, err)
		}
	}

	// 读取meta文件，其中保存了当前的需要写入的vfile id， 其内容结构为
	// current file id(int32) + file id cksum(uint32)
	var metaFilePath = logStore.path + "/meta"
	metaFile, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return fmt.Errorf("open meta file %s error: %v", metaFilePath, err)
	}
	logStore.metaFile = metaFile

	_, err = metaFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("seek file %s error %v", metaFile.Name(), err)
	}

	buff := make([]byte, INT32SIZE) // 读取当前的file id
	n, err := metaFile.Read(buff)
	if n != INT32SIZE {
		if n == 0 { // no content
			logStore.fileId = 0
		} else {
			return fmt.Errorf("read meta file %s info fail, real len %d", metaFile.Name(), n)
		}
	}
	util.DecodeInt32(buff, 0, &logStore.fileId) // 设置fileId
	ckSum := util.Crc32(0, buff, 0)             // file id 编码为crc32

	var metaCkSum uint32
	buff = make([]byte, UINT32SIZE)
	n, err = metaFile.Read(buff)
	if n == UINT32SIZE {
		util.DecodeUint32(buff, 0, &metaCkSum)
		if metaCkSum != ckSum { // 检查checksum
			return fmt.Errorf("meta file checksum %d not same to calc checksum %d, file id %d",
				metaCkSum, ckSum, logStore.fileId)
		}
	}

	err = logStore.rebuildIndex(db, &logStore.nowFileOffset)
	if err != nil {
		return err
	}

	logStore.file, err = logStore.OpenFile(logStore.fileId)
	if err != nil {
		return err
	}

	// 扩展文件，只有在初始文件为空时，会生成一个固定大小的文件
	err = logStore.expandFile(logStore.file, &logStore.nowFileSize)
	if err != nil {
		return err
	}

	nowFileOffset, err := logStore.file.Seek(int64(logStore.nowFileOffset), os.SEEK_SET)
	if err != nil {
		return err
	}
	logStore.nowFileOffset = uint64(nowFileOffset)

	log.Infof("init write fileid %d now_write_offset %d filesize %d",
		logStore.fileId, logStore.nowFileOffset, logStore.nowFileSize)

	log.Infof("ok, path %s fileid %d meta cksum %d nowfilesize %d nowfilewriteoffset %d",
		logStore.path, logStore.fileId, metaCkSum, logStore.nowFileSize, logStore.nowFileOffset)

	return nil
}

func (logStore *LogStore) Close() {
	logStore.metaFile.Close()
	logStore.file.Close()
}

// 追加value， 参看value文件的数据格式
func (logStore *LogStore) Append(options *WriteOptions, instanceId uint64, buffer []byte, fileIdStr *string) error {
	begin := util.NowTimeMs()

	logStore.mutex.Lock()
	defer logStore.mutex.Unlock()

	bufferLen := len(buffer)
	len := UINT64SIZE + bufferLen
	tmpBufLen := len + INT32SIZE

	var fileId int32
	var offset uint32
	err := logStore.getFileId(uint32(tmpBufLen), &fileId, &offset)
	if err != nil {
		return err
	}

	tmpBuf := make([]byte, tmpBufLen)
	util.EncodeInt32(tmpBuf, 0, int32(len))
	util.EncodeUint64(tmpBuf, INT32SIZE, instanceId)
	copy(tmpBuf[INT32SIZE+UINT64SIZE:], []byte(buffer))

	ret, err := logStore.file.Write(tmpBuf) // 写入vfile
	if ret != tmpBufLen {
		err = fmt.Errorf("writelen %d not equal to %d,buffer size %d",
			ret, tmpBufLen, bufferLen)
		return err
	}

	if options.Sync {
		// 在windows下sync只把数据写入磁盘，不写元数据，由于我们这里使用了固定大小的元素，所以不需要写入元数据
		// 如果要同时写入数据和元数据使用 syscall.FlushFileBuffers()
		// 在Linux中Sync是同时写入数据和元数据，fdatasync只写入数据
		logStore.file.Sync()

	}

	logStore.nowFileOffset += uint64(tmpBufLen)

	ckSum := util.Crc32(0, tmpBuf[INT32SIZE:], CRC32_SKIP)
	logStore.EncodeFileId(fileId, uint64(offset), ckSum, fileIdStr)

	useMs := util.NowTimeMs() - begin

	log.Infof("ok, offset %d fileid %d cksum %d instanceid %d buffersize %d usetime %d ms sync %t",
		offset, fileId, ckSum, instanceId, bufferLen, useMs, options.Sync)
	return nil
}

// 读取value
func (logStore *LogStore) Read(fileIdstr string, instanceId *uint64) ([]byte, error) {
	var fileId int32
	var offset uint64
	var cksum uint32
	logStore.DecodeFileId(fileIdstr, &fileId, &offset, &cksum)

	file, err := logStore.OpenFile(fileId)
	if err != nil {
		log.Errorf("open file %s error %v", fileId, err)
		return nil, err
	}

	_, err = file.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	tmpbuf := make([]byte, INT32SIZE)
	n, err := file.Read(tmpbuf)
	if err != nil {
		return nil, err
	}
	if n != INT32SIZE {
		return nil, fmt.Errorf("read len %d not equal to %d", n, INT32SIZE)
	}

	var bufferlen int32
	util.DecodeInt32(tmpbuf, 0, &bufferlen)

	logStore.readMutex.Lock()
	defer logStore.readMutex.Unlock()

	tmpbuf = make([]byte, bufferlen)
	n, err = file.Read(tmpbuf)
	if err != nil {
		return nil, err
	}

	if n != int(bufferlen) {
		return nil, fmt.Errorf("read len %d not equal to %d", n, bufferlen)
	}

	fileCkSum := util.Crc32(0, tmpbuf, CRC32_SKIP)
	if fileCkSum != cksum {
		return nil, fmt.Errorf("cksum not equal, file cksum %d, cksum %d", fileCkSum, cksum)
	}

	util.DecodeUint64(tmpbuf, 0, instanceId)

	return tmpbuf[UINT64SIZE:], nil
}

// 删除fileId之前的vfile
func (logStore *LogStore) Del(fileIdStr string, instanceId uint64) error {
	var fileId int32 = -1
	var offset uint64
	var cksum uint32
	logStore.DecodeFileId(fileIdStr, &fileId, &offset, &cksum)

	if fileId > logStore.fileId {
		return fmt.Errorf("del fileid %d larger than using fileid %d", fileId, logStore.fileId)
	}

	if fileId > 0 {
		return logStore.DeleteFile(fileId - 1)
	}

	return nil
}

// 清空fileid指向文件中fileid中的offset所指向的后面内容
func (logStore *LogStore) ForceDel(fileIdStr string, instanceId uint64) error {
	var fileId int32
	var offset uint64
	var cksum uint32
	logStore.DecodeFileId(fileIdStr, &fileId, &offset, &cksum)

	if logStore.fileId != fileId {
		err := fmt.Errorf("del fileid %d not equal to fileid %d", fileId, logStore.fileId)
		log.Error(err)
		return err
	}

	filePath := fmt.Sprintf("%s/%d.f", logStore.path, fileId)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	file.Truncate(int64(offset))
	return nil
}

// 删除fileId <= 指定fileId的vfile
func (logStore *LogStore) DeleteFile(fileId int32) error {
	if logStore.deletedMaxFileId == -1 {
		if fileId - 2000 > 0 {
			logStore.deletedMaxFileId = fileId - 2000
		}
	}

	if fileId <= logStore.deletedMaxFileId {
		log.Debug("file already deleted, fileid %d deletedmaxfileid %d", fileId, logStore.deletedMaxFileId)
		return nil
	}

	var err error
	for deleteFileId := logStore.deletedMaxFileId + 1; deleteFileId <= fileId; deleteFileId++ {
		filePath := fmt.Sprintf("%s/%d.f", logStore.path, deleteFileId)

		exists, err := util.Exists(filePath)
		if !exists {
			log.Debug("file already deleted, filepath %s", filePath)
			logStore.deletedMaxFileId = deleteFileId
			err = nil
			continue
		}

		err = os.Remove(filePath)
		if err != nil {
			log.Errorf("remove fail, file path %s error: %v", filePath, err)
			break
		}

		logStore.deletedMaxFileId = deleteFileId
		log.Infof("delete fileid %d", deleteFileId)
	}

	return err
}

// nowOffset 初始为文件开头
func (logStore *LogStore) rebuildIndex(db *Database, nowOffset *uint64) error {
	// 1. get max instance id and file id from leveldb 从level db 获取最大的instance id和file id
	lastFileId, nowInstanceId, err := db.GetMaxInstanceIdFileId()
	if err != nil {
		return err
	}

	// 2. decode last file id info
	var fileId int32
	var offset uint64
	var cksum uint32
	if len(lastFileId) > 0 {
		logStore.DecodeFileId(lastFileId, &fileId, &offset, &cksum)
	}

	if fileId > logStore.fileId { // 数据一致检查
		return fmt.Errorf("leveldb last fileid %d lagger than meta now fileid %d", fileId, logStore.fileId)
	}


	for nowFileId := fileId; ; nowFileId++ {
		err = logStore.RebuildIndexForOneFile(nowFileId, offset, db, nowOffset, &nowInstanceId)
		if err != nil {
			if err == ErrFileNotExist {
				if nowFileId != 0 && nowFileId != logStore.fileId + 1 {
					err = fmt.Errorf("meta file wrong, now file id %d meta file id %d", nowFileId, logStore.fileId)
					log.Errorf("%v", err)
					return ErrInvalidMetaFileId
				}
				log.Infof("end rebuild ok, now file id:%d", nowFileId)
				err = nil
			}
			break
		}

		offset = 0
	}

	return err
}

// 从vfile中读取已存储的值并编码成LevelDB中fileId的形式保存到LevelDB
func (logStore *LogStore) RebuildIndexForOneFile(fileId int32, offset uint64,
	db *Database, nowWriteOffset *uint64, nowInstanceId *uint64) error {

	var err error = nil
	var file *os.File = nil

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	// value file 的格式是1.f,2.f,...
	filePath := fmt.Sprintf("%s/%d.f", logStore.path, fileId)

	exists, _ := util.Exists(filePath)
	if !exists {
		return ErrFileNotExist
	}

	file, err = logStore.OpenFile(fileId)
	if err != nil {
		return err
	}

	fileLen, err := file.Seek(0, os.SEEK_END) // 文件大小
	if err != nil {
		file.Close()
		return err
	}

	_, err = file.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		file.Close()
		return err
	}

	var nowOffset = offset
	var needTruncate = false
	// value 在log_store中的存储是 长度len(32 bytes) + value_content(len bytes)
	// 其中value_content由 instanceId + AcceptorStateData 组成
	for {
		buffer := make([]byte, INT32SIZE)
		n, err := file.Read(buffer)
		if n == 0 {
			*nowWriteOffset = nowOffset // 到达文件末尾
			err = nil
			break
		}

		if n != INT32SIZE {
			needTruncate = true
			log.Errorf("read len %d not equal to %d, need truncate", n, INT32SIZE)
			err = nil
			break
		}

		len, _ := strconv.Atoi(string(buffer))
		if len == 0 {
			*nowWriteOffset = nowOffset
			log.Debugf("file end, file id %d offset %d", fileId, nowOffset)
			break
		}

		if int64(len) > fileLen || len < UINT64SIZE {
			err = fmt.Errorf("file data len wrong, data len %d filelen %d", len, fileLen)
			log.Error(err)
			break
		}

		buffer = make([]byte, len)
		n, err = file.Read(buffer)
		if n != len {
			needTruncate = true
			log.Errorf("read len %d not equal to %d, need truncate", n, len)
			break
		}

		var instanceId uint64
		util.DecodeUint64(buffer, 0, &instanceId)

		if instanceId < *nowInstanceId {
			log.Errorf("file data wrong, read instanceid %d smaller than now instanceid %d", instanceId, *nowInstanceId)
			err = ErrInvalidInstanceId
			break
		}

		*nowInstanceId = instanceId

		var state AcceptorStateData
		err = proto.Unmarshal(buffer[UINT64SIZE:], &state)
		if err != nil {
			logStore.nowFileOffset = uint64(nowOffset)
			needTruncate = true
			log.Errorf("this instance buffer wrong, can't parse to acceptState, instanceid %d bufferlen %d nowoffset %d",
				instanceId, len-UINT64SIZE, nowOffset)
			err = nil
			break
		}

		fileCkSum := util.Crc32(0, buffer, CRC32_SKIP)
		var fileIdstr string
		logStore.EncodeFileId(fileId, nowOffset, fileCkSum, &fileIdstr)

		err = db.rebuildOneIndex(instanceId, fileIdstr)
		if err != nil {
			break
		}

		log.Infof("rebuild one index ok, fileid %d offset %d instanceid %d cksum %d buffer size %d",
			fileId, nowOffset, instanceId, fileCkSum, len-UINT64SIZE)

		nowOffset += uint64(INT32SIZE) + uint64(len)
	}

	if needTruncate {
		log.Infof("truncate fileid %d offset %d filesize %d", fileId, nowOffset, fileLen)
		err = os.Truncate(filePath, int64(nowOffset))
		if err != nil {
			log.Errorf("truncate fail, file path %s truncate to length %d error:%v",
				filePath, nowOffset, err)
			return err
		}
	}
	return err
}

// 打开value file
func (logStore *LogStore) OpenFile(fileId int32) (*os.File, error) {
	filePath := fmt.Sprintf("%s/%d.f", logStore.path, fileId)
	return os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
}

// fileId在LevelDb中保存的结构是fileId(32 bytes + offset(64 bytes) + checksum(32 byte)
func (logStore *LogStore) DecodeFileId(fileIdStr string, fileId *int32, offset *uint64, cksum *uint32) {
	buffer := bytes.NewBufferString(fileIdStr).Bytes()

	util.DecodeInt32(buffer, 0, fileId)
	util.DecodeUint64(buffer, INT32SIZE, offset)
	util.DecodeUint32(buffer, INT32SIZE+UINT64SIZE, cksum)
}

func (logStore *LogStore) EncodeFileId(fileId int32, offset uint64, cksum uint32, fileIdStr *string) {
	buffer := make([]byte, INT32SIZE+UINT64SIZE+UINT32SIZE)
	util.EncodeInt32(buffer, 0, fileId)
	util.EncodeUint64(buffer, INT32SIZE, offset)
	util.EncodeUint32(buffer, INT32SIZE+UINT64SIZE, cksum)

	*fileIdStr = string(buffer)
}

func (logStore *LogStore) expandFile(file *os.File, fileSize *uint64) error {
	var err error
	var size int64
	size, err = file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	*fileSize = uint64(size)

	if *fileSize == 0 {
		maxLogFileSize := GetLogFileMaxSize()
		size, err = file.Seek(int64(maxLogFileSize-1), os.SEEK_SET)
		*fileSize = uint64(size)
		if err != nil {
			return err
		}

		file.Write([]byte{0})

		*fileSize = uint64(maxLogFileSize)
		file.Seek(0, os.SEEK_SET)
		logStore.nowFileOffset = 0
	}

	return nil
}

func (logStore *LogStore) getFileId(needWriteSize uint32, fileId *int32, offset *uint32) error {
	var err error
	if logStore.file == nil {
		err = fmt.Errorf("file already broken, file id %d", logStore.fileId)
		log.Error(err)
		return err
	}

	ret, err := logStore.file.Seek(int64(logStore.nowFileOffset), os.SEEK_SET)
	if err != nil {
		return err
	}
	*offset = uint32(ret)

	// 如果当前文件大小不够，需要新建一个vfile
	if uint64(*offset + needWriteSize) > logStore.nowFileSize {
		logStore.file.Close()
		logStore.file = nil

		err = logStore.IncreaseFileId()
		if err != nil {
			log.Errorf("new file increase fileid fail, now fileid %d", logStore.fileId)
			return err
		}

		logStore.file, err = logStore.OpenFile(logStore.fileId)
		if err != nil {
			log.Errorf("new file increase fileid fail, now fileid %d", logStore.fileId)
			return err
		}

		ret = -1
		ret, err = logStore.file.Seek(0, os.SEEK_END)
		if ret != 0 {
			log.Errorf("new file but file already exist,now file id %d exist filesize %d", logStore.fileId, ret)
			err = fmt.Errorf("increase file id success, but file exist, data wrong, file size %d", ret)
			return err
		}
		*offset = uint32(ret)

		err = logStore.expandFile(logStore.file, &logStore.nowFileSize)
		if err != nil {
			err = fmt.Errorf("new file expand fail, file id %d", logStore.fileId)
			log.Errorf("new file expand file fail, now file id %d", logStore.fileId)
			logStore.file.Close()
			logStore.file = nil
			return err
		}

		log.Infof("new file expand ok, file id %d filesize %d", logStore.fileId, logStore.nowFileSize)
	}

	*fileId = logStore.fileId
	return nil
}

// fileid 加1
func (logStore *LogStore) IncreaseFileId() error {
	fileId := logStore.fileId + 1
	buffer := make([]byte, INT32SIZE)
	util.EncodeInt32(buffer, 0, fileId)

	_, err := logStore.metaFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	// 更新meta file 内容
	n, err := logStore.metaFile.Write(buffer)
	if err != nil {
		return err
	}
	if n != INT32SIZE {
		return fmt.Errorf("write len %d not equal to %d", n, INT32SIZE)
	}
	ckSum := util.Crc32(0, buffer, 0)
	buffer = make([]byte, UINT32SIZE)
	util.EncodeUint32(buffer, 0, ckSum)
	n, err = logStore.metaFile.Write(buffer)
	if err != nil {
		return err
	}

	if n != UINT32SIZE {
		return fmt.Errorf("write len %d not equal to %d", n, UINT32SIZE)
	}

	logStore.metaFile.Sync()

	logStore.fileId += 1
	return nil
}
