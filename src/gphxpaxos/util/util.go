package util

import (
	"time"
	"math/rand"
	"os"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"strconv"
	"bytes"
	"reflect"
	"errors"
	"path/filepath"

	"io/ioutil"
	"math"
)

func Rand(up int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(up)
}

//--------------------------------文件操作--------------------------------//
func Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func DeleteDir(path string) error {
	return os.RemoveAll(path)
}

func IterDir(path string) ([]string, error) {
	var allFile []string
	finfos, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, x := range finfos {
		realPath := path + string(filepath.Separator) + x.Name()
		if x.IsDir() {
			subDirFiles, err := IterDir(realPath)
			if err != nil {
				return nil, err
			}
			allFile = append(allFile, subDirFiles...)
		} else {
			allFile = append(allFile, realPath)
		}
	}
	return allFile, nil
}

//---------------------------------[]byte操作-------------------------------------//

func AppendBytes(inputs ...[]byte) [] byte {
	return bytes.Join(inputs, []byte(""))
}

func CopyBytes(src []byte) [] byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// ---------------------------[]byte和类型的转换-----------------------------//
func DecodeUint64(buffer []byte, offset int, ret *uint64) {
	*ret = binary.LittleEndian.Uint64(buffer[offset:])
}

func EncodeUint64(buffer []byte, offset int, ret uint64) {
	binary.LittleEndian.PutUint64(buffer[offset:], ret)
}

func DecodeInt32(buffer []byte, offset int, ret *int32) {
	tmp := binary.LittleEndian.Uint32(buffer[offset:])
	*ret = int32(tmp)
}

func EncodeInt32(buffer []byte, offset int, ret int32) {
	binary.LittleEndian.PutUint32(buffer[offset:], uint32(ret))
}

func DecodeUint32(buffer []byte, offset int, ret *uint32) {
	*ret = binary.LittleEndian.Uint32(buffer[offset:])
}

func EncodeUint32(buffer []byte, offset int, ret uint32) {
	binary.LittleEndian.PutUint32(buffer[offset:], ret)
}

func DecodeUint16(buffer []byte, offset int, ret *uint16) {
	*ret = binary.LittleEndian.Uint16(buffer[offset:])
}

func EncodeUint16(buffer []byte, offset int, ret uint16) {
	binary.LittleEndian.PutUint16(buffer[offset:], ret)
}

//----------------------------------加密和编码------------------------------------//
func Crc32(crc uint32, value []byte, skiplen int) uint32 { // crc32编码
	vlen := len(value)
	data := value[:vlen-skiplen]
	return crc32.Update(crc, crc32.IEEETable, []byte(data))
}

// ---------------------------------时间相关操作---------------------------------//
func NowTimeMs() uint64 {
	return uint64(time.Now().UnixNano() / 1000000)
}

func SleepMs(ms uint64) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func Inet_addr(ipaddr string) uint32 {
	var (
		ip                 = strings.Split(ipaddr, ".")
		ip1, ip2, ip3, ip4 uint64
		ret                uint32
	)
	ip1, _ = strconv.ParseUint(ip[0], 10, 8)
	ip2, _ = strconv.ParseUint(ip[1], 10, 8)
	ip3, _ = strconv.ParseUint(ip[2], 10, 8)
	ip4, _ = strconv.ParseUint(ip[3], 10, 8)
	ret = uint32(ip4)<<24 + uint32(ip3)<<16 + uint32(ip2)<<8 + uint32(ip1)
	return ret
}

type TimeStat struct {
	mTime uint64
}

func NewTimeStat() *TimeStat {
	return &TimeStat{mTime: NowTimeMs()}
}

func (t *TimeStat) Point() uint64 {
	nowTime := NowTimeMs()

	passTime := uint64(0)

	if nowTime > t.mTime {
		passTime = nowTime - t.mTime
	}

	t.mTime = passTime

	return passTime
}

// ---------------------------------对象相关操作---------------------------------//

// dst should be a pointer to struct, src should be a struct
func CopyStruct(dst interface{}, src interface{}) (err error) {
	dstValue := reflect.ValueOf(dst)
	if dstValue.Kind() != reflect.Ptr {
		err = errors.New("dst isn't a pointer to struct")
		return
	}
	dstElem := dstValue.Elem()
	if dstElem.Kind() != reflect.Struct {
		err = errors.New("pointer doesn't point to struct")
		return
	}

	srcValue := reflect.ValueOf(src)
	srcType := reflect.TypeOf(src)
	if srcType.Kind() != reflect.Struct {
		err = errors.New("src isn't struct")
		return
	}

	for i := 0; i < srcType.NumField(); i++ {
		sf := srcType.Field(i)
		sv := srcValue.FieldByName(sf.Name)
		// make sure the value which in dst is valid and can set
		if dv := dstElem.FieldByName(sf.Name); dv.IsValid() && dv.CanSet() {
			dv.Set(sv)
		}
	}
	return
}

// ---------------------------------业务相关---------------------------------//

func GenGid(instanceId uint64) uint64 {
	rand := uint64(Rand(math.MaxUint32))
	return rand ^ instanceId + rand
}