package checkpoint

import (
	"os"
	"sync"

	LOG "github.com/vinllen/log4go"
	"io/ioutil"
	"fmt"
	"encoding/json"
	"path/filepath"
	"reflect"
	"bytes"
	"strings"
)

// marshal in json
type FileWriter struct {
	dir         string
	fileHandler *sync.Map // file name -> fd
	fileLock    *sync.Map // file name -> lock
}

func NewFileWriter(dir string) *FileWriter {
	// create dir if not exist
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			// create dir
			if err = os.Mkdir(dir, 0755); err != nil {
				LOG.Crashf("create dir[%v] failed[%v]", dir, err)
				return nil
			}
		} else {
			LOG.Crashf("stat dir[%v] failed[%v]", dir, err)
			return nil
		}
	}

	return &FileWriter{
		dir:         dir,
		fileHandler: new(sync.Map),
		fileLock:    new(sync.Map),
	}
}

// find current status
func (fw *FileWriter) FindStatus() (string, error) {
	// lock file
	fw.lockFile(CheckpointStatusTable)
	defer fw.unlockFile(CheckpointStatusTable)

	file := fmt.Sprintf("%s/%s", fw.dir, CheckpointStatusTable)
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return CheckpointStatusValueEmpty, nil
		}
	}

	jsonFile, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return "", err
	}

	var ret Status
	if err := json.Unmarshal(byteValue, &ret); err != nil {
		return "", err
	}

	return ret.Value, nil
}

// update status
func (fw *FileWriter) UpdateStatus(status string) error {
	// lock file
	fw.lockFile(CheckpointStatusTable)
	defer fw.unlockFile(CheckpointStatusTable)

	file := fmt.Sprintf("%s/%s", fw.dir, CheckpointStatusTable)
	input := &Status{
		Key:   CheckpointStatusKey,
		Value: status,
	}

	val, err := json.Marshal(input)
	if err != nil {
		return nil
	}

	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(val)
	return err
}

// extract all checkpoint
func (fw *FileWriter) ExtractCheckpoint() (map[string]map[string]*Checkpoint, error) {
	ckptMap := make(map[string]map[string]*Checkpoint)
	// fileList isn't include directory
	var fileList []string
	err := filepath.Walk(fw.dir, func(path string, info os.FileInfo, err error) error {
		if path != fw.dir {
			pathList := strings.Split(path, "/")
			fileList = append(fileList, pathList[len(pathList)-1])
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	for _, file := range fileList {
		if FilterCkptCollection(file) {
			continue
		}

		innerMap, err := fw.ExtractSingleCheckpoint(file)
		if err != nil {
			return nil, err
		}
		ckptMap[file] = innerMap
	}

	return ckptMap, nil
}

// extract single checkpoint
func (fw *FileWriter) ExtractSingleCheckpoint(table string) (map[string]*Checkpoint, error) {
	fw.lockFile(table)
	defer fw.unlockFile(table)

	file := fmt.Sprintf("%s/%s", fw.dir, table)
	jsonFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()

	data, err := fw.readJsonList(jsonFile)
	if err != nil {
		return nil, err
	}

	innerMap := make(map[string]*Checkpoint)
	for _, ele := range data {
		innerMap[ele.ShardId] = ele
	}

	return innerMap, nil
}

// insert checkpoint
func (fw *FileWriter) Insert(ckpt *Checkpoint, table string) error {
	fw.lockFile(table)
	defer fw.unlockFile(table)

	file := fmt.Sprintf("%s/%s", fw.dir, table)
	jsonFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	LOG.Debug("file[%s] insert data: %v", file, *ckpt)

	return fw.writeJsonList(jsonFile, []*Checkpoint{ckpt})
}

// update checkpoint
func (fw *FileWriter) Update(shardId string, ckpt *Checkpoint, table string) error {
	fw.lockFile(table)
	defer fw.unlockFile(table)

	file := fmt.Sprintf("%s/%s", fw.dir, table)
	jsonFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	data, err := fw.readJsonList(jsonFile)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	match := false
	for i := range data {
		if data[i].ShardId == shardId {
			match = true
			data[i] = ckpt
			break
		}
	}
	if !match {
		return fmt.Errorf("shardId[%v] not exists", shardId)
	}

	// truncate file
	jsonFile.Truncate(0)
	jsonFile.Seek(0, 0)

	// write
	return fw.writeJsonList(jsonFile, data)
}

// update with set
func (fw *FileWriter) UpdateWithSet(shardId string, input map[string]interface{}, table string) error {
	fw.lockFile(table)
	defer fw.unlockFile(table)

	file := fmt.Sprintf("%s/%s", fw.dir, table)
	jsonFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	data, err := fw.readJsonList(jsonFile)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	match := false
	for i := range data {
		if data[i].ShardId == shardId {
			match = true
			// set partial
			for key, val := range input {
				field := reflect.ValueOf(data[i]).Elem().FieldByName(key)
				switch field.Kind() {
				case reflect.String:
					v, _ := val.(string)
					field.SetString(v)
				case reflect.Invalid:
					printData, _ := json.Marshal(data[i])
					return fmt.Errorf("invalid field[%v], current checkpoint[%s]", key, printData)
				}
			}

			break
		}
	}
	if !match {
		return fmt.Errorf("shardId[%v] not exists", shardId)
	}

	// truncate file
	jsonFile.Truncate(0)
	jsonFile.Seek(0, 0)

	// write
	return fw.writeJsonList(jsonFile, data)
}

// query
func (fw *FileWriter) Query(shardId string, table string) (*Checkpoint, error) {
	fw.lockFile(table)
	defer fw.unlockFile(table)

	file := fmt.Sprintf("%s/%s", fw.dir, table)
	jsonFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()

	data, err := fw.readJsonList(jsonFile)
	if err != nil {
		return nil, err
	}

	for _, ele := range data {
		if ele.ShardId == shardId {
			return ele, nil
		}
	}

	return nil, fmt.Errorf("not found")
}

// drop
func (fw *FileWriter) DropAll() error {
	var fileList []string
	err := filepath.Walk(fw.dir, func(path string, info os.FileInfo, err error) error {
		if path != fw.dir {
			fileList = append(fileList, path)
		}
		return nil
	})

	if err != nil {
		return err
	}

	LOG.Info("drop file list: %v", fileList)

	for _, file := range fileList {
		fw.lockFile(file)
		if err := os.Remove(file); err != nil {
			fw.unlockFile(file)
			return err
		}
		fw.unlockFile(file)
	}

	return nil
}

func (fw *FileWriter) lockFile(table string) {
	val, ok := fw.fileLock.Load(CheckpointStatusTable)
	if !ok {
		val = new(sync.Mutex)
		fw.fileLock.Store(CheckpointStatusTable, val)
	}

	lock := val.(*sync.Mutex)
	lock.Lock()
}

func (fw *FileWriter) unlockFile(table string) {
	val, ok := fw.fileLock.Load(CheckpointStatusTable)
	if !ok {
		val = new(sync.Mutex)
		fw.fileLock.Store(CheckpointStatusTable, val)
	}

	lock := val.(*sync.Mutex)
	lock.Unlock()
}

func (fw *FileWriter) readJsonList(f *os.File) ([]*Checkpoint, error) {
	byteValue, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	byteList := bytes.Split(byteValue, []byte{10})
	ret := make([]*Checkpoint, 0, len(byteList))
	for i := 0; i < len(byteList)-1; i++ {
		var ele Checkpoint
		if err := json.Unmarshal(byteList[i], &ele); err != nil {
			return nil, err
		}
		ret = append(ret, &ele)
	}

	return ret, nil
}

func (fw *FileWriter) writeJsonList(f *os.File, input []*Checkpoint) error {
	for _, single := range input {
		val, err := json.Marshal(single)
		if err != nil {
			return nil
		}

		val = append(val, byte(10)) // suffix
		if _, err := f.Write(val); err != nil {
			return err
		}
	}
	return nil
}
