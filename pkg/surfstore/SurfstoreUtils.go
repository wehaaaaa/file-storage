package surfstore

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// load local files

	baseDir, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("read base directory err:", err)
	}
	localMap := make(map[string]*FileMetaData)
	localBlocksMap := make(map[string](map[string]*Block))
	for _, file := range baseDir {
		if strings.HasSuffix(file.Name(), "index.txt") {
			continue
		}
		data, blockMap := CreateFileMetaData(file, client.BlockSize, client.BaseDir)
		localMap[file.Name()] = data
		localBlocksMap[file.Name()] = blockMap
	}
	// load index files
	indexMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("load index txt err:", err)
	}
	// create empty index file
	if len(indexMap) == 0 {
		outputMetaPath := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
		_, err := os.Create(outputMetaPath)
		if err != nil {
			log.Fatal("error during create empty index txt")
		}
		//indexMap = localMap
	}
	log.Println(localMap)
	// sync with blockStore and metaStore
	var counter int
	counter = 0
	for counter < 5 {
		// get remote file
		remoteMap := make(map[string]*FileMetaData)
		err = client.GetFileInfoMap(&remoteMap)
		if err != nil {
			log.Println("get file info map failed")
		}
		err := RemoteFileSync(localMap, indexMap, remoteMap, client, localBlocksMap)
		log.Println("remoteMap:", remoteMap)
		if err == nil {
			err = client.GetFileInfoMap(&remoteMap)
			if err != nil {
				log.Println("sb")
			}
			log.Println("remoteMap:", remoteMap)
			break
		}
		counter += 1

	}

	for name, data := range indexMap {
		if _, ok := localMap[name]; !ok {
			if len(data.BlockHashList) == 1 && data.BlockHashList[0] == "0" {
				continue
			}
			tombstone := []string{"0"}
			data.BlockHashList = tombstone
			data.Version += 1
			err := UpdateMetaStore(data, client)
			if err != nil {
				log.Println("update deleted file failed")
			}
		}
	}
	log.Println("adsdsadsads")
	if len(indexMap) == 0 && len(localMap) != 0 {
		indexMap = localMap
	}
	log.Println("abcdefgds")
	// try again
	remoteMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteMap)

	if err != nil {
		log.Println("sb")
	}
	err = WriteMetaFile(remoteMap, client.BaseDir)
	log.Println(indexMap)
	if err != nil {
		log.Println("write index.txt failed")
	}

}

// create fileMetaData
func CreateFileMetaData(file os.FileInfo, size int, baseDir string) (*FileMetaData, map[string]*Block) {
	path, _ := filepath.Abs(ConcatPath(baseDir, strings.TrimSpace(file.Name())))
	f, err := os.Open(path)
	if err != nil {
		log.Println("cannot open file", file.Name())
	}
	blockHashList := []string{}
	fileBlockMap := make(map[string]*Block)

	for {
		buf := make([]byte, size)
		block := Block{}
		b, err := f.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("read file err:", err)
		}
		if b == 0 {
			break
		}
		blockHashList = append(blockHashList, GetBlockHashString(buf[:b]))
		log.Println(buf[:b])
		block.BlockData = buf[:b]
		block.BlockSize = int32(size)
		fileBlockMap[GetBlockHashString(buf[:b])] = &block
	}
	return &FileMetaData{Filename: file.Name(), Version: 0, BlockHashList: blockHashList}, fileBlockMap
}

// sync with server
func RemoteFileSync(localMap map[string]*FileMetaData, indexMap map[string]*FileMetaData, remoteMap map[string]*FileMetaData, client RPCClient, localBlocksMap map[string](map[string]*Block)) error {
	err := client.GetFileInfoMap(&remoteMap)
	if err != nil {
		log.Println("get file info map err:", err)
		//return err
	}
	tombstone := []string{"0"}
	// check tombstone hashlist in remote, then delete it from local
	for name, data := range remoteMap {
		if len(data.BlockHashList) == 1 && data.BlockHashList[0] == "0" {
			if val, ok := indexMap[name]; ok {
				if val.Version == data.Version {
					continue
				}
			}
			for n := range localMap {
				if n == name {
					delete(localMap, name)
					delete(localBlocksMap, name)
				}
			}
			for n := range indexMap {
				if n == name {
					indexMap[name].BlockHashList = tombstone
					indexMap[name].Version = data.Version
				}
			}

			err := os.Remove(ConcatPath(client.BaseDir, name))
			if err != nil {
				log.Println("delete file failed")
			}

		}
	}
	// compare remote verison to index version
	for name, data := range remoteMap {
		if !IsTombStone(data) {
			if val, ok := indexMap[name]; ok {
				if data.Version > val.Version {
					DownloadBlocks(data, client, localBlocksMap)
					val.Version = data.Version
					val.BlockHashList = data.BlockHashList
					localMap[name] = data
				}
			} else {
				DownloadBlocks(data, client, localBlocksMap)
				indexMap[name] = data
				localMap[name] = data
			}
		}
	}
	// compare local verison to index version
	for name, data := range localMap {
		if data.Version == 0 {
			if val, ok := indexMap[name]; ok {
				log.Println(!reflect.DeepEqual(data.BlockHashList, val.BlockHashList))
				if !reflect.DeepEqual(data.BlockHashList, val.BlockHashList) {
					if data.Version < val.Version {
						data.Version = val.Version
					}
					data.Version += 1
					log.Println(data.Version)
					val = data
					log.Println("debuglalala: ", val.Version)
					err := UpdateBlockStore(data, client, localBlocksMap)
					if err != nil {
						log.Println("local vs index1")
						return err
					}
					err = UpdateMetaStore(data, client)
					if err != nil {
						log.Println("local vs index1")
						return err
					}
				}
			} else {
				data.Version = 1
				val = data
				err := UpdateBlockStore(data, client, localBlocksMap)
				if err != nil {
					log.Println("local vs index2")
					return err
				}
				err = UpdateMetaStore(data, client)
				if err != nil {
					log.Println("local vs index2")
					return err
				}
			}
		}
	}
	return nil
}

// download blocks ,write to local files
func DownloadBlocks(data *FileMetaData, client RPCClient, localBlocksMap map[string](map[string]*Block)) {
	targetPath := ConcatPath(client.BaseDir, data.Filename)
	tempPath := ConcatPath(client.BaseDir, "temp"+data.Filename)
	tempF, err := os.Create(tempPath)
	if err != nil {
		log.Println("create temp file failed")
	}
	defer tempF.Close()
	for _, hash := range data.BlockHashList {
		block := Block{}
		var blockStoreAddr string
		err := client.GetBlockStoreAddr(&blockStoreAddr)
		if err != nil {
			log.Println("get blockStoreAddr failed")
		}
		err = client.GetBlock(hash, blockStoreAddr, &block)
		if err != nil {
			log.Println("get block failed")
		}
		_, err = tempF.Write(block.BlockData)
		if err != nil {
			log.Println("write block data failed")
		}
		log.Println(&block.BlockData)
		log.Println(localBlocksMap)
		log.Println(data.Filename)
		if val, ok := localBlocksMap[data.Filename]; ok {
			val[hash] = &block
		}
	}
	os.Remove(targetPath)
	os.Rename(tempPath, targetPath)

}

func IsTombStone(data *FileMetaData) bool {
	if len(data.BlockHashList) == 1 && data.BlockHashList[0] == "0" {
		return true
	} else {
		return false
	}
}

func UpdateBlockStore(data *FileMetaData, client RPCClient, localBlocksMap map[string](map[string]*Block)) error {
	var blockStoreAddr string
	err := client.GetBlockStoreAddr(&blockStoreAddr)

	if err != nil {
		log.Println("get blockStoreAddr failed")
		return err
	}
	//log.Println(blockStoreAddr)
	for _, hash := range data.BlockHashList {
		block := localBlocksMap[data.Filename][hash]
		var succ bool
		log.Println("blockstoreaddr:", blockStoreAddr)
		err := client.PutBlock(block, blockStoreAddr, &succ)
		if err != nil {
			log.Println("put block failed")
			log.Println(err)
			return err
		}
	}
	return nil
}

func UpdateMetaStore(data *FileMetaData, client RPCClient) error {
	var latestVersion int32
	err := client.UpdateFile(data, &latestVersion)
	log.Println("update!!!!!!")
	log.Println(err)
	if err != nil {
		log.Println("update file failed")
		return err
	}
	return nil
}
