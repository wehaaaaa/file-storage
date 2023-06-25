package surfstore

import (
	context "context"
	"errors"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
	mu sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := fileMetaData.Filename
	_, exist := m.FileMetaMap[name]
	if !exist {
		m.FileMetaMap[name] = fileMetaData
	} else {
		if fileMetaData.Version-m.FileMetaMap[name].Version == 1 {
			m.FileMetaMap[name] = fileMetaData
		} else if fileMetaData.Version == -1 {
			m.FileMetaMap[name] = fileMetaData
		} else {
			return &Version{Version: -1}, errors.New("new verison is not 1 greater")
		}
	}
	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
