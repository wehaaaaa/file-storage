package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	mu sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return &Block{BlockData: bs.BlockMap[blockHash.Hash].BlockData,
		BlockSize: bs.BlockMap[blockHash.Hash].BlockSize}, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	hash := sha256.New()
	hash.Write(block.BlockData)
	hashBytes := hash.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)
	bs.BlockMap[hashString] = block
	return &Success{Flag: true}, nil

}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	var blockHashesOut *[]string
	for _, hash := range blockHashesIn.Hashes {
		_, check := bs.BlockMap[hash]
		if check {
			*blockHashesOut = append(*blockHashesOut, hash)
		}
	}
	return &BlockHashes{Hashes: *blockHashesOut}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
