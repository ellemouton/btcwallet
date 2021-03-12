package chain

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/cache/lru"
)

// blockCache holds a LRU cache along with a getBlockImpl function to be used
// to fetch new blocks if they are not found in the cache.
type blockCache struct {
	cache *lru.Cache
}

// getBlock checks the blockCache LRU cache to see if the block for the
// associated hash is already there and returns it if it is. If it is not then
// the blockCache's getBlockImpl function is used to fetch the block.
// The new block is then stored in the cache and the LFU block is evicted if
// the cache is at maximum capacity.
func (b *blockCache) getBlock(hash *chainhash.Hash,
	fetchNewBlock func(hash *chainhash.Hash) (*wire.MsgBlock,
		error)) (*wire.MsgBlock, error) {

	var block *wire.MsgBlock

	// Check if the block corresponding to the given hash is already
	// stored in the blockCache and return it if it is.
	cacheBlock, err := b.cache.Get(*hash)
	if err != nil && err != cache.ErrElementNotFound {
		return nil, err
	}
	if cacheBlock != nil {
		return cacheBlock.(*cache.CacheableBlock).MsgBlock(), nil
	}

	// Fetch the block from the chain backends.
	block, err = fetchNewBlock(hash)
	if err != nil {
		return nil, err
	}

	// Add the new block to blockCache. If the cache is at its maximum
	// capacity then the LFU item will be evicted in favour of this new
	// block.
	_, err = b.cache.Put(*hash, &cache.CacheableBlock{
		Block: btcutil.NewBlock(block),
	})
	if err != nil {
		return nil, err
	}

	return block, nil
}
