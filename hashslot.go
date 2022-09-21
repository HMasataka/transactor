package transactor

import (
	"hash/crc32"
)

func GetShardingIndex(shardKey []byte, maxSlot uint32) int {
	return int(crc32.ChecksumIEEE(shardKey) % maxSlot)
}
