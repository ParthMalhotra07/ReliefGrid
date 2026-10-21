// Package ring maps entity keys (req:{id}, res:{id}) to the three logical
// shards using a fixed xxhash64 consistent-hash ring with a published
// ringVersion and checksum.
package ring
