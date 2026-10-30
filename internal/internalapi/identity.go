package internalapi

// Response identity (issue #46). A storage node sets both headers on every
// answer of its data routes; the coordinator accepts a forwarded answer
// only if they name the server it dialled. An address alone is not an
// identity: a freed port or a reused pod IP can be answered by another
// process.
const (
	// HeaderServerID is the answering replica's Raft ServerID,
	// podName/incarnation.
	HeaderServerID = "X-ReliefGrid-Server-Id"
	// HeaderShard is the answering replica's shard, in decimal.
	HeaderShard = "X-ReliefGrid-Shard"
)
