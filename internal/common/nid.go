package common

// Nid is the logical node identifier. Layout: the high 16 bits encode the
// owning bucket's BucketId; the low 48 bits are a bucket-local NodeId.
type Nid uint64

// Nid layout constants.
const (
	NidBucketShift = 48
	NidNodeMask    = (uint64(1) << NidBucketShift) - 1 // 0x0000FFFFFFFFFFFF
	MaxBucketId    = (uint64(1) << 16) - 1             // 65535
)

// BucketId is the 16-bit identifier of a top-level bucket. It occupies the high
// 16 bits of every Nid owned by the bucket. BucketId 0 is reserved (it would
// alias Nid 0, the "unassigned" sentinel), so allocation starts at 1.
type BucketId uint16

// MakeNid composes a globally-unique Nid from a bucket id and a bucket-local
// node id. The node id is masked to the low 48 bits.
func MakeNid(bucket BucketId, node uint64) Nid {
	return Nid(uint64(bucket)<<NidBucketShift | (node & NidNodeMask))
}

// BucketOf returns the high 16 bits of the Nid: the owning bucket's id.
func (n Nid) BucketOf() BucketId { return BucketId(uint64(n) >> NidBucketShift) }

// NodeId returns the low 48 bits of the Nid: the bucket-local node id.
func (n Nid) NodeId() uint64 { return uint64(n) & NidNodeMask }
