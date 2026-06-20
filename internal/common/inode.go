package common

// Inode represents an internal entry inside a B+tree node: either a leaf
// key/value pair or a branch separator pointing at a child Nid.
type Inode struct {
	flags uint32
	nid   Nid
	key   []byte
	value []byte
}

// NewInode constructs an Inode.
func NewInode(flags uint32, nid Nid, key, value []byte) Inode {
	return Inode{flags: flags, nid: nid, key: key, value: value}
}

// Inodes is a slice of Inode entries.
type Inodes []Inode

func (in *Inode) Flags() uint32         { return in.flags }
func (in *Inode) SetFlags(flags uint32) { in.flags = flags }
func (in *Inode) Nid() Nid              { return in.nid }
func (in *Inode) SetNid(id Nid)         { in.nid = id }
func (in *Inode) Key() []byte           { return in.key }
func (in *Inode) SetKey(key []byte)     { in.key = key }
func (in *Inode) Value() []byte         { return in.value }
func (in *Inode) SetValue(value []byte) { in.value = value }
