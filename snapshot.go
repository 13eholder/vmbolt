package vmbolt

import (
	"encoding/binary"
	"fmt"
	"io"
)

// BMSP ("Bbolt Memory SnaPshot") on-disk format, v1.
//
// This is the snapshot format used by Tx.WriteTo/Copy/CopyFile and DB.Restore.
// It is deliberately NOT vmbolt's on-disk page format: the engine is page-less,
// so the snapshot is a simple, streaming, fully-ordered KV dump.
//
// All integers are little-endian. Buckets are emitted in sorted name order (via
// Tx.Cursor) and keys within a bucket in sorted order (via bucket.ForEach), so
// the output is deterministic and round-trips losslessly with Restore. Sentinel
// zero length fields terminate lists, so the writer never has to pre-count.
//
//	layout:
//	  header (16 bytes):
//	    magic    [4]byte  "BMSP"
//	    version  uint32   = snapshotVersion
//	    reserved uint32   = 0
//	    reserved uint32   = 0
//	  repeated bucket record (terminated by nameLen == 0):
//	    nameLen  uint32   (>0)
//	    name     [nameLen]byte
//	    repeated kv pair (terminated by keyLen == 0):
//	      keyLen uint32   (>0)
//	      key    [keyLen]byte
//	      valLen uint64
//	      val    [valLen]byte
//	  end-of-buckets: nameLen uint32 == 0
const (
	snapshotMagic   = "BMSP"
	snapshotVersion = uint32(1)
)

var snapshotMagicBytes = []byte(snapshotMagic) // exactly 4 bytes

// byteCounter wraps a writer and counts the bytes written.
type byteCounter struct {
	w io.Writer
	n int64
}

func (c *byteCounter) Write(p []byte) (int, error) {
	nn, err := c.w.Write(p)
	c.n += int64(nn)
	return nn, err
}

// serializeSnapshot writes the whole database to w as a BMSP snapshot.
func (tx *Tx) serializeSnapshot(w io.Writer) (int64, error) {
	cw := &byteCounter{w: w}

	// Header.
	hdr := make([]byte, 16)
	copy(hdr[0:4], snapshotMagicBytes)
	binary.LittleEndian.PutUint32(hdr[4:8], snapshotVersion)
	binary.LittleEndian.PutUint32(hdr[8:12], 0)
	binary.LittleEndian.PutUint32(hdr[12:16], 0)
	if _, err := cw.Write(hdr); err != nil {
		return cw.n, err
	}

	var buf4 [4]byte
	var buf8 [8]byte

	// Buckets, sorted by name via the directory cursor.
	cur := tx.Cursor()
	for name, _ := cur.First(); name != nil; name, _ = cur.Next() {
		b := tx.Bucket(name)
		if b == nil {
			continue
		}
		// name
		binary.LittleEndian.PutUint32(buf4[:], uint32(len(name)))
		if _, err := cw.Write(buf4[:]); err != nil {
			return cw.n, err
		}
		if _, err := cw.Write(name); err != nil {
			return cw.n, err
		}

		// KV pairs (sorted).
		if err := b.ForEach(func(k, v []byte) error {
			binary.LittleEndian.PutUint32(buf4[:], uint32(len(k)))
			if _, err := cw.Write(buf4[:]); err != nil {
				return err
			}
			if _, err := cw.Write(k); err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(buf8[:], uint64(len(v)))
			if _, err := cw.Write(buf8[:]); err != nil {
				return err
			}
			if _, err := cw.Write(v); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return cw.n, err
		}

		// End-of-bucket marker: keyLen == 0.
		binary.LittleEndian.PutUint32(buf4[:], 0)
		if _, err := cw.Write(buf4[:]); err != nil {
			return cw.n, err
		}
	}

	// End-of-buckets marker: nameLen == 0.
	binary.LittleEndian.PutUint32(buf4[:], 0)
	if _, err := cw.Write(buf4[:]); err != nil {
		return cw.n, err
	}
	return cw.n, nil
}

// Restore reads a BMSP snapshot from r and rebuilds the database in memory.
// It must be called on a fresh/empty DB (e.g. right after Open). No durability
// is added: the snapshot is a bulk rehydration, not incremental persistence.
func (db *DB) Restore(r io.Reader) error {
	var hdr [16]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return fmt.Errorf("vmbolt: snapshot header: %w", err)
	}
	if string(hdr[0:4]) != snapshotMagic {
		return fmt.Errorf("vmbolt: not a BMSP snapshot (magic %q)", string(hdr[0:4]))
	}
	if v := binary.LittleEndian.Uint32(hdr[4:8]); v != snapshotVersion {
		return fmt.Errorf("vmbolt: unsupported snapshot version %d", v)
	}
	// hdr[8:16] are reserved; ignored.

	return db.Update(func(tx *Tx) error {
		var buf4 [4]byte
		var buf8 [8]byte
		for {
			if _, err := io.ReadFull(r, buf4[:]); err != nil {
				return fmt.Errorf("vmbolt: snapshot bucket nameLen: %w", err)
			}
			nameLen := binary.LittleEndian.Uint32(buf4[:])
			if nameLen == 0 {
				return nil // end of buckets
			}
			name := make([]byte, nameLen)
			if _, err := io.ReadFull(r, name); err != nil {
				return fmt.Errorf("vmbolt: snapshot bucket name: %w", err)
			}
			b, err := tx.CreateBucket(name)
			if err != nil {
				return fmt.Errorf("vmbolt: restore bucket %q: %w", name, err)
			}
			b.FillPercent = 0.9 // keys arrive pre-sorted from the snapshot

			for {
				if _, err := io.ReadFull(r, buf4[:]); err != nil {
					return fmt.Errorf("vmbolt: snapshot keyLen: %w", err)
				}
				keyLen := binary.LittleEndian.Uint32(buf4[:])
				if keyLen == 0 {
					break // end of this bucket's pairs
				}
				key := make([]byte, keyLen)
				if _, err := io.ReadFull(r, key); err != nil {
					return fmt.Errorf("vmbolt: snapshot key: %w", err)
				}
				if _, err := io.ReadFull(r, buf8[:]); err != nil {
					return fmt.Errorf("vmbolt: snapshot valLen: %w", err)
				}
				val := make([]byte, binary.LittleEndian.Uint64(buf8[:]))
				if _, err := io.ReadFull(r, val); err != nil {
					return fmt.Errorf("vmbolt: snapshot val: %w", err)
				}
				if err := b.Put(key, val); err != nil {
					return fmt.Errorf("vmbolt: restore put %q: %w", key, err)
				}
			}
		}
	})
}
