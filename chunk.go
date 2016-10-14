//
// object storage types: chunk and replica
//
package surge

import (
	"fmt"
	"time"
)

//
// type Chunk
//
type Chunk struct {
	cid     int64
	sid     int64 // short id
	gateway RunnerInterface
	crtime  time.Time // creation time
	sizeb   int64     // bytes
}

func NewChunk(gwy RunnerInterface, sizebytes int) *Chunk {
	uqid, printid := uqrandom64(gwy.GetID())
	return &Chunk{cid: uqid, sid: printid, gateway: gwy, crtime: Now, sizeb: int64(sizebytes)}
}

func (chunk *Chunk) String() string {
	return fmt.Sprintf("c#%d", chunk.sid)
}

//
// type PutReplica
//
type PutReplica struct {
	chunk  *Chunk
	crtime time.Time // creation time
	num    int       // 1 .. configStorage.numReplicas
}

func (replica *PutReplica) String() string {
	return fmt.Sprintf("c#%d(%d)", replica.chunk.sid, replica.num)
}

func NewPutReplica(c *Chunk, n int) *PutReplica {
	return &PutReplica{chunk: c, crtime: Now, num: n}
}
