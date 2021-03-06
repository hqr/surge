// Package surge provides a framework for discrete event simulation, as well as
// a number of models for Unsolicited and Reservation Group based Edge-driven
// load balancing. Targeted modeling area includes large and super-large storage
// clusters with multiple access points (referred to as "gateways") and multiple
// storage targets (referred to as "servers").
package surge

import (
	"fmt"
	"sync"
	"time"
)

//=====================================================================
// RateBucketInterface
//=====================================================================
// RateBucketInterface provides a common interface for a variety of leaky-
// bucket type schemes and lists all the methods common for the simple
// RateBucket, RateBucketAIMD and possible TBD rate bucket implementations.
//
// For instance, prior to transmitting a packet, a user
// will execute the use(size) method, where size is the corresponding
// number of bits the user is intending to send.
// If the underlying bucket does not contain enough bit "units",
// it'll return false indicating that the send operation must be postponed.
//
type RateBucketInterface interface {
	use(units int64) bool
	setrate(newrate int64)

	getrate() int64
	above(units int64) bool
	below(units int64) bool

	String() string
}

//=====================================================================
// RateBucket
//=====================================================================
// RateBucket represents the most basic leaky bucket type where the "units"
// (for instance, bits of the network packets) get recharged at the given
// RateBucket.rate up to the specified RateBucket.maxval.
// RateBucket.value contains currently available number of units,
// as the implies.
type RateBucket struct {
	maxval    int64
	rate      int64 // units/sec
	value     int64
	timestamp time.Time
}

func NewRateBucket(m int64, r int64, v int64) *RateBucket {
	rb := &RateBucket{maxval: m, rate: r, value: v, timestamp: TimeNil}
	rb.setrate(rb.rate)
	return rb
}

// private
func (rb *RateBucket) __addtime() {
	if !Now.After(rb.timestamp) {
		return
	}
	if rb.value == rb.maxval || rb.rate == 0 {
		rb.timestamp = TimeNil // STOP the ratebucket timer
		return
	}

	d := Now.Sub(rb.timestamp)
	rb.value += rb.rate * int64(d) / int64(time.Second)
	if rb.value > rb.maxval {
		rb.value = rb.maxval
	}
	rb.timestamp = Now
}

func (rb *RateBucket) use(units int64) bool {
	rb.__addtime()
	if units > rb.value {
		return false
	}
	if rb.value == rb.maxval && rb.rate > 0 {
		rb.timestamp = Now // START the ratebucket timer
	}
	rb.value -= units
	return true
}

func (rb *RateBucket) setrate(newrate int64) {
	rb.__addtime()
	rb.rate = newrate
	if rb.value < rb.maxval && rb.rate > 0 {
		rb.timestamp = Now // START the ratebucket timer
	}
}

func (rb *RateBucket) getrate() int64 {
	return rb.rate
}

func (rb *RateBucket) above(units int64) bool {
	rb.__addtime()
	return units <= rb.value
}

func (rb *RateBucket) below(units int64) bool {
	rb.__addtime()
	return units > rb.value
}

func (rb *RateBucket) String() string {
	return fmt.Sprintf("%+v", *rb)
}

//=====================================================================
// RateBucketProtected
//=====================================================================
type RateBucketProtected struct {
	RateBucket
	rw *sync.RWMutex
}

func NewRateBucketProtected(m int64, r int64, v int64) *RateBucketProtected {
	rb := NewRateBucket(m, r, v)
	return &RateBucketProtected{*rb, &sync.RWMutex{}}
}

func (rb *RateBucketProtected) use(units int64) bool {
	rb.rw.Lock()
	defer rb.rw.Unlock()
	return rb.RateBucket.use(units)
}

func (rb *RateBucketProtected) setrate(newrate int64) {
	rb.rw.Lock()
	defer rb.rw.Unlock()
	rb.RateBucket.setrate(newrate)
}

func (rb *RateBucketProtected) getrate() int64 {
	rb.rw.RLock()
	defer rb.rw.RUnlock()
	return rb.RateBucket.getrate()
}

func (rb *RateBucketProtected) above(units int64) bool {
	rb.rw.RLock()
	defer rb.rw.RUnlock()
	return rb.RateBucket.above(units)
}

func (rb *RateBucketProtected) below(units int64) bool {
	rb.rw.RLock()
	defer rb.rw.RUnlock()
	return rb.RateBucket.below(units)
}

func (rb *RateBucketProtected) String() string {
	rb.rw.RLock()
	defer rb.rw.RUnlock()
	return "rbp:" + rb.RateBucket.String()
}

//=====================================================================
// RateBucketAIMD
//=====================================================================
// RateBucketAIMD embeds the simple RateBucket and implements RateBucketInterface.
//
// The logic inside will add minrate to its own current rate (the additive step)
// and divides the RateBucketAIMD.rate by the RateBucketAIMD.div when
// being dinged. The ding() method here represents congestion
// notification (think "ECN") for the client, prompting the latter to
// reduce its own rate multiplicatively.
// The "additive" step is automatically performed each configAIMD.sizeAddBits
// units used (via RateBucketAIMD.use()) without any intermediate ding()
// calls.
//
type RateBucketAIMD struct {
	RateBucket
	minrate int64
	maxrate int64
	//
	div           int
	units2addrate int64 // units to use at the current rate
}

func NewRateBucketAIMD(minrate int64, maxrate int64, maxval int64, div int) *RateBucketAIMD {
	rb := NewRateBucket(maxval, minrate, maxval) // fully charged & timer-stopped
	return &RateBucketAIMD{*rb, minrate, maxrate, div, configAIMD.sizeAddBits}
}

func (rb *RateBucketAIMD) __addrate() {
	rb.rate += rb.minrate // add == minrate
	if rb.rate > rb.maxrate {
		rb.rate = rb.maxrate
	}
}

func (rb *RateBucketAIMD) use(units int64) bool {
	rb.__addtime()
	if units > rb.value {
		return false
	}
	if rb.value == rb.maxval && rb.rate > 0 {
		rb.timestamp = Now // START the ratebucket timer
	}
	rb.value -= units
	rb.units2addrate -= units
	if rb.units2addrate <= 0 {
		rb.__addrate()
		rb.units2addrate = configAIMD.sizeAddBits
	}
	return true
}

func (rb *RateBucketAIMD) ding() {
	rb.rate /= int64(rb.div)
	if rb.rate < rb.minrate {
		rb.rate = rb.minrate
	}
	rb.units2addrate = configAIMD.sizeAddBits
}

func (rb *RateBucketAIMD) String() string {
	return fmt.Sprintf("%+v", *rb)
}

//=====================================================================
// DummyRateBucket
//=====================================================================
// DummyRateBucket represents link rate and unlimited value (to use)
// serving as a convenience-placeholder for the reservation-based flows,
// or more generally, flows that are throttled by non-ratebucket type
// mechanisms
type DummyRateBucket struct {
}

func (rb *DummyRateBucket) use(units int64) bool {
	return true
}

func (rb *DummyRateBucket) setrate(newrate int64) {
}

func (rb *DummyRateBucket) getrate() int64 {
	return configNetwork.linkbps
}

func (rb *DummyRateBucket) above(units int64) bool {
	return true
}

func (rb *DummyRateBucket) below(units int64) bool {
	return false
}

func (rb *DummyRateBucket) String() string {
	return "dummy-rb"
}

//=====================================================================
// DatagramRateBucket
//=====================================================================
// DatagramRateBucket provides a limited link rate semantics
// to send/receive a packet (a datagram) of a given size
// The "value" (e.g. max num of bits) this bucket can hold is in the
// no way limited: that is - assumed to replenish itself instantaneously
type DatagramRateBucket struct {
	rate    int64     // units/sec
	nextime time.Time // next time can send/receive
}

func NewDatagramRateBucket(r int64) *DatagramRateBucket {
	return &DatagramRateBucket{r, Now}
}

func (rb *DatagramRateBucket) use(units int64) bool {
	if rb.nextime.After(Now) {
		return false
	}
	d := sizeToDuration(int(units), "b", rb.rate, "b")
	rb.nextime = Now.Add(d)
	return true
}

func (rb *DatagramRateBucket) setrate(newrate int64) {
	rb.rate = newrate
}

func (rb *DatagramRateBucket) getrate() int64 {
	return rb.rate
}

func (rb *DatagramRateBucket) above(units int64) bool {
	return !rb.below(units)
}

func (rb *DatagramRateBucket) below(units int64) bool {
	return rb.nextime.After(Now)
}

func (rb *DatagramRateBucket) String() string {
	return fmt.Sprintf("%+v", *rb)
}
