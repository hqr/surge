package surge

import (
	"fmt"
	"time"
)

//=====================================================================
// RateBucketInterface
//=====================================================================
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
	if rb.value == rb.maxval || rb.rate == int64(0) {
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
	if rb.value == rb.maxval && rb.rate > int64(0) {
		rb.timestamp = Now // START the ratebucket timer
	}
	rb.value -= units
	return true
}

func (rb *RateBucket) setrate(newrate int64) {
	rb.__addtime()
	rb.rate = newrate
	if rb.value < rb.maxval && rb.rate > int64(0) {
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
// RateBucketAIMD
//=====================================================================
type RateBucketAIMD struct {
	RateBucket
	minrate int64
	maxrate int64
	//
	addival       time.Duration
	div           int
	units2addrate int64 // units to use at the current rate
}

func NewRateBucketAIMD(minrate int64, maxrate int64, maxval int64, addival time.Duration, div int) *RateBucketAIMD {
	rb := NewRateBucket(maxval, minrate, maxval) // fully charged & timer-stopped

	u := rb.rate * int64(addival) / int64(time.Second)
	return &RateBucketAIMD{*rb, minrate, maxrate, addival, div, u}
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
	if rb.value == rb.maxval && rb.rate > int64(0) {
		rb.timestamp = Now // START the ratebucket timer
	}
	rb.value -= units
	rb.units2addrate -= units
	if rb.units2addrate <= 0 {
		rb.__addrate()
		rb.units2addrate = rb.rate * int64(rb.addival) / int64(time.Second)
	}
	return true
}

func (rb *RateBucketAIMD) ding() {
	rb.rate /= int64(rb.div)
	if rb.rate < rb.minrate {
		rb.rate = rb.minrate
	}
	rb.units2addrate = rb.rate * int64(rb.addival) / int64(time.Second)
}

func (rb *RateBucketAIMD) String() string {
	return fmt.Sprintf("%+v", *rb)
}
