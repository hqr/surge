package surge

import (
	"fmt"
	"time"
)

//
// types
//
type RateBucket struct {
	max       int64
	rate      int64 // units/sec
	value     int64
	rateptr   *int64
	timestamp time.Time
}

func NewRateBucket(m int64, r int64, v int64, rptr *int64) *RateBucket {
	if rptr != nil {
		r = *rptr
	}
	return &RateBucket{max: m, rate: r, value: v, rateptr: rptr, timestamp: Now}
}

func (rb *RateBucket) String() string {
	return fmt.Sprintf("%+v", *rb)
}

func (rb *RateBucket) addtime() bool {
	if rb.value == rb.max || rb.timestamp.Equal(Now) {
		return false
	}
	if rb.rateptr != nil {
		rb.rate = *rb.rateptr
	}
	d := Now.Sub(rb.timestamp)
	rb.value += rb.rate * int64(d) / int64(time.Second)
	if rb.value > rb.max {
		rb.value = rb.max
	}
	rb.timestamp = Now
	return true
}

func (rb *RateBucket) maximize() {
	rb.value = rb.max
}

func (rb *RateBucket) use(units int64) bool {
	if units > rb.value {
		return false
	}
	rb.value -= units
	return true
}

func (rb *RateBucket) above(units int64) bool {
	return units <= rb.value
}

func (rb *RateBucket) below(units int64) bool {
	return units > rb.value
}
