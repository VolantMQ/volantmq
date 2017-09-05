package systree

import (
	"sync/atomic"

	"github.com/VolantMQ/volantmq/types"
)

type stat struct {
	curr *dynamicValueInteger
	max  *dynamicValueInteger
}

type topicStat struct {
	stat
}

type subscriptionsStat struct {
	stat
}

func newStat(topicPrefix string, retained *[]types.RetainObject) stat {
	s := stat{
		curr: newDynamicValueInteger(topicPrefix + "/current"),
		max:  newDynamicValueInteger(topicPrefix + "/max"),
	}

	*retained = append(*retained, s.max)
	*retained = append(*retained, s.curr)

	return s
}

func newStatTopic(topicPrefix string, retained *[]types.RetainObject) topicStat {
	return topicStat{
		stat: newStat(topicPrefix+"/topics", retained),
	}
}

func newStatSubscription(topicPrefix string, retained *[]types.RetainObject) subscriptionsStat {
	return subscriptionsStat{
		stat: newStat(topicPrefix+"/subscriptions", retained),
	}
}

// Subscribed add to statistic subscriber
func (t *subscriptionsStat) Subscribed() {
	newVal := atomic.AddUint64(&t.curr.val, 1)
	if atomic.LoadUint64(&t.max.val) < newVal {
		atomic.StoreUint64(&t.max.val, newVal)
	}
}

// UnSubscribed remove subscriber from statistic
func (t *subscriptionsStat) UnSubscribed() {
	atomic.AddUint64(&t.curr.val, ^uint64(0))
}

// Added add topic to statistic
func (t *topicStat) Added() {
	newVal := atomic.AddUint64(&t.curr.val, 1)
	if atomic.LoadUint64(&t.max.val) < newVal {
		atomic.StoreUint64(&t.max.val, newVal)
	}
}

// Removed remove topic from statistic
func (t *topicStat) Removed() {
	atomic.AddUint64(&t.curr.val, ^uint64(0))
}
