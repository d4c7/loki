package metric

import (
	"sync"
	"time"

	"github.com/grafana/loki/pkg/util"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
)

// Expirable allows checking if something has exceeded the provided maxAge based on the provided currentTime
type Expirable interface {
	HasExpired(currentTimeSec int64, maxAgeSec int64) bool
}

// Timestamp returns the last explicit time registered or nil
type ExplicitTimestamp interface {
	Timestamp() *time.Time
	ApplyTimestamp(t *time.Time)
}

type metricVec struct {
	factory   func(labels map[string]string) prometheus.Metric
	mtx       sync.Mutex
	metrics   map[model.Fingerprint]prometheus.Metric
	maxAgeSec int64
}

func newMetricVec(factory func(labels map[string]string) prometheus.Metric, maxAgeSec int64) *metricVec {
	return &metricVec{
		metrics:   map[model.Fingerprint]prometheus.Metric{},
		factory:   factory,
		maxAgeSec: maxAgeSec,
	}
}

// Describe implements prometheus.Collector and doesn't declare any metrics on purpose to bypass prometheus validation.
// see https://godoc.org/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics search for "unchecked"
func (c *metricVec) Describe(ch chan<- *prometheus.Desc) {}

// Collect implements prometheus.Collector
func (c *metricVec) Collect(ch chan<- prometheus.Metric) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, m := range c.metrics {
		ch <- c.metric(m)
	}
	c.prune()
}

// With returns the metric associated with the labelset.
func (c *metricVec) With(labels model.LabelSet) prometheus.Metric {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	fp := labels.Fingerprint()
	var ok bool
	var metric prometheus.Metric
	if metric, ok = c.metrics[fp]; !ok {
		metric = c.factory(util.ModelLabelSetToMap(labels))
		c.metrics[fp] = metric
	}
	return metric
}

// prune will remove all metrics which implement the Expirable interface and have expired
// it does not take out a lock on the metrics map so whoever calls this function should do so.
func (c *metricVec) prune() {
	currentTimeSec := time.Now().Unix()
	for fp, m := range c.metrics {
		if em, ok := m.(Expirable); ok {
			if em.HasExpired(currentTimeSec, c.maxAgeSec) {
				delete(c.metrics, fp)
			}
		}
	}
}

func (c *metricVec) metric(m prometheus.Metric) prometheus.Metric {
	var explicitTime *time.Time

	if ts, ok := m.(ExplicitTimestamp); ok {
		explicitTime = ts.Timestamp()
	}

	if explicitTime == nil {
		// regular metric
		return m
	}

	w := dto.Metric{}
	//write and resets the metric
	if err := m.Write(&w); err != nil {
		//TODO:log warn or debug
		return m
	}

	//create const metric from the dto metric
	var metricConst prometheus.Metric
	switch m.(type) {
	case *expiringCounter:
		tc := w.Counter
		metricConst = prometheus.MustNewConstMetric(m.Desc(), prometheus.CounterValue, *tc.Value)
	case *expiringGauge:
		tc := w.Gauge
		metricConst = prometheus.MustNewConstMetric(m.Desc(), prometheus.GaugeValue, *tc.Value)
	case *expiringHistogram:
		tc := w.Histogram
		metricConst = prometheus.MustNewConstHistogram(m.Desc(), *tc.SampleCount, *tc.SampleSum, buckets(tc.Bucket))
	default:
		//TODO:log warn or debug
		//probably a new metric type was added and it's not managed here
		return m
	}
	//timestamped metric
	return prometheus.NewMetricWithTimestamp(*explicitTime, metricConst)

}

func buckets(buckets []*dto.Bucket) map[float64]uint64 {
	m := map[float64]uint64{}
	for _, i := range buckets {
		m[*i.UpperBound] = *i.CumulativeCount
	}
	return m
}
