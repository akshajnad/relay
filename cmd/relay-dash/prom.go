package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// scrape is one coordinator's /metrics: counters and gauges by name, and
// histograms by name. Relay's metrics are unlabelled apart from constant
// labels, so each family holds a single series.
type scrape struct {
	values map[string]float64
	hists  map[string]histogram
}

func fetchScrape(ctx context.Context, client *http.Client, addr string) (*scrape, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /metrics: %s", resp.Status)
	}
	return parseScrape(resp.Body)
}

func parseScrape(r io.Reader) (*scrape, error) {
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(r)
	if err != nil {
		return nil, err
	}
	s := &scrape{values: map[string]float64{}, hists: map[string]histogram{}}
	for name, f := range fams {
		if len(f.GetMetric()) == 0 {
			continue
		}
		m := f.GetMetric()[0]
		switch f.GetType() {
		case dto.MetricType_COUNTER:
			s.values[name] = m.GetCounter().GetValue()
		case dto.MetricType_GAUGE:
			s.values[name] = m.GetGauge().GetValue()
		case dto.MetricType_HISTOGRAM:
			s.hists[name] = histogramFrom(m.GetHistogram())
		}
	}
	return s, nil
}

// histogram is a Prometheus histogram: ascending upper bounds, the last
// being +Inf, and cumulative counts.
type histogram struct {
	bounds []float64
	counts []float64
}

func histogramFrom(h *dto.Histogram) histogram {
	var out histogram
	for _, b := range h.GetBucket() {
		out.bounds = append(out.bounds, b.GetUpperBound())
		out.counts = append(out.counts, float64(b.GetCumulativeCount()))
	}
	if n := len(out.bounds); n == 0 || !math.IsInf(out.bounds[n-1], 1) {
		out.bounds = append(out.bounds, math.Inf(1))
		out.counts = append(out.counts, float64(h.GetSampleCount()))
	}
	return out
}

func (h histogram) total() float64 {
	if len(h.counts) == 0 {
		return 0
	}
	return h.counts[len(h.counts)-1]
}

func (h histogram) sameBuckets(o histogram) bool {
	if len(h.bounds) != len(o.bounds) {
		return false
	}
	for i := range h.bounds {
		if h.bounds[i] != o.bounds[i] {
			return false
		}
	}
	return true
}

// since returns the observations recorded after prev. If any count went
// down the process restarted (or its metrics were reset), so everything in
// h is new.
func (h histogram) since(prev histogram) histogram {
	if !h.sameBuckets(prev) {
		return h
	}
	out := histogram{bounds: h.bounds, counts: make([]float64, len(h.counts))}
	for i := range h.counts {
		d := h.counts[i] - prev.counts[i]
		if d < 0 {
			return h
		}
		out.counts[i] = d
	}
	return out
}

// plus adds two histograms with the same buckets; an empty one is the
// identity.
func (h histogram) plus(o histogram) histogram {
	if len(h.bounds) == 0 {
		return o
	}
	if len(o.bounds) == 0 || !h.sameBuckets(o) {
		return h
	}
	out := histogram{bounds: h.bounds, counts: make([]float64, len(h.counts))}
	for i := range h.counts {
		out.counts[i] = h.counts[i] + o.counts[i]
	}
	return out
}

// quantile estimates the q-quantile the way PromQL's histogram_quantile
// does: find the bucket holding the rank, then interpolate linearly inside
// it. A rank in the +Inf bucket reports the highest finite bound.
func (h histogram) quantile(q float64) (float64, bool) {
	total := h.total()
	if total == 0 {
		return 0, false
	}
	rank := q * total
	i := sort.Search(len(h.counts), func(i int) bool { return h.counts[i] >= rank })
	if i >= len(h.counts) {
		i = len(h.counts) - 1
	}
	if math.IsInf(h.bounds[i], 1) {
		if i == 0 {
			return 0, false
		}
		return h.bounds[i-1], true
	}
	lower, below := 0.0, 0.0
	if i > 0 {
		lower, below = h.bounds[i-1], h.counts[i-1]
	}
	inBucket := h.counts[i] - below
	if inBucket <= 0 {
		return h.bounds[i], true
	}
	return lower + (h.bounds[i]-lower)*(rank-below)/inBucket, true
}
