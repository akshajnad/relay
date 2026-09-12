package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"image/color"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
)

// Each backend keeps one color in every chart (validated categorical
// palette, fixed order). Two of these sit under 3:1 contrast on the
// surface, so every bar also carries a value label, and the README has
// the same numbers as tables.
var backendColor = map[string]color.RGBA{
	"relay-redis": rgb(0x2a78d6),
	"relay-raft":  rgb(0xeb6834),
	"asynq":       rgb(0x1baf7a),
	"asynq-tuned": rgb(0xeda100),
}

var (
	inkPrimary   = rgb(0x1f1f1d)
	inkSecondary = rgb(0x5f5e58)
	gridColor    = rgb(0xe6e5e0)
	surface      = rgb(0xfcfcfb)
)

func rgb(v uint32) color.RGBA {
	return color.RGBA{R: uint8(v >> 16), G: uint8(v >> 8), B: uint8(v), A: 255}
}

func plotMain(args []string) error {
	fs := flag.NewFlagSet("plot", flag.ExitOnError)
	in := fs.String("results", "", "compare results JSON")
	out := fs.String("out", "", "chart directory (default: next to the results)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	b, err := os.ReadFile(*in)
	if err != nil {
		return err
	}
	var rep CompareReport
	if err := json.Unmarshal(b, &rep); err != nil {
		return err
	}
	dir := *out
	if dir == "" {
		dir = strings.TrimSuffix(*in, ".json")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return plotReport(&rep, dir)
}

func newPlot(title, ylabel string) *plot.Plot {
	p := plot.New()
	p.Title.Text = title
	p.Title.TextStyle.Color = inkPrimary
	p.Title.TextStyle.Font.Size = vg.Points(13)
	p.Title.Padding = vg.Points(8)
	p.BackgroundColor = surface
	p.Y.Label.Text = ylabel
	for _, ax := range []*plot.Axis{&p.X, &p.Y} {
		ax.Color = inkSecondary
		ax.Label.TextStyle.Color = inkSecondary
		ax.Tick.Label.Color = inkSecondary
		ax.Tick.Color = inkSecondary
	}
	g := plotter.NewGrid()
	g.Vertical.Color = color.Transparent
	g.Horizontal.Color = gridColor
	g.Horizontal.Width = vg.Points(0.5)
	p.Add(g)
	p.Legend.Top = true
	p.Legend.TextStyle.Color = inkPrimary
	p.Legend.TextStyle.Font.Size = vg.Points(9)
	return p
}

func save(p *plot.Plot, dir, name string, w, h vg.Length) error {
	return p.Save(w, h, filepath.Join(dir, name))
}

func fmtValue(v float64, unit string) string {
	switch {
	case v >= 100:
		return fmt.Sprintf("%.0f%s", v, unit)
	case v >= 10:
		return fmt.Sprintf("%.1f%s", v, unit)
	default:
		return fmt.Sprintf("%.2f%s", v, unit)
	}
}

// barChart draws one bar per backend with its value on top.
func barChart(dir, name, title, ylabel, unit string, names []string, values []float64) error {
	if len(values) == 0 {
		return nil
	}
	p := newPlot(title, ylabel)
	w := vg.Points(42)
	var labels plotter.XYLabels
	for i, v := range values {
		bars, err := plotter.NewBarChart(plotter.Values{v}, w)
		if err != nil {
			return err
		}
		bars.Color = backendColor[names[i]]
		bars.LineStyle.Width = 0
		bars.XMin = float64(i)
		p.Add(bars)
		labels.XYs = append(labels.XYs, plotter.XY{X: float64(i), Y: v})
		labels.Labels = append(labels.Labels, fmtValue(v, unit))
	}
	l, err := plotter.NewLabels(labels)
	if err != nil {
		return err
	}
	for i := range l.TextStyle {
		l.TextStyle[i].Color = inkPrimary
		l.TextStyle[i].XAlign = draw.XCenter
		l.TextStyle[i].YAlign = draw.YBottom
	}
	l.Offset = vg.Point{Y: vg.Points(3)}
	p.Add(l)
	p.NominalX(names...)
	p.X.Min, p.X.Max = -0.6, float64(len(names))-0.4
	p.Y.Min = 0
	p.Y.Max = maxOf(values) * 1.15
	return save(p, dir, name, vg.Points(560), vg.Points(340))
}

// percentileChart is a range-dot plot on a log axis (backends differ by
// orders of magnitude): per backend, a line from p50 to p99 with a marker
// at p50, p95 and p99, each labelled with its value. Marker shape encodes
// the percentile; color and the x-axis name encode the backend.
func percentileChart(dir, name, title string, names []string, dists []Dist) error {
	if len(dists) == 0 {
		return nil
	}
	p := newPlot(title, "milliseconds (log scale)")
	p.Y.Scale = plot.LogScale{}
	p.Y.Tick.Marker = plot.LogTicks{Prec: -1}
	shapes := []draw.GlyphDrawer{draw.CircleGlyph{}, draw.TriangleGlyph{}, draw.SquareGlyph{}}
	pctNames := []string{"p50", "p95", "p99"}
	floor, ceil := math.Inf(1), 0.0
	// p50 and p99 are labelled on the right, p95 on the left, so close
	// values never collide.
	var right, left plotter.XYLabels
	for i, d := range dists {
		vals := []float64{math.Max(d.P50Ms, 0.01), math.Max(d.P95Ms, 0.01), math.Max(d.P99Ms, 0.01)}
		x := float64(i)
		line, err := plotter.NewLine(plotter.XYs{{X: x, Y: vals[0]}, {X: x, Y: vals[2]}})
		if err != nil {
			return err
		}
		line.Color = backendColor[names[i]]
		line.Width = vg.Points(2)
		p.Add(line)
		for k, v := range vals {
			sc, err := plotter.NewScatter(plotter.XYs{{X: x, Y: v}})
			if err != nil {
				return err
			}
			sc.GlyphStyle = draw.GlyphStyle{Color: backendColor[names[i]], Radius: vg.Points(4.5), Shape: shapes[k]}
			p.Add(sc)
			if i == 0 {
				legend := draw.GlyphStyle{Color: inkSecondary, Radius: vg.Points(4), Shape: shapes[k]}
				p.Legend.Add(pctNames[k], legendGlyph{legend})
			}
			floor, ceil = math.Min(floor, v), math.Max(ceil, v)
			side := &right
			if k == 1 {
				side = &left
			}
			side.XYs = append(side.XYs, plotter.XY{X: x, Y: v})
			side.Labels = append(side.Labels, pctNames[k]+" "+fmtValue(v, "ms"))
		}
	}
	for _, side := range []struct {
		xy    plotter.XYLabels
		align draw.XAlignment
		dx    vg.Length
	}{{right, draw.XLeft, vg.Points(8)}, {left, draw.XRight, vg.Points(-8)}} {
		l, err := plotter.NewLabels(side.xy)
		if err != nil {
			return err
		}
		for i := range l.TextStyle {
			l.TextStyle[i].Color = inkPrimary
			l.TextStyle[i].XAlign = side.align
			l.TextStyle[i].YAlign = draw.YCenter
			l.TextStyle[i].Font.Size = vg.Points(7.5)
		}
		l.Offset = vg.Point{X: side.dx}
		p.Add(l)
	}
	p.NominalX(names...)
	p.X.Min, p.X.Max = -0.6, float64(len(names))-0.3
	p.Y.Min = math.Pow(10, math.Floor(math.Log10(floor)))
	p.Y.Max = math.Pow(10, math.Ceil(math.Log10(ceil*1.5)))
	return save(p, dir, name, vg.Points(680), vg.Points(400))
}

// legendGlyph draws a percentile's marker shape in the legend.
type legendGlyph struct{ style draw.GlyphStyle }

func (g legendGlyph) Thumbnail(c *draw.Canvas) {
	c.DrawGlyph(g.style, c.Center())
}

// cdfChart plots empirical CDFs of trial times, one line per backend.
func cdfChart(dir, name, title, xlabel string, names []string, series [][]float64, logX bool) error {
	if len(series) == 0 {
		return nil
	}
	p := newPlot(title, "fraction of trials")
	p.X.Label.Text = xlabel
	if logX {
		p.X.Scale = plot.LogScale{}
		p.X.Tick.Marker = plot.LogTicks{Prec: -1}
	}
	for i, s := range series {
		if len(s) == 0 {
			continue
		}
		v := append([]float64(nil), s...)
		sort.Float64s(v)
		pts := make(plotter.XYs, 0, 2*len(v))
		for k, x := range v {
			prev := float64(k) / float64(len(v))
			pts = append(pts, plotter.XY{X: x, Y: prev}, plotter.XY{X: x, Y: float64(k+1) / float64(len(v))})
		}
		line, err := plotter.NewLine(pts)
		if err != nil {
			return err
		}
		line.Color = backendColor[names[i]]
		line.Width = vg.Points(2)
		p.Add(line)
		// Median by nearest rank, matching Dist.P50Ms in the tables.
		median := v[int(math.Ceil(0.5*float64(len(v))))-1]
		p.Legend.Add(fmt.Sprintf("%s (n=%d, median %.2fs)", names[i], len(v), median), line)
	}
	p.Y.Min, p.Y.Max = 0, 1.05
	if logX {
		lo, hi := math.Inf(1), 0.0
		for _, s := range series {
			for _, v := range s {
				lo, hi = math.Min(lo, v), math.Max(hi, v)
			}
		}
		p.X.Min = math.Pow(10, math.Floor(math.Log10(lo)))
		p.X.Max = math.Pow(10, math.Ceil(math.Log10(hi)))
	} else {
		p.X.Min = 0 // a linear time axis starts at zero
	}
	// Bottom of the middle of the axis: CDFs start at the bottom-left and
	// finish at the top-right, and backends separate by orders of magnitude.
	p.Legend.Top = false
	p.Legend.Left = true
	p.Legend.XOffs, p.Legend.YOffs = vg.Points(230), vg.Points(40)
	return save(p, dir, name, vg.Points(640), vg.Points(380))
}

func maxOf(v []float64) float64 {
	m := 0.0
	for _, x := range v {
		m = math.Max(m, x)
	}
	return m
}

// plotReport writes every chart for a compare run.
func plotReport(rep *CompareReport, dir string) error {
	var satNames, latNames, recNames, foNames []string
	var sat []float64
	var disp, wait []Dist
	var rec, fo [][]float64
	for _, r := range rep.Results {
		n := r.Backend.Name
		if r.Saturation != nil {
			satNames, sat = append(satNames, n), append(sat, r.Saturation.JobsPerSec)
		}
		if r.Latency != nil {
			latNames = append(latNames, n)
			disp, wait = append(disp, r.Latency.Dispatch), append(wait, r.Latency.QueueWait)
		}
		if r.Recovery != nil && len(r.Recovery.Seconds) > 0 {
			recNames, rec = append(recNames, n), append(rec, r.Recovery.Seconds)
		}
		if r.Failover != nil && len(r.Failover.Seconds) > 0 {
			foNames, fo = append(foNames, n), append(fo, r.Failover.Seconds)
		}
	}
	steps := []func() error{
		func() error {
			return barChart(dir, "throughput.png", "Throughput at saturation (noop jobs, 100 slots)", "jobs / second", "", satNames, sat)
		},
		func() error {
			return percentileChart(dir, "dispatch_latency.png", "Dispatch latency, externally measured (500 jobs/s)", latNames, disp)
		},
		func() error {
			return percentileChart(dir, "queue_wait.png", "End-to-end queue wait: submit to handler start (500 jobs/s)", latNames, wait)
		},
		func() error {
			return cdfChart(dir, "worker_recovery.png", "Worker-failure recovery: kill to jobs re-leased", "seconds (log scale)", recNames, rec, true)
		},
		func() error {
			return cdfChart(dir, "leader_failover.png", "Raft leader failover: kill to first acquire on the new leader", "seconds", foNames, fo, false)
		},
	}
	for _, s := range steps {
		if err := s(); err != nil {
			return err
		}
	}
	return nil
}

// printCompare prints the headline tables.
func printCompare(rep *CompareReport) {
	fmt.Println()
	fmt.Printf("%-12s %12s %26s %26s %22s %16s\n", "backend", "jobs/s", "dispatch p50/p95/p99 ms", "queue wait p50/p95/p99", "recovery p50/max s", "failover p50 s")
	for _, r := range rep.Results {
		row := fmt.Sprintf("%-12s", r.Backend.Name)
		if r.Saturation != nil {
			row += fmt.Sprintf(" %12.0f", r.Saturation.JobsPerSec)
		} else {
			row += fmt.Sprintf(" %12s", "-")
		}
		if l := r.Latency; l != nil {
			row += fmt.Sprintf(" %26s %22s", fmt.Sprintf("%.2f/%.2f/%.2f", l.Dispatch.P50Ms, l.Dispatch.P95Ms, l.Dispatch.P99Ms),
				fmt.Sprintf("%.1f/%.1f/%.1f", l.QueueWait.P50Ms, l.QueueWait.P95Ms, l.QueueWait.P99Ms))
		} else {
			row += fmt.Sprintf(" %26s %22s", "-", "-")
		}
		if t := r.Recovery; t != nil && t.Trials > 0 {
			row += fmt.Sprintf(" %22s", fmt.Sprintf("%.2f/%.2f", t.Dist.P50Ms/1000, t.Dist.MaxMs/1000))
		} else {
			row += fmt.Sprintf(" %22s", "-")
		}
		if t := r.Failover; t != nil && t.Trials > 0 {
			row += fmt.Sprintf(" %16.3f", t.Dist.P50Ms/1000)
		}
		fmt.Println(row)
	}
}
