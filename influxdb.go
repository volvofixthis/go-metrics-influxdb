package influxdb

import (
	"fmt"
	"log"
	uurl "net/url"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
)

type reporter struct {
	reg      metrics.Registry
	interval time.Duration
	align    bool
	url      uurl.URL
	database string

	measurement string
	username    string
	password    string
	tags        map[string]string

	client client.Client
}

// InfluxDB starts a InfluxDB reporter which will post the metrics from the given registry at each d interval.
func InfluxDB(r metrics.Registry, d time.Duration, url, database, measurement, username, password string, align bool) {
	InfluxDBWithTags(r, d, url, database, measurement, username, password, map[string]string{}, align)
}

// InfluxDBWithTags starts a InfluxDB reporter which will post the metrics from the given registry at each d interval with the specified tags
func InfluxDBWithTags(r metrics.Registry, d time.Duration, url, database, measurement, username, password string, tags map[string]string, align bool) {
	u, err := uurl.Parse(url)
	if err != nil {
		log.Printf("unable to parse InfluxDB url %s. err=%v", url, err)
		return
	}

	rep := &reporter{
		reg:         r,
		interval:    d,
		url:         *u,
		database:    database,
		measurement: measurement,
		username:    username,
		password:    password,
		tags:        tags,
		align:       align,
	}
	if err := rep.makeClient(); err != nil {
		log.Printf("unable to make InfluxDB client. err=%v", err)
		return
	}

	rep.run()
}

func (r *reporter) makeClient() (err error) {
	if r.url.Scheme == "http" {
		r.client, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     r.url.String(),
			Username: r.username,
			Password: r.password,
			Timeout:  10 * time.Second,
		})
	} else {
		r.client, err = client.NewUDPClient(client.UDPConfig{
			Addr:        r.url.Host,
			PayloadSize: 512,
		})
	}

	return
}

func (r *reporter) run() {
	intervalTicker := time.Tick(r.interval)
	pingTicker := time.Tick(time.Second * 5)

	for {
		select {
		case <-intervalTicker:
			if err := r.send(); err != nil {
				log.Printf("unable to send metrics to InfluxDB. err=%v", err)
			}
		case <-pingTicker:
			_, _, err := r.client.Ping(time.Second)
			if err != nil {
				log.Printf("got error while sending a ping to InfluxDB, trying to recreate client. err=%v", err)

				if err = r.makeClient(); err != nil {
					log.Printf("unable to make InfluxDB client. err=%v", err)
				}
			}
		}
	}
}

func (r *reporter) send() error {
	var pts []*client.Point

	now := time.Now()
	if r.align {
		now = now.Truncate(r.interval)
	}
	r.reg.Each(func(name string, i interface{}) {

		switch metric := i.(type) {
		case metrics.Counter:
			ms := metric.Snapshot()
			pt, err := client.NewPoint(
				r.measurement,
				r.tags,
				map[string]interface{}{
					fmt.Sprintf("%s.count", name): ms.Count(),
				},
				now,
			)
			if err != nil {
				return
			}
			pts = append(pts, pt)
		case metrics.Gauge:
			ms := metric.Snapshot()
			pt, err := client.NewPoint(
				r.measurement,
				r.tags,
				map[string]interface{}{
					fmt.Sprintf("%s.gauge", name): ms.Value(),
				},
				now,
			)
			if err != nil {
				return
			}
			pts = append(pts, pt)
		case metrics.GaugeFloat64:
			ms := metric.Snapshot()
			pt, err := client.NewPoint(
				r.measurement,
				r.tags,
				map[string]interface{}{
					fmt.Sprintf("%s.gauge", name): ms.Value(),
				},
				now,
			)
			if err != nil {
				return
			}
			pts = append(pts, pt)
		case metrics.Histogram:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fields := map[string]float64{
				"count":    float64(ms.Count()),
				"max":      float64(ms.Max()),
				"mean":     ms.Mean(),
				"min":      float64(ms.Min()),
				"stddev":   ms.StdDev(),
				"variance": ms.Variance(),
				"p50":      ps[0],
				"p75":      ps[1],
				"p95":      ps[2],
				"p99":      ps[3],
				"p999":     ps[4],
				"p9999":    ps[5],
			}
			for k, v := range fields {
				pt, err := client.NewPoint(
					r.measurement,
					bucketTags(k, r.tags),
					map[string]interface{}{
						fmt.Sprintf("%s.histogram", name): v,
					},
					now,
				)
				if err != nil {
					continue
				}
				pts = append(pts, pt)
			}
		case metrics.Meter:
			ms := metric.Snapshot()
			fields := map[string]float64{
				"count": float64(ms.Count()),
				"m1":    ms.Rate1(),
				"m5":    ms.Rate5(),
				"m15":   ms.Rate15(),
				"mean":  ms.RateMean(),
			}
			for k, v := range fields {
				pt, err := client.NewPoint(
					r.measurement,
					bucketTags(k, r.tags),
					map[string]interface{}{
						fmt.Sprintf("%s.meter", name): v,
					},
					now,
				)
				if err != nil {
					continue
				}
				pts = append(pts, pt)
			}

		case metrics.Timer:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fields := map[string]float64{
				"count":    float64(ms.Count()),
				"max":      float64(ms.Max()),
				"mean":     ms.Mean(),
				"min":      float64(ms.Min()),
				"stddev":   ms.StdDev(),
				"variance": ms.Variance(),
				"p50":      ps[0],
				"p75":      ps[1],
				"p95":      ps[2],
				"p99":      ps[3],
				"p999":     ps[4],
				"p9999":    ps[5],
				"m1":       ms.Rate1(),
				"m5":       ms.Rate5(),
				"m15":      ms.Rate15(),
				"meanrate": ms.RateMean(),
			}
			for k, v := range fields {
				pt, err := client.NewPoint(
					r.measurement,
					bucketTags(k, r.tags),
					map[string]interface{}{
						fmt.Sprintf("%s.timer", name): v,
					},
					now,
				)
				if err != nil {
					continue
				}
				pts = append(pts, pt)
			}
		}
	})

	retryBatch, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  r.database,
		Precision: "s",
	})
	retryBatch.AddPoints(pts)

	err := r.client.Write(retryBatch)
	return err
}

func bucketTags(bucket string, tags map[string]string) map[string]string {
	m := map[string]string{}
	for tk, tv := range tags {
		m[tk] = tv
	}
	m["bucket"] = bucket
	return m
}
