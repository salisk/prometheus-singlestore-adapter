package s2prometheus

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"

	"prometheus-singlestore-adapter/pkg/reader"
)

type Querier struct {
	mint, maxt int64
	client     reader.Reader

	// Derived from configuration.
	externalLabels   labels.Labels
	requiredMatchers []*labels.Matcher
}

func (q *Querier) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	//TODO implement me
	return q, nil
}

func NewQuerier(mint, maxt int64, client reader.Reader, externalLabels labels.Labels, requiredMatchers []*labels.Matcher) Querier {
	return Querier{
		mint:             mint,
		maxt:             maxt,
		client:           client,
		externalLabels:   externalLabels,
		requiredMatchers: requiredMatchers,
	}
}

func (q *Querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if len(q.requiredMatchers) > 0 {
		// Copy to not modify slice configured by user.
		requiredMatchers := append([]*labels.Matcher{}, q.requiredMatchers...)
		for _, m := range matchers {
			for i, r := range requiredMatchers {
				if m.Type == labels.MatchEqual && m.Name == r.Name && m.Value == r.Value {
					// Requirement matched.
					requiredMatchers = append(requiredMatchers[:i], requiredMatchers[i+1:]...)
					break
				}
			}
			if len(requiredMatchers) == 0 {
				break
			}
		}
		if len(requiredMatchers) > 0 {
			return storage.NoopSeriesSet()
		}
	}

	m, added := q.addExternalLabels(matchers)
	query, err := remote.ToQuery(q.mint, q.maxt, m, hints)
	if err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("toQuery: %w", err))
	}

	fmt.Printf("[Promql] matchers: %v, query: %s\n", m, query.String())
	res, err := q.client.Read(&prompb.ReadRequest{Queries: []*prompb.Query{query}})
	fmt.Printf("[Promql] response: %v\n", res)
	if err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("remote_read: %w", err))
	}
	fmt.Printf("[Promql] response sort: %d, added: %v, results: %v\n", sortSeries, added, res.Results[0])
	fmt.Printf("[Promql] response result 0 timeseries: %v\n", res.Results[0].Timeseries)
	//seriesSet := newSeriesSetFilter(remote.FromQueryResult(sortSeries, res.Results[0]), added)
	seriesSet := remote.FromQueryResult(sortSeries, res.Results[0])
	//fmt.Printf("[Promql] series set at: %v, warnings: %s\n", seriesSet.At(), seriesSet.Warnings())
	return seriesSet
}

// addExternalLabels adds matchers for each external label. External labels
// that already have a corresponding user-supplied matcher are skipped, as we
// assume that the user explicitly wants to select a different value for them.
// We return the new set of matchers, along with a map of labels for which
// matchers were added, so that these can later be removed from the result
// time series again.
func (q *Querier) addExternalLabels(ms []*labels.Matcher) ([]*labels.Matcher, labels.Labels) {
	el := make(labels.Labels, len(q.externalLabels))
	copy(el, q.externalLabels)

	// ms won't be sorted, so have to O(n^2) the search.
	for _, m := range ms {
		for i := 0; i < len(el); {
			if el[i].Name == m.Name {
				el = el[:i+copy(el[i:], el[i+1:])]
				continue
			}
			i++
		}
	}

	for _, l := range el {
		m, err := labels.NewMatcher(labels.MatchEqual, l.Name, l.Value)
		if err != nil {
			panic(err)
		}
		ms = append(ms, m)
	}
	return ms, el
}

// LabelValues implements storage.Querier and is a noop.
func (q *Querier) LabelValues(string, ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return []string{"code", "container", "endpoint"}, nil, nil
}

// LabelNames implements storage.Querier and is a noop.
func (q *Querier) LabelNames(...*labels.Matcher) ([]string, storage.Warnings, error) {
	return []string{"200"}, nil, nil
}

// Close implements storage.Querier and is a noop.
func (q *Querier) Close() error {
	return nil
}

func newSeriesSetFilter(ss storage.SeriesSet, toFilter labels.Labels) storage.SeriesSet {
	return &seriesSetFilter{
		SeriesSet: ss,
		toFilter:  toFilter,
	}
}

type seriesSetFilter struct {
	storage.SeriesSet
	toFilter labels.Labels
	querier  storage.Querier
}

func (ssf *seriesSetFilter) GetQuerier() storage.Querier {
	return ssf.querier
}

func (ssf *seriesSetFilter) SetQuerier(querier storage.Querier) {
	ssf.querier = querier
}

func (ssf seriesSetFilter) At() storage.Series {
	return seriesFilter{
		Series:   ssf.SeriesSet.At(),
		toFilter: ssf.toFilter,
	}
}

type seriesFilter struct {
	storage.Series
	toFilter labels.Labels
}

func (sf seriesFilter) Labels() labels.Labels {
	label := sf.Series.Labels()
	for i, j := 0, 0; i < len(label) && j < len(sf.toFilter); {
		if label[i].Name < sf.toFilter[j].Name {
			i++
		} else if label[i].Name > sf.toFilter[j].Name {
			j++
		} else {
			label = label[:i+copy(label[i:], label[i+1:])]
			j++
		}
	}
	return label
}
