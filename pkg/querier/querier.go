package querier

import (
	"context"
	"flag"
	"net/http"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/health/grpc_health_v1"

	cortex_client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	cortex_validation "github.com/cortexproject/cortex/pkg/util/validation"

	"github.com/grafana/loki/pkg/ingester/client"
	"github.com/grafana/loki/pkg/iter"
	"github.com/grafana/loki/pkg/loghttp"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql"
	"github.com/grafana/loki/pkg/logql/marshal"
	"github.com/grafana/loki/pkg/storage"
	"github.com/grafana/loki/pkg/util/validation"
)

const (
	// How long the Tailer should wait - once there are no entries to read from ingesters -
	// before checking if a new entry is available (to avoid spinning the CPU in a continuous
	// check loop)
	tailerWaitEntryThrottle = time.Second / 2
)

var readinessProbeSuccess = []byte("Ready")

// Config for a querier.
type Config struct {
	QueryTimeout    time.Duration    `yaml:"query_timeout"`
	TailMaxDuration time.Duration    `yaml:"tail_max_duration"`
	ExtraQueryDelay time.Duration    `yaml:"extra_query_delay,omitempty"`
	Engine          logql.EngineOpts `yaml:"engine,omitempty"`
}

// RegisterFlags register flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.TailMaxDuration, "querier.tail-max-duration", 1*time.Hour, "Limit the duration for which live tailing request would be served")
	f.DurationVar(&cfg.QueryTimeout, "querier.query_timeout", 1*time.Minute, "Timeout when querying backends (ingesters or storage) during the execution of a query request")
	f.DurationVar(&cfg.ExtraQueryDelay, "distributor.extra-query-delay", 0, "Time to wait before sending more than the minimum successful query requests.")
}

// Querier handlers queries.
type Querier struct {
	cfg    Config
	ring   ring.ReadRing
	pool   *cortex_client.Pool
	store  storage.Store
	engine *logql.Engine
	limits *validation.Overrides
}

// New makes a new Querier.
func New(cfg Config, clientCfg client.Config, ring ring.ReadRing, store storage.Store, limits *validation.Overrides) (*Querier, error) {
	factory := func(addr string) (grpc_health_v1.HealthClient, error) {
		return client.New(clientCfg, addr)
	}

	return newQuerier(cfg, clientCfg, factory, ring, store, limits)
}

// newQuerier creates a new Querier and allows to pass a custom ingester client factory
// used for testing purposes
func newQuerier(cfg Config, clientCfg client.Config, clientFactory cortex_client.Factory, ring ring.ReadRing, store storage.Store, limits *validation.Overrides) (*Querier, error) {
	return &Querier{
		cfg:    cfg,
		ring:   ring,
		pool:   cortex_client.NewPool(clientCfg.PoolConfig, ring, clientFactory, util.Logger),
		store:  store,
		engine: logql.NewEngine(cfg.Engine),
		limits: limits,
	}, nil
}

type responseFromIngesters struct {
	addr     string
	response interface{}
}

// ReadinessHandler is used to indicate to k8s when the querier is ready.
// Returns 200 when the querier is ready, 500 otherwise.
func (q *Querier) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	_, err := q.ring.GetAll()
	if err != nil {
		http.Error(w, "Not ready: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(readinessProbeSuccess); err != nil {
		level.Error(util.Logger).Log("msg", "error writing success message", "error", err)
	}
}

// forAllIngesters runs f, in parallel, for all ingesters
// TODO taken from Cortex, see if we can refactor out an usable interface.
func (q *Querier) forAllIngesters(ctx context.Context, f func(logproto.QuerierClient) (interface{}, error)) ([]responseFromIngesters, error) {
	replicationSet, err := q.ring.GetAll()
	if err != nil {
		return nil, err
	}

	return q.forGivenIngesters(ctx, replicationSet, f)
}

// forGivenIngesters runs f, in parallel, for given ingesters
// TODO taken from Cortex, see if we can refactor out an usable interface.
func (q *Querier) forGivenIngesters(ctx context.Context, replicationSet ring.ReplicationSet, f func(logproto.QuerierClient) (interface{}, error)) ([]responseFromIngesters, error) {
	results, err := replicationSet.Do(ctx, q.cfg.ExtraQueryDelay, func(ingester *ring.IngesterDesc) (interface{}, error) {
		client, err := q.pool.GetClientFor(ingester.Addr)
		if err != nil {
			return nil, err
		}

		resp, err := f(client.(logproto.QuerierClient))
		if err != nil {
			return nil, err
		}

		return responseFromIngesters{ingester.Addr, resp}, nil
	})
	if err != nil {
		return nil, err
	}

	responses := make([]responseFromIngesters, 0, len(results))
	for _, result := range results {
		responses = append(responses, result.(responseFromIngesters))
	}

	return responses, err
}

// Select Implements logql.Querier which select logs via matchers and regex filters.
func (q *Querier) Select(ctx context.Context, params logql.SelectParams) (iter.EntryIterator, error) {
	err := q.validateQueryRequest(ctx, params.QueryRequest)
	if err != nil {
		return nil, err
	}

	ingesterIterators, err := q.queryIngesters(ctx, params)
	if err != nil {
		return nil, err
	}
	chunkStoreIterators, err := q.store.LazyQuery(ctx, params)
	if err != nil {
		return nil, err
	}
	iterators := append(ingesterIterators, chunkStoreIterators)
	return iter.NewHeapIterator(iterators, params.Direction), nil
}

func (q *Querier) queryIngesters(ctx context.Context, params logql.SelectParams) ([]iter.EntryIterator, error) {
	clients, err := q.forAllIngesters(ctx, func(client logproto.QuerierClient) (interface{}, error) {
		return client.Query(ctx, params.QueryRequest)
	})
	if err != nil {
		return nil, err
	}

	iterators := make([]iter.EntryIterator, len(clients))
	for i := range clients {
		iterators[i] = iter.NewQueryClientIterator(clients[i].response.(logproto.Querier_QueryClient), params.Direction)
	}
	return iterators, nil
}

// Label does the heavy lifting for a Label query.
func (q *Querier) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	resps, err := q.forAllIngesters(ctx, func(client logproto.QuerierClient) (interface{}, error) {
		return client.Label(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	from, through := model.TimeFromUnixNano(req.Start.UnixNano()), model.TimeFromUnixNano(req.End.UnixNano())
	var storeValues []string
	if req.Values {
		storeValues, err = q.store.LabelValuesForMetricName(ctx, userID, from, through, "logs", req.Name)
		if err != nil {
			return nil, err
		}
	} else {
		storeValues, err = q.store.LabelNamesForMetricName(ctx, userID, from, through, "logs")
		if err != nil {
			return nil, err
		}
	}

	results := make([][]string, 0, len(resps))
	for _, resp := range resps {
		results = append(results, resp.response.(*logproto.LabelResponse).Values)
	}
	results = append(results, storeValues)

	return &logproto.LabelResponse{
		Values: mergeLists(results...),
	}, nil
}

// Check implements the grpc healthcheck
func (*Querier) Check(_ context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func mergeLists(ss ...[]string) []string {
	switch len(ss) {
	case 0:
		return nil
	case 1:
		return ss[0]
	case 2:
		return mergePair(ss[0], ss[1])
	default:
		n := len(ss) / 2
		return mergePair(mergeLists(ss[:n]...), mergeLists(ss[n:]...))
	}
}

func mergePair(s1, s2 []string) []string {
	i, j := 0, 0
	result := make([]string, 0, len(s1)+len(s2))
	for i < len(s1) && j < len(s2) {
		if s1[i] < s2[j] {
			result = append(result, s1[i])
			i++
		} else if s1[i] > s2[j] {
			result = append(result, s2[j])
			j++
		} else {
			result = append(result, s1[i])
			i++
			j++
		}
	}
	for ; i < len(s1); i++ {
		result = append(result, s1[i])
	}
	for ; j < len(s2); j++ {
		result = append(result, s2[j])
	}
	return result
}

// Tail keeps getting matching logs from all ingesters for given query
func (q *Querier) Tail(ctx context.Context, req *logproto.TailRequest) (*Tailer, error) {
	histReq := logql.SelectParams{
		QueryRequest: &logproto.QueryRequest{
			Selector:  req.Query,
			Start:     req.Start,
			End:       time.Now(),
			Limit:     req.Limit,
			Direction: logproto.BACKWARD,
		},
	}

	err := q.validateQueryRequest(ctx, histReq.QueryRequest)
	if err != nil {
		return nil, err
	}

	// Enforce the query timeout except when tailing, otherwise the tailing
	// will be terminated once the query timeout is reached
	tailCtx := ctx
	queryCtx, cancelQuery := context.WithDeadline(ctx, time.Now().Add(q.cfg.QueryTimeout))
	defer cancelQuery()

	clients, err := q.forAllIngesters(ctx, func(client logproto.QuerierClient) (interface{}, error) {
		return client.Tail(tailCtx, req)
	})
	if err != nil {
		return nil, err
	}

	tailClients := make(map[string]logproto.Querier_TailClient)
	for i := range clients {
		tailClients[clients[i].addr] = clients[i].response.(logproto.Querier_TailClient)
	}

	histIterators, err := q.Select(queryCtx, histReq)
	if err != nil {
		return nil, err
	}

	reversedIterator, err := iter.NewEntryIteratorForward(histIterators, req.Limit, true)
	if err != nil {
		return nil, err
	}

	return newTailer(
		time.Duration(req.DelayFor)*time.Second,
		tailClients,
		reversedIterator,
		func(connectedIngestersAddr []string) (map[string]logproto.Querier_TailClient, error) {
			return q.tailDisconnectedIngesters(tailCtx, req, connectedIngestersAddr)
		},
		q.cfg.TailMaxDuration,
		tailerWaitEntryThrottle,
	), nil
}

// passed to tailer for (re)connecting to new or disconnected ingesters
func (q *Querier) tailDisconnectedIngesters(ctx context.Context, req *logproto.TailRequest, connectedIngestersAddr []string) (map[string]logproto.Querier_TailClient, error) {
	// Build a map to easily check if an ingester address is already connected
	connected := make(map[string]bool)
	for _, addr := range connectedIngestersAddr {
		connected[addr] = true
	}

	// Get the current replication set from the ring
	replicationSet, err := q.ring.GetAll()
	if err != nil {
		return nil, err
	}

	// Look for disconnected ingesters or new one we should (re)connect to
	reconnectIngesters := []ring.IngesterDesc{}

	for _, ingester := range replicationSet.Ingesters {
		if _, ok := connected[ingester.Addr]; ok {
			continue
		}

		// Skip ingesters which are leaving or joining the cluster
		if ingester.State != ring.ACTIVE {
			continue
		}

		reconnectIngesters = append(reconnectIngesters, ingester)
	}

	if len(reconnectIngesters) == 0 {
		return nil, nil
	}

	// Instance a tail client for each ingester to re(connect)
	reconnectClients, err := q.forGivenIngesters(ctx, ring.ReplicationSet{Ingesters: reconnectIngesters}, func(client logproto.QuerierClient) (interface{}, error) {
		return client.Tail(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	reconnectClientsMap := make(map[string]logproto.Querier_TailClient)
	for _, client := range reconnectClients {
		reconnectClientsMap[client.addr] = client.response.(logproto.Querier_TailClient)
	}

	return reconnectClientsMap, nil
}

// Series fetches any matching series for a list of matcher sets
func (q *Querier) Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	if err = q.validateQueryTimeRange(userID, &req.Start, &req.End); err != nil {
		return nil, err
	}

	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	return q.awaitSeries(ctx, req)

}

func (q *Querier) awaitSeries(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {

	// buffer the channels to the # of calls they're expecting su
	series := make(chan [][]logproto.SeriesIdentifier, 2)
	errs := make(chan error, 2)

	// fetch series from ingesters and store concurrently

	go func() {
		// fetch series identifiers from ingesters
		resps, err := q.forAllIngesters(ctx, func(client logproto.QuerierClient) (interface{}, error) {
			return client.Series(ctx, req)
		})
		if err != nil {
			errs <- err
			return
		}
		var acc [][]logproto.SeriesIdentifier
		for _, resp := range resps {
			acc = append(acc, resp.response.(*logproto.SeriesResponse).Series)
		}
		series <- acc
	}()

	go func() {
		storeValues, err := q.seriesForMatchers(ctx, req.Start, req.End, req.GetGroups())
		if err != nil {
			errs <- err
			return
		}
		series <- [][]logproto.SeriesIdentifier{storeValues}
	}()

	var sets [][]logproto.SeriesIdentifier
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			return nil, err
		case s := <-series:
			sets = append(sets, s...)
		}
	}

	deduped := make(map[string]logproto.SeriesIdentifier)
	for _, set := range sets {
		for _, s := range set {
			key := loghttp.LabelSet(s.Labels).String()
			if _, exists := deduped[key]; !exists {
				deduped[key] = s
			}
		}
	}

	response := &logproto.SeriesResponse{
		Series: make([]logproto.SeriesIdentifier, 0, len(deduped)),
	}

	for _, s := range deduped {
		response.Series = append(response.Series, s)
	}

	return response, nil
}

// seriesForMatchers fetches series from the store for each matcher set
// TODO: make efficient if/when the index supports labels so we don't have to read chunks
func (q *Querier) seriesForMatchers(
	ctx context.Context,
	from, through time.Time,
	groups []string,
) ([]logproto.SeriesIdentifier, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var results []logproto.SeriesIdentifier
	for _, group := range groups {
		iter, err := q.store.LazyQuery(ctx, logql.SelectParams{
			QueryRequest: &logproto.QueryRequest{
				Selector:  group,
				Limit:     1,
				Start:     from,
				End:       through,
				Direction: logproto.FORWARD,
			},
		})
		if err != nil {
			return nil, err
		}

		for iter.Next() {
			ls, err := marshal.NewLabelSet(iter.Labels())
			if err != nil {
				return nil, err
			}

			results = append(results, logproto.SeriesIdentifier{
				Labels: ls.Map(),
			})
		}
	}
	return results, nil

}

func (q *Querier) validateQueryRequest(ctx context.Context, req *logproto.QueryRequest) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	selector, err := logql.ParseLogSelector(req.Selector)
	if err != nil {
		return err
	}
	matchers := selector.Matchers()

	maxStreamMatchersPerQuery := q.limits.MaxStreamsMatchersPerQuery(userID)
	if len(matchers) > maxStreamMatchersPerQuery {
		return httpgrpc.Errorf(http.StatusBadRequest,
			"max streams matchers per query exceeded, matchers-count > limit (%d > %d)", len(matchers), maxStreamMatchersPerQuery)
	}

	return q.validateQueryTimeRange(userID, &req.Start, &req.End)
}

func (q *Querier) validateQueryTimeRange(userID string, from *time.Time, through *time.Time) error {
	if (*through).Before(*from) {
		return httpgrpc.Errorf(http.StatusBadRequest, "invalid query, through < from (%s < %s)", *through, *from)
	}

	maxQueryLength := q.limits.MaxQueryLength(userID)
	if maxQueryLength > 0 && (*through).Sub(*from) > maxQueryLength {
		return httpgrpc.Errorf(http.StatusBadRequest, cortex_validation.ErrQueryTooLong, (*through).Sub(*from), maxQueryLength)
	}

	return nil
}
