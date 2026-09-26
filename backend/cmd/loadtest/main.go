// Command loadtest prepares isolated transaction data and executes repeatable
// order/payment pressure runs against a running local or test deployment.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goblog/backend/internal/auth"
	"github.com/goblog/backend/internal/config"
	"github.com/goblog/backend/internal/contracts"
	"github.com/goblog/backend/internal/monitoring"
	"github.com/goblog/backend/internal/order"
	"github.com/goblog/backend/internal/platform"
	"github.com/goblog/backend/internal/store"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

const (
	profileVersion     = 1
	loadtestPassword   = "LoadtestOnly!123"
	defaultHTTPTimeout = 10 * time.Second
)

type profile struct {
	Version   int           `json:"version"`
	CreatedAt time.Time     `json:"created_at"`
	ListingID uint64        `json:"listing_id"`
	CodeCount int           `json:"code_count"`
	Users     []profileUser `json:"users"`
}

// profileUser deliberately has no password or database connection settings.
// Its short-lived access token makes the profile sensitive and it must stay
// inside the ignored artifacts/loadtest directory.
type profileUser struct {
	UserID      uint64 `json:"user_id"`
	AccessToken string `json:"access_token"`
}

type runReport struct {
	RunID      string       `json:"run_id"`
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt time.Time    `json:"finished_at"`
	BaseURL    string       `json:"base_url"`
	Order      *phaseReport `json:"order,omitempty"`
	Payment    *phaseReport `json:"payment,omitempty"`
}

type phaseReport struct {
	Rate            int            `json:"target_rate_per_second"`
	Duration        time.Duration  `json:"target_duration"`
	Launched        int            `json:"launched"`
	Succeeded       int            `json:"succeeded"`
	Responses       map[string]int `json:"responses"`
	SuccessQPS      float64        `json:"success_qps"`
	P50MS           *float64       `json:"p50_ms,omitempty"`
	P95MS           *float64       `json:"p95_ms,omitempty"`
	P99MS           *float64       `json:"p99_ms,omitempty"`
	LastSuccessAt   *time.Time     `json:"last_success_at,omitempty"`
	Created         int            `json:"created,omitempty"`
	Rejected        int            `json:"rejected,omitempty"`
	PeakQueueLag    *int64         `json:"peak_queue_lag,omitempty"`
	DrainAfterInput *time.Duration `json:"drain_after_last_success,omitempty"`
}

type phaseAccumulator struct {
	mu            sync.Mutex
	launched      int
	succeeded     int
	responses     map[string]int
	latencies     []time.Duration
	lastSucceeded time.Time
}

func main() {
	action := flag.String("action", "", "操作：seed 或 run")
	profilePath := flag.String("profile", "", "压测资料文件路径")
	resultPath := flag.String("result", "", "压测结果 JSON 路径")
	baseURL := flag.String("base-url", "http://localhost:8081/api", "API 基地址")
	users := flag.Int("users", 100, "seed 时创建的测试用户数")
	codes := flag.Int("codes", 10000, "seed 时创建的可售激活码数")
	orderRate := flag.Int("order-rate", 0, "下单目标请求数/秒；0 表示跳过")
	orderDuration := flag.Duration("order-duration", time.Minute, "下单发送时长")
	paymentRate := flag.Int("payment-rate", 0, "支付目标请求数/秒；0 表示跳过")
	paymentDuration := flag.Duration("payment-duration", time.Minute, "支付发送时长")
	maxInflight := flag.Int("max-inflight", 2000, "压测端允许的最大并发请求数")
	drainTimeout := flag.Duration("drain-timeout", 5*time.Minute, "订单队列恢复等待上限")
	flag.Parse()

	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	switch *action {
	case "seed":
		if *profilePath == "" {
			log.Fatal("seed requires -profile")
		}
		if err := seed(context.Background(), cfg, *profilePath, *users, *codes); err != nil {
			log.Fatal(err)
		}
	case "run":
		if *profilePath == "" || *resultPath == "" {
			log.Fatal("run requires -profile and -result")
		}
		options := runOptions{
			profilePath: *profilePath, resultPath: *resultPath, baseURL: *baseURL,
			orderRate: *orderRate, orderDuration: *orderDuration,
			paymentRate: *paymentRate, paymentDuration: *paymentDuration,
			maxInflight: *maxInflight, drainTimeout: *drainTimeout,
		}
		if err := run(context.Background(), cfg, options); err != nil {
			log.Fatal(err)
		}
	default:
		flag.Usage()
		log.Fatal("-action must be seed or run")
	}
}

func seed(ctx context.Context, cfg config.Config, outputPath string, userCount, codeCount int) error {
	if userCount <= 0 || codeCount <= 0 {
		return errors.New("users and codes must be positive")
	}
	clients, err := platform.Open(cfg.MySQLDSN, cfg.RedisAddr, cfg.KafkaBroker)
	if err != nil {
		return fmt.Errorf("open test dependencies: %w", err)
	}
	defer clients.Close()
	if err := store.Migrate(clients.Gorm); err != nil {
		return fmt.Errorf("migrate test database: %w", err)
	}

	runID, err := randomID("load")
	if err != nil {
		return err
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(loadtestPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash load-test password: %w", err)
	}
	ids, err := auth.NewSnowflake(cfg.SnowflakeNodeID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	seededUsers := make([]store.User, 0, userCount)
	listing := store.LicenseListing{SourceType: "official", Title: "压测商品-" + runID, UnitPriceFen: 1, Status: "active"}

	if err := clients.Gorm.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&listing).Error; err != nil {
			return err
		}
		for index := 0; index < userCount; index++ {
			userID, err := ids.NextID()
			if err != nil {
				return err
			}
			email := fmt.Sprintf("load-%s-%04d@example.test", runID, index)
			user := store.User{ID: userID, DisplayName: "压测用户", Email: &email, EmailVerifiedAt: &now, PasswordHash: passwordHash, Role: auth.RoleUser, Status: "active"}
			if err := tx.Create(&user).Error; err != nil {
				return err
			}
			access := store.UserAccess{UserID: user.ID, CanBuy: true, CanChat: true, CanSell: true, CanHandleTicket: true, CanManageUser: true, CanManageSystem: true}
			if err := tx.Create(&access).Error; err != nil {
				return err
			}
			seededUsers = append(seededUsers, user)
		}

		codes := make([]store.ActivationCode, 0, min(codeCount, 500))
		for index := 0; index < codeCount; index++ {
			fingerprint := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", runID, index)))
			codes = append(codes, store.ActivationCode{
				ListingID: listing.ID, SecretCiphertext: []byte("loadtest-ciphertext"), SecretFingerprint: fingerprint[:],
				EncryptionKeyVersion: "loadtest", Status: "available",
			})
			if len(codes) == cap(codes) {
				if err := tx.CreateInBatches(codes, len(codes)).Error; err != nil {
					return err
				}
				codes = codes[:0]
			}
		}
		if len(codes) > 0 {
			return tx.CreateInBatches(codes, len(codes)).Error
		}
		return nil
	}); err != nil {
		return fmt.Errorf("seed load-test data: %w", err)
	}

	service := auth.NewService(clients.Gorm, clients.Redis, clients.RedisAtomic, auth.NewEmailSender(auth.EmailConfig{Mode: cfg.EmailMode}), auth.ServiceConfig{
		Secret: cfg.AuthSecret, Issuer: cfg.AuthIssuer, Audience: cfg.AuthAudience,
		AccessTokenTTL: cfg.AccessTokenTTL, RefreshTokenTTL: cfg.RefreshTokenTTL, EmailCodeTTL: cfg.EmailCodeTTL, SnowflakeNodeID: cfg.SnowflakeNodeID,
	})
	result := profile{Version: profileVersion, CreatedAt: now, ListingID: listing.ID, CodeCount: codeCount, Users: make([]profileUser, 0, len(seededUsers))}
	for _, user := range seededUsers {
		if user.Email == nil {
			return errors.New("seeded user has no email")
		}
		pair, err := service.LoginByPassword(ctx, *user.Email, loadtestPassword, auth.DevicePC)
		if err != nil {
			return fmt.Errorf("create access token for load user %d: %w", user.ID, err)
		}
		result.Users = append(result.Users, profileUser{UserID: user.ID, AccessToken: pair.AccessToken})
	}
	if err := writeJSON(outputPath, result, 0o600); err != nil {
		return fmt.Errorf("write sensitive load-test profile: %w", err)
	}
	log.Printf("load-test seed created listing=%d users=%d codes=%d profile=%s", listing.ID, userCount, codeCount, outputPath)
	return nil
}

type runOptions struct {
	profilePath                    string
	resultPath                     string
	baseURL                        string
	orderRate, paymentRate         int
	orderDuration, paymentDuration time.Duration
	maxInflight                    int
	drainTimeout                   time.Duration
}

func run(ctx context.Context, cfg config.Config, options runOptions) error {
	if options.orderRate <= 0 && options.paymentRate <= 0 {
		return errors.New("at least one of -order-rate or -payment-rate must be positive")
	}
	if options.maxInflight <= 0 || options.drainTimeout <= 0 {
		return errors.New("max-inflight and drain-timeout must be positive")
	}
	if options.orderRate > 0 && options.orderDuration <= 0 || options.paymentRate > 0 && options.paymentDuration <= 0 {
		return errors.New("selected phase duration must be positive")
	}
	baseURL, err := normalizeBaseURL(options.baseURL)
	if err != nil {
		return err
	}
	loaded, err := readProfile(options.profilePath)
	if err != nil {
		return err
	}
	if options.orderRate > 0 && requestedCount(options.orderRate, options.orderDuration) > loaded.CodeCount {
		return fmt.Errorf("order run needs %d codes but profile contains %d", requestedCount(options.orderRate, options.orderDuration), loaded.CodeCount)
	}

	db, err := store.OpenMySQL(cfg.MySQLDSN)
	if err != nil {
		return fmt.Errorf("open database observer: %w", err)
	}
	sqlDB, err := db.DB()
	if err == nil {
		defer sqlDB.Close()
	}
	runID, err := randomID("run")
	if err != nil {
		return err
	}
	report := runReport{RunID: runID, StartedAt: time.Now().UTC(), BaseURL: baseURL}
	client := &http.Client{Timeout: defaultHTTPTimeout}
	var createdOrders []paymentWork

	if options.orderRate > 0 {
		observer := newLagObserver(cfg)
		observeCtx, stopObserver := context.WithCancel(ctx)
		observer.start(observeCtx)
		if err := waitForEmptyQueue(ctx, observer, 30*time.Second); err != nil {
			stopObserver()
			observer.wait()
			return err
		}

		phase, accepted := submitOrders(ctx, client, baseURL, loaded, runID, options.orderRate, options.orderDuration, options.maxInflight)
		created, rejected, drain, err := waitForOrderDrain(ctx, db, observer, runID, accepted, phase.LastSuccessAt, options.drainTimeout)
		stopObserver()
		observer.wait()
		if err != nil {
			return err
		}
		phase.Created, phase.Rejected = created, rejected
		phase.DrainAfterInput = drain
		phase.PeakQueueLag = observer.maxLagValue()
		report.Order = &phase
		createdOrders, err = findCreatedOrders(ctx, db, runID, loaded)
		if err != nil {
			return err
		}
		if len(createdOrders) != created {
			return fmt.Errorf("created order lookup mismatch: found %d, expected %d", len(createdOrders), created)
		}
	}

	if options.paymentRate > 0 {
		if len(createdOrders) == 0 {
			return errors.New("payment phase requires an order phase in the same run")
		}
		wanted := requestedCount(options.paymentRate, options.paymentDuration)
		if wanted > len(createdOrders) {
			return fmt.Errorf("payment run needs %d pending orders but only %d were created", wanted, len(createdOrders))
		}
		phase := submitPayments(ctx, client, baseURL, createdOrders[:wanted], runID, options.paymentRate, options.paymentDuration, options.maxInflight)
		report.Payment = &phase
	}

	report.FinishedAt = time.Now().UTC()
	if err := writeJSON(options.resultPath, report, 0o644); err != nil {
		return fmt.Errorf("write load-test result: %w", err)
	}
	log.Printf("load-test run completed run_id=%s result=%s", runID, options.resultPath)
	return nil
}

func submitOrders(ctx context.Context, client *http.Client, baseURL string, loaded profile, runID string, rate int, duration time.Duration, maxInflight int) (phaseReport, int) {
	payload, _ := json.Marshal(map[string]any{"items": []map[string]any{{"listing_id": loaded.ListingID, "quantity": 1}}})
	accumulator := newPhaseAccumulator()
	runAtRate(ctx, rate, duration, maxInflight, func(index int) {
		user := loaded.Users[index%len(loaded.Users)]
		requestID := fmt.Sprintf("%s-order-%08d", runID, index)
		start := time.Now()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/order-requests", strings.NewReader(string(payload)))
		if err != nil {
			accumulator.record("client_error", time.Since(start), false)
			return
		}
		request.Header.Set("Authorization", "Bearer "+user.AccessToken)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", requestID)
		response, err := client.Do(request)
		if err != nil {
			accumulator.record("transport_error", time.Since(start), false)
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		accumulator.record("http_"+strconv.Itoa(response.StatusCode), time.Since(start), response.StatusCode == http.StatusAccepted)
	})
	result := accumulator.report(rate, duration)
	return result, result.Succeeded
}

type paymentWork struct {
	orderNo string
	token   string
}

func submitPayments(ctx context.Context, client *http.Client, baseURL string, orders []paymentWork, runID string, rate int, duration time.Duration, maxInflight int) phaseReport {
	accumulator := newPhaseAccumulator()
	runAtRate(ctx, rate, duration, maxInflight, func(index int) {
		work := orders[index]
		body, _ := json.Marshal(map[string]string{"order_no": work.orderNo})
		start := time.Now()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/payments", strings.NewReader(string(body)))
		if err != nil {
			accumulator.record("client_error", time.Since(start), false)
			return
		}
		request.Header.Set("Authorization", "Bearer "+work.token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", fmt.Sprintf("%s-payment-%08d", runID, index))
		response, err := client.Do(request)
		if err != nil {
			accumulator.record("transport_error", time.Since(start), false)
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		accumulator.record("http_"+strconv.Itoa(response.StatusCode), time.Since(start), response.StatusCode == http.StatusOK)
	})
	return accumulator.report(rate, duration)
}

func runAtRate(ctx context.Context, rate int, duration time.Duration, maxInflight int, request func(int)) {
	if rate <= 0 || duration <= 0 {
		return
	}
	startedAt := time.Now()
	count := requestedCount(rate, duration)
	semaphore := make(chan struct{}, maxInflight)
	var workers sync.WaitGroup
	for index := 0; index < count; index++ {
		target := startedAt.Add(time.Duration(index) * time.Second / time.Duration(rate))
		if wait := time.Until(target); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				workers.Wait()
				return
			case <-timer.C:
			}
		}
		select {
		case <-ctx.Done():
			workers.Wait()
			return
		case semaphore <- struct{}{}:
		}
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			defer func() { <-semaphore }()
			request(index)
		}(index)
	}
	workers.Wait()
}

func waitForOrderDrain(ctx context.Context, db *gorm.DB, observer *lagObserver, runID string, accepted int, lastSuccess *time.Time, timeout time.Duration) (int, int, *time.Duration, error) {
	if accepted == 0 {
		return 0, 0, nil, errors.New("no order request was accepted")
	}
	deadline := time.Now().Add(timeout)
	for {
		created, rejected, err := terminalOrderCounts(ctx, db, runID)
		if err != nil {
			return 0, 0, nil, err
		}
		if created+rejected >= accepted && observer.isDrained() {
			var drain *time.Duration
			if lastSuccess != nil {
				elapsed := time.Since(*lastSuccess)
				drain = &elapsed
			}
			return created, rejected, drain, nil
		}
		if time.Now().After(deadline) {
			return 0, 0, nil, fmt.Errorf("order queue did not drain within %s: terminal=%d accepted=%d", timeout, created+rejected, accepted)
		}
		select {
		case <-ctx.Done():
			return 0, 0, nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func waitForEmptyQueue(ctx context.Context, observer *lagObserver, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if observer.isDrained() {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("order queue must be empty before a load-test run")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func terminalOrderCounts(ctx context.Context, db *gorm.DB, runID string) (int, int, error) {
	var rows []struct {
		Status string
		Count  int
	}
	err := db.WithContext(ctx).Model(&store.OrderCreationRequest{}).
		Select("status, COUNT(*) AS count").
		Where("request_id LIKE ? AND status IN ?", runID+"-order-%", []string{"created", "rejected"}).
		Group("status").Scan(&rows).Error
	if err != nil {
		return 0, 0, err
	}
	var created, rejected int
	for _, row := range rows {
		switch row.Status {
		case "created":
			created = row.Count
		case "rejected":
			rejected = row.Count
		}
	}
	return created, rejected, nil
}

func findCreatedOrders(ctx context.Context, db *gorm.DB, runID string, loaded profile) ([]paymentWork, error) {
	tokens := make(map[uint64]string, len(loaded.Users))
	for _, user := range loaded.Users {
		tokens[user.UserID] = user.AccessToken
	}
	var rows []struct {
		BuyerUserID uint64
		OrderNo     string
	}
	err := db.WithContext(ctx).Table("order_creation_requests").
		Select("order_creation_requests.buyer_user_id, orders.order_no").
		Joins("JOIN orders ON orders.id = order_creation_requests.order_id").
		Where("order_creation_requests.request_id LIKE ? AND order_creation_requests.status = ?", runID+"-order-%", "created").
		Order("order_creation_requests.request_id").Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	works := make([]paymentWork, 0, len(rows))
	for _, row := range rows {
		token, ok := tokens[row.BuyerUserID]
		if !ok {
			return nil, fmt.Errorf("no access token for order buyer %d", row.BuyerUserID)
		}
		works = append(works, paymentWork{orderNo: row.OrderNo, token: token})
	}
	return works, nil
}

type lagObserver struct {
	cfg       config.Config
	mu        sync.Mutex
	maxLag    *int64
	latestLag *int64
	done      chan struct{}
}

func newLagObserver(cfg config.Config) *lagObserver {
	return &lagObserver{cfg: cfg, done: make(chan struct{})}
}

func (o *lagObserver) start(ctx context.Context) {
	go func() {
		defer close(o.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			o.sample(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (o *lagObserver) sample(ctx context.Context) {
	brokers := strings.Split(o.cfg.KafkaBroker, ",")
	snapshot, err := monitoring.SampleKafkaLag(ctx, brokers, contracts.OrderCreationTopic, order.ConsumerGroupID, 3*time.Hour)
	if err != nil || snapshot.State != "ok" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	latest := snapshot.Lag
	o.latestLag = &latest
	if o.maxLag == nil || snapshot.Lag > *o.maxLag {
		lag := snapshot.Lag
		o.maxLag = &lag
	}
}

func (o *lagObserver) wait() { <-o.done }

func (o *lagObserver) maxLagValue() *int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.maxLag == nil {
		return nil
	}
	value := *o.maxLag
	return &value
}

func (o *lagObserver) isDrained() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.latestLag != nil && *o.latestLag == 0
}

func newPhaseAccumulator() *phaseAccumulator {
	return &phaseAccumulator{responses: make(map[string]int)}
}

func (a *phaseAccumulator) record(response string, duration time.Duration, success bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.launched++
	a.responses[response]++
	if !success {
		return
	}
	a.succeeded++
	a.latencies = append(a.latencies, duration)
	now := time.Now().UTC()
	a.lastSucceeded = now
}

func (a *phaseAccumulator) report(rate int, duration time.Duration) phaseReport {
	a.mu.Lock()
	defer a.mu.Unlock()
	result := phaseReport{Rate: rate, Duration: duration, Launched: a.launched, Succeeded: a.succeeded, Responses: a.responses}
	if duration > 0 {
		result.SuccessQPS = float64(a.succeeded) / duration.Seconds()
	}
	if !a.lastSucceeded.IsZero() {
		value := a.lastSucceeded
		result.LastSuccessAt = &value
	}
	result.P50MS = percentileMillis(a.latencies, 0.50)
	result.P95MS = percentileMillis(a.latencies, 0.95)
	result.P99MS = percentileMillis(a.latencies, 0.99)
	return result
}

func percentileMillis(values []time.Duration, quantile float64) *float64 {
	if len(values) == 0 || quantile <= 0 || quantile > 1 {
		return nil
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(float64(len(ordered))*quantile)) - 1
	if index < 0 {
		index = 0
	}
	value := float64(ordered[index]) / float64(time.Millisecond)
	return &value
}

func requestedCount(rate int, duration time.Duration) int {
	if rate <= 0 || duration <= 0 {
		return 0
	}
	seconds := duration.Seconds()
	if seconds > float64(math.MaxInt)/float64(rate) {
		return math.MaxInt
	}
	return int(math.Floor(float64(rate) * seconds))
}

func normalizeBaseURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("base-url must be an absolute HTTP URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("base-url must use http or https")
	}
	return strings.TrimRight(raw, "/"), nil
}

func readProfile(path string) (profile, error) {
	var loaded profile
	contents, err := os.ReadFile(path)
	if err != nil {
		return loaded, err
	}
	if err := json.Unmarshal(contents, &loaded); err != nil {
		return loaded, err
	}
	if loaded.Version != profileVersion || loaded.ListingID == 0 || loaded.CodeCount <= 0 || len(loaded.Users) == 0 {
		return loaded, errors.New("invalid load-test profile")
	}
	for _, user := range loaded.Users {
		if user.UserID == 0 || user.AccessToken == "" {
			return loaded, errors.New("load-test profile contains an incomplete user")
		}
	}
	return loaded, nil
}

func writeJSON(path string, value any, mode os.FileMode) error {
	if path == "" {
		return errors.New("output path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	contents, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(contents)
	return err
}

func randomID(prefix string) (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(bytes), nil
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
