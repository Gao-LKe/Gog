package order

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/contracts"
	"github.com/goblog/backend/internal/platform"
	"github.com/goblog/backend/internal/store"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	paymentWindow     = 10 * time.Minute
	inventoryCacheTTL = time.Minute
	rebuildLockTTL    = 3 * time.Second
	ConsumerGroupID   = "goblog-order-creation-v1"
)

var (
	ErrInvalidRequest      = errors.New("invalid order request")
	ErrListingUnavailable  = errors.New("listing is unavailable")
	ErrOutOfStock          = errors.New("insufficient stock")
	ErrInventoryRefreshing = errors.New("inventory is refreshing")
)

type CreateRequest struct {
	Items []Item `json:"items" binding:"required,min=1"`
}

type Item struct {
	ListingID uint64 `json:"listing_id" binding:"required,gt=0"`
	Quantity  uint   `json:"quantity" binding:"required,gt=0"`
}

type RequestStatus struct {
	RequestID    string     `json:"request_id"`
	Status       string     `json:"status"`
	OrderNo      string     `json:"order_no,omitempty"`
	RejectReason string     `json:"reject_reason,omitempty"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
}

type Availability struct {
	ListingID uint64 `json:"listing_id"`
	Available uint64 `json:"available"`
}

type Service struct {
	db       *gorm.DB
	redis    inventoryCache
	kafka    platform.KafkaProducer
	observer Observer
}

// Observer receives aggregate outcomes without reading order identities.
type Observer interface {
	OrderSubmitted(result string, duration time.Duration)
	OrderProcessed(result string, duration time.Duration)
	OrderConsumer(result string)
	ConsumerAlive(alive bool)
}

type inventoryCache interface {
	Set(context.Context, string, any, time.Duration) error
	Get(context.Context, string) (string, error)
	Del(context.Context, ...string) error
	DeleteIfEqual(context.Context, string, string) (bool, error)
	SetIfNotExists(context.Context, string, string, time.Duration) (bool, error)
	DecrementIfExists(context.Context, string, uint64) (bool, error)
}

func NewService(db *gorm.DB, redis inventoryCache, kafka platform.KafkaProducer, observers ...Observer) *Service {
	service := &Service{db: db, redis: redis, kafka: kafka}
	if len(observers) > 0 {
		service.observer = observers[0]
	}
	return service
}

func (s *Service) Submit(ctx context.Context, buyerID uint64, requestID string, request CreateRequest) (RequestStatus, error) {
	start := time.Now()
	result := "invalid"
	defer func() {
		if s.observer != nil {
			s.observer.OrderSubmitted(result, time.Since(start))
		}
	}()
	if s.kafka == nil || buyerID == 0 || strings.TrimSpace(requestID) == "" {
		return RequestStatus{}, ErrInvalidRequest
	}
	items, err := normalizeItems(request.Items)
	if err != nil {
		return RequestStatus{}, err
	}
	payload, err := json.Marshal(contracts.OrderCreationRequestedEvent{RequestID: requestID, BuyerID: buyerID, Items: toEventItems(items)})
	if err != nil {
		return RequestStatus{}, err
	}
	result = "failed"
	if err := s.kafka.Publish(ctx, contracts.OrderCreationTopic, requestID, payload); err != nil {
		return RequestStatus{}, err
	}
	result = "accepted"
	return RequestStatus{RequestID: requestID, Status: "processing"}, nil
}

func (s *Service) Status(ctx context.Context, buyerID uint64, requestID string) (RequestStatus, error) {
	if s.db == nil || buyerID == 0 || strings.TrimSpace(requestID) == "" {
		return RequestStatus{}, ErrInvalidRequest
	}
	var request store.OrderCreationRequest
	err := s.db.WithContext(ctx).Preload("Order").Where("request_id = ? AND buyer_user_id = ?", requestID, buyerID).First(&request).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return RequestStatus{RequestID: requestID, Status: "processing"}, nil
	}
	if err != nil {
		return RequestStatus{}, err
	}
	status := RequestStatus{RequestID: request.RequestID, Status: request.Status, RejectReason: request.RejectReason}
	if request.Order != nil {
		status.OrderNo = request.Order.OrderNo
		status.ExpiresAt = request.Order.ExpiresAt
	}
	return status, nil
}

// Process creates the real order after Kafka has durably accepted the request.
// Business rejections are persisted and acknowledged; infrastructure errors are
// returned so the message remains eligible for redelivery.
func (s *Service) Process(ctx context.Context, event contracts.OrderCreationRequestedEvent) error {
	start := time.Now()
	result := "failed"
	defer func() {
		if s.observer != nil {
			s.observer.OrderProcessed(result, time.Since(start))
		}
	}()
	if s.db == nil || event.BuyerID == 0 || strings.TrimSpace(event.RequestID) == "" {
		return ErrInvalidRequest
	}
	items, err := normalizeEventItems(event.Items)
	if err != nil {
		err = s.reject(ctx, event, err.Error())
		if err == nil {
			result = "rejected"
		}
		return err
	}
	created, err := s.createOrder(ctx, event, items)
	if errors.Is(err, ErrListingUnavailable) || errors.Is(err, ErrOutOfStock) || errors.Is(err, ErrInvalidRequest) {
		err = s.reject(ctx, event, err.Error())
		if err == nil {
			result = "rejected"
		}
		return err
	}
	if err == nil {
		if created {
			result = "created"
		} else {
			result = "duplicate"
		}
		return nil
	}
	var existing store.OrderCreationRequest
	if lookupErr := s.db.WithContext(ctx).Where("request_id = ?", event.RequestID).First(&existing).Error; lookupErr == nil {
		result = "duplicate"
		return nil
	}
	return err
}

func (s *Service) createOrder(ctx context.Context, event contracts.OrderCreationRequestedEvent, items []Item) (bool, error) {
	reservedByListing := make(map[uint64]uint64, len(items))
	created := false
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing store.OrderCreationRequest
		if err := tx.Where("request_id = ?", event.RequestID).First(&existing).Error; err == nil {
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		listings := make([]store.LicenseListing, 0, len(items))
		var total uint64
		for _, item := range items {
			var listing store.LicenseListing
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&listing, item.ListingID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrListingUnavailable
				}
				return err
			}
			if listing.Status != "active" || listing.UnitPriceFen > math.MaxUint64/uint64(item.Quantity) {
				return ErrListingUnavailable
			}
			amount := listing.UnitPriceFen * uint64(item.Quantity)
			if total > math.MaxUint64-amount {
				return ErrInvalidRequest
			}
			total += amount
			listings = append(listings, listing)
		}

		now := time.Now().UTC()
		expiresAt := now.Add(paymentWindow)
		orderNo, err := newReference("ord")
		if err != nil {
			return err
		}
		order := store.Order{OrderNo: orderNo, BuyerUserID: event.BuyerID, TotalAmountFen: total, Status: "pending_payment", ExpiresAt: &expiresAt}
		if err := tx.Create(&order).Error; err != nil {
			return err
		}

		for index, item := range items {
			listing := listings[index]
			orderItem := store.OrderItem{OrderID: order.ID, ListingID: listing.ID, SellerUserID: listing.SellerUserID, SourceType: listing.SourceType, TitleSnapshot: listing.Title, UnitPriceFen: listing.UnitPriceFen, Quantity: item.Quantity, LineAmountFen: listing.UnitPriceFen * uint64(item.Quantity), Status: "pending_payment"}
			if err := tx.Create(&orderItem).Error; err != nil {
				return err
			}
			var codes []store.ActivationCode
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("listing_id = ? AND status = ?", listing.ID, "available").Order("id").Limit(int(item.Quantity)).Find(&codes).Error; err != nil {
				return err
			}
			if len(codes) != int(item.Quantity) {
				return ErrOutOfStock
			}
			ids := make([]uint64, 0, len(codes))
			for _, code := range codes {
				ids = append(ids, code.ID)
			}
			result := tx.Model(&store.ActivationCode{}).Where("id IN ? AND status = ?", ids, "available").Updates(map[string]any{"status": "reserved", "reserved_order_id": order.ID, "reserved_order_item_id": orderItem.ID, "reserved_until": expiresAt})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != int64(len(ids)) {
				return ErrOutOfStock
			}
			reservedByListing[listing.ID] += uint64(item.Quantity)
		}

		request := store.OrderCreationRequest{RequestID: event.RequestID, BuyerUserID: event.BuyerID, IdempotencyKey: event.RequestID, Status: "created", OrderID: &order.ID}
		if err := tx.Create(&request).Error; err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return false, err
	}
	for listingID, quantity := range reservedByListing {
		// Redis is only a display cache: a cache failure cannot roll back an
		// already committed order reservation. Its short TTL bounds any stale view.
		if s.redis != nil {
			_, _ = s.redis.DecrementIfExists(ctx, inventoryCacheKey(listingID), quantity)
		}
	}
	return created, nil
}

func (s *Service) reject(ctx context.Context, event contracts.OrderCreationRequestedEvent, reason string) error {
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&store.OrderCreationRequest{RequestID: event.RequestID, BuyerUserID: event.BuyerID, IdempotencyKey: event.RequestID, Status: "rejected", RejectReason: reason}).Error
}

func (s *Service) ExpirePending(ctx context.Context, limit int) error {
	if s.db == nil || limit <= 0 {
		return ErrInvalidRequest
	}
	var orders []store.Order
	if err := s.db.WithContext(ctx).Where("status = ? AND expires_at <= ?", "pending_payment", time.Now().UTC()).Order("expires_at, id").Limit(limit).Find(&orders).Error; err != nil {
		return err
	}
	for _, order := range orders {
		listingIDs, err := s.expireOne(ctx, order.ID)
		if err != nil {
			return err
		}
		for _, listingID := range listingIDs {
			if s.redis != nil {
				_ = s.redis.Del(ctx, inventoryCacheKey(listingID))
			}
		}
	}
	return nil
}

func (s *Service) expireOne(ctx context.Context, orderID uint64) ([]uint64, error) {
	var listingIDs []uint64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var order store.Order
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&order, orderID).Error; err != nil {
			return err
		}
		if order.Status != "pending_payment" || order.ExpiresAt == nil || order.ExpiresAt.After(time.Now().UTC()) {
			return nil
		}
		result := tx.Model(&store.Order{}).Where("id = ? AND status = ?", order.ID, "pending_payment").Update("status", "expired")
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return nil
		}
		var codes []store.ActivationCode
		if err := tx.Where("reserved_order_id = ? AND status = ?", order.ID, "reserved").Find(&codes).Error; err != nil {
			return err
		}
		seen := make(map[uint64]struct{}, len(codes))
		for _, code := range codes {
			seen[code.ListingID] = struct{}{}
		}
		for listingID := range seen {
			listingIDs = append(listingIDs, listingID)
		}
		if err := tx.Model(&store.ActivationCode{}).Where("reserved_order_id = ? AND status = ?", order.ID, "reserved").Updates(map[string]any{"status": "available", "reserved_order_id": nil, "reserved_order_item_id": nil, "reserved_until": nil}).Error; err != nil {
			return err
		}
		return tx.Model(&store.OrderItem{}).Where("order_id = ? AND status = ?", order.ID, "pending_payment").Update("status", "expired").Error
	})
	return listingIDs, err
}

func (s *Service) Available(ctx context.Context, listingID uint64) (Availability, error) {
	if s.db == nil || listingID == 0 {
		return Availability{}, ErrInvalidRequest
	}
	key := inventoryCacheKey(listingID)
	if s.redis != nil {
		if raw, err := s.redis.Get(ctx, key); err == nil {
			if available, parseErr := strconv.ParseUint(raw, 10, 64); parseErr == nil {
				return Availability{ListingID: listingID, Available: available}, nil
			}
		}
		token, err := newReference("cache")
		if err != nil {
			return Availability{}, err
		}
		locked, err := s.redis.SetIfNotExists(ctx, inventoryRebuildLockKey(listingID), token, rebuildLockTTL)
		if err != nil {
			return Availability{}, err
		}
		if !locked {
			return Availability{}, ErrInventoryRefreshing
		}
		defer func() { _, _ = s.redis.DeleteIfEqual(ctx, inventoryRebuildLockKey(listingID), token) }()
	}
	var available int64
	if err := s.db.WithContext(ctx).Model(&store.ActivationCode{}).Where("listing_id = ? AND status = ?", listingID, "available").Count(&available).Error; err != nil {
		return Availability{}, err
	}
	result := Availability{ListingID: listingID, Available: uint64(available)}
	if s.redis != nil {
		if err := s.redis.Set(ctx, key, strconv.FormatUint(result.Available, 10), inventoryCacheTTL); err != nil {
			return Availability{}, err
		}
	}
	return result, nil
}

func (s *Service) Consume(ctx context.Context, rawBrokers string) error {
	if s.observer != nil {
		s.observer.ConsumerAlive(true)
		defer s.observer.ConsumerAlive(false)
	}
	brokers := splitBrokers(rawBrokers)
	if len(brokers) == 0 {
		return errors.New("kafka broker is not configured")
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: contracts.OrderCreationTopic, GroupID: ConsumerGroupID, MinBytes: 1, MaxBytes: 10e6, CommitInterval: 0})
	defer reader.Close()
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if s.observer != nil {
			s.observer.OrderConsumer("fetched")
		}
		var event contracts.OrderCreationRequestedEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			if s.observer != nil {
				s.observer.OrderConsumer("decode_failed")
			}
			return fmt.Errorf("decode order creation message: %w", err)
		}
		if err := s.Process(ctx, event); err != nil {
			if s.observer != nil {
				s.observer.OrderConsumer("process_failed")
			}
			return err
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			if s.observer != nil {
				s.observer.OrderConsumer("commit_failed")
			}
			return err
		}
		if s.observer != nil {
			s.observer.OrderConsumer("committed")
		}
	}
}

func RegisterRoutes(r *gin.RouterGroup, service *Service) {
	r.POST("/order-requests", func(c *gin.Context) {
		var request CreateRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			if service.observer != nil {
				service.observer.OrderSubmitted("invalid", 0)
			}
			c.JSON(400, gin.H{"error": "invalid order request"})
			return
		}
		user, ok := c.Get("auth.user")
		if !ok {
			c.JSON(401, gin.H{"error": "authentication required"})
			return
		}
		result, err := service.Submit(c.Request.Context(), user.(store.User).ID, c.GetHeader("Idempotency-Key"), request)
		if err != nil {
			status := 503
			if errors.Is(err, ErrInvalidRequest) {
				status = 400
			}
			c.JSON(status, gin.H{"error": err.Error()})
			return
		}
		c.JSON(202, result)
	})

	r.GET("/order-requests/:requestID", func(c *gin.Context) {
		user, ok := c.Get("auth.user")
		if !ok {
			c.JSON(401, gin.H{"error": "authentication required"})
			return
		}
		result, err := service.Status(c.Request.Context(), user.(store.User).ID, c.Param("requestID"))
		if err != nil {
			c.JSON(500, gin.H{"error": "read order request failed"})
			return
		}
		c.JSON(200, result)
	})

	r.GET("/listings/:listingID/availability", func(c *gin.Context) {
		listingID, err := strconv.ParseUint(c.Param("listingID"), 10, 64)
		if err != nil {
			c.JSON(400, gin.H{"error": "invalid listing id"})
			return
		}
		result, err := service.Available(c.Request.Context(), listingID)
		if err != nil {
			if errors.Is(err, ErrInventoryRefreshing) {
				c.JSON(202, gin.H{"status": "refreshing"})
				return
			}
			c.JSON(500, gin.H{"error": "read availability failed"})
			return
		}
		c.JSON(200, result)
	})
}

func normalizeItems(items []Item) ([]Item, error) {
	if len(items) == 0 {
		return nil, ErrInvalidRequest
	}
	merged := make(map[uint64]uint)
	for _, item := range items {
		if item.ListingID == 0 || item.Quantity == 0 || merged[item.ListingID] > ^uint(0)-item.Quantity {
			return nil, ErrInvalidRequest
		}
		merged[item.ListingID] += item.Quantity
	}
	result := make([]Item, 0, len(merged))
	for listingID, quantity := range merged {
		result = append(result, Item{ListingID: listingID, Quantity: quantity})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ListingID < result[j].ListingID })
	return result, nil
}

func normalizeEventItems(items []contracts.OrderCreationRequestedItem) ([]Item, error) {
	requestItems := make([]Item, 0, len(items))
	for _, item := range items {
		requestItems = append(requestItems, Item{ListingID: item.ListingID, Quantity: item.Quantity})
	}
	return normalizeItems(requestItems)
}

func toEventItems(items []Item) []contracts.OrderCreationRequestedItem {
	result := make([]contracts.OrderCreationRequestedItem, 0, len(items))
	for _, item := range items {
		result = append(result, contracts.OrderCreationRequestedItem{ListingID: item.ListingID, Quantity: item.Quantity})
	}
	return result
}

func newReference(prefix string) (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(bytes), nil
}

func inventoryCacheKey(listingID uint64) string {
	return "inventory:available:" + strconv.FormatUint(listingID, 10)
}
func inventoryRebuildLockKey(listingID uint64) string {
	return "inventory:rebuild-lock:" + strconv.FormatUint(listingID, 10)
}

func splitBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			result = append(result, broker)
		}
	}
	return result
}
