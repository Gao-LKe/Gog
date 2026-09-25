package order

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/goblog/backend/internal/contracts"
	"github.com/goblog/backend/internal/store"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestProcessCreatesOneReservedOrderForRepeatedMessage(t *testing.T) {
	db, buyer, listing, code := orderTestData(t)
	service := NewService(db, nil, nil)
	event := contracts.OrderCreationRequestedEvent{RequestID: "request-001", BuyerID: buyer.ID, Items: []contracts.OrderCreationRequestedItem{{ListingID: listing.ID, Quantity: 1}}}
	if err := service.Process(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := service.Process(context.Background(), event); err != nil {
		t.Fatal(err)
	}

	var request store.OrderCreationRequest
	var order store.Order
	var reservedCode store.ActivationCode
	var orders int64
	if err := db.Preload("Order").First(&request, "request_id = ?", event.RequestID).Error; err != nil {
		t.Fatal(err)
	}
	if request.Status != "created" || request.Order == nil {
		t.Fatalf("unexpected request: %+v", request)
	}
	if err := db.First(&order, request.OrderID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&reservedCode, code.ID).Error; err != nil {
		t.Fatal(err)
	}
	db.Model(&store.Order{}).Count(&orders)
	if order.Status != "pending_payment" || reservedCode.Status != "reserved" || reservedCode.ReservedOrderID == nil || *reservedCode.ReservedOrderID != order.ID || orders != 1 {
		t.Fatalf("order=%+v code=%+v count=%d", order, reservedCode, orders)
	}
}

func TestExpirePendingReleasesOnlyExpiredReservation(t *testing.T) {
	db, buyer, listing, code := orderTestData(t)
	past := time.Now().UTC().Add(-time.Minute)
	order := store.Order{OrderNo: "ORDER-EXPIRED", BuyerUserID: buyer.ID, TotalAmountFen: listing.UnitPriceFen, Status: "pending_payment", ExpiresAt: &past}
	if err := db.Create(&order).Error; err != nil {
		t.Fatal(err)
	}
	item := store.OrderItem{OrderID: order.ID, ListingID: listing.ID, SourceType: listing.SourceType, TitleSnapshot: listing.Title, UnitPriceFen: listing.UnitPriceFen, Quantity: 1, LineAmountFen: listing.UnitPriceFen, Status: "pending_payment"}
	if err := db.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&code).Updates(map[string]any{"status": "reserved", "reserved_order_id": order.ID, "reserved_order_item_id": item.ID, "reserved_until": past}).Error; err != nil {
		t.Fatal(err)
	}

	if err := NewService(db, nil, nil).ExpirePending(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	var storedOrder store.Order
	var storedCode store.ActivationCode
	db.First(&storedOrder, order.ID)
	db.First(&storedCode, code.ID)
	if storedOrder.Status != "expired" || storedCode.Status != "available" || storedCode.ReservedOrderID != nil {
		t.Fatalf("order=%+v code=%+v", storedOrder, storedCode)
	}
}

func TestSubmitOnlyPublishesOrderCommand(t *testing.T) {
	producer := &recordingProducer{}
	service := NewService(nil, nil, producer)
	result, err := service.Submit(context.Background(), 7, "request-001", CreateRequest{Items: []Item{{ListingID: 9, Quantity: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "processing" || producer.topic != contracts.OrderCreationTopic || producer.key != "request-001" {
		t.Fatalf("result=%+v producer=%+v", result, producer)
	}
}

func TestProcessAtomicallyDecrementsExistingInventoryCacheOnce(t *testing.T) {
	db, buyer, listing, _ := orderTestData(t)
	cache := &recordingInventoryCache{values: map[string]string{inventoryCacheKey(listing.ID): "2"}}
	service := NewService(db, cache, nil)
	event := contracts.OrderCreationRequestedEvent{RequestID: "request-cache-001", BuyerID: buyer.ID, Items: []contracts.OrderCreationRequestedItem{{ListingID: listing.ID, Quantity: 1}}}
	if err := service.Process(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := service.Process(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if got := cache.values[inventoryCacheKey(listing.ID)]; got != "1" {
		t.Fatalf("cache availability=%q, want 1", got)
	}
}

func orderTestData(t *testing.T) (*gorm.DB, store.User, store.LicenseListing, store.ActivationCode) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&store.User{}, &store.LicenseListing{}, &store.ActivationCode{}, &store.OrderCreationRequest{}, &store.Order{}, &store.OrderItem{}); err != nil {
		t.Fatal(err)
	}
	buyer := store.User{ID: 9_000_000_000_000_000_001, DisplayName: "buyer", Status: "active"}
	listing := store.LicenseListing{SourceType: "official", Title: "测试码", UnitPriceFen: 19900, Status: "active"}
	if err := db.Create(&buyer).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&listing).Error; err != nil {
		t.Fatal(err)
	}
	code := store.ActivationCode{ListingID: listing.ID, SecretCiphertext: []byte("cipher"), SecretFingerprint: []byte("12345678901234567890123456789012"), EncryptionKeyVersion: "test", Status: "available"}
	if err := db.Create(&code).Error; err != nil {
		t.Fatal(err)
	}
	return db, buyer, listing, code
}

type recordingProducer struct {
	topic string
	key   string
	value []byte
}

func (p *recordingProducer) Publish(_ context.Context, topic, key string, value []byte) error {
	p.topic, p.key, p.value = topic, key, value
	return nil
}
func (*recordingProducer) Ping(context.Context) error { return nil }

type recordingInventoryCache struct{ values map[string]string }

func (c *recordingInventoryCache) Set(_ context.Context, key string, value any, _ time.Duration) error {
	c.values[key] = value.(string)
	return nil
}
func (c *recordingInventoryCache) Get(_ context.Context, key string) (string, error) {
	value, ok := c.values[key]
	if !ok {
		return "", errors.New("cache miss")
	}
	return value, nil
}
func (c *recordingInventoryCache) Del(_ context.Context, keys ...string) error {
	for _, key := range keys {
		delete(c.values, key)
	}
	return nil
}
func (c *recordingInventoryCache) DeleteIfEqual(_ context.Context, key, expected string) (bool, error) {
	if c.values[key] != expected {
		return false, nil
	}
	delete(c.values, key)
	return true, nil
}
func (c *recordingInventoryCache) SetIfNotExists(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	if _, exists := c.values[key]; exists {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}
func (c *recordingInventoryCache) DecrementIfExists(_ context.Context, key string, amount uint64) (bool, error) {
	value, exists := c.values[key]
	if !exists {
		return false, nil
	}
	available, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		delete(c.values, key)
		return false, nil
	}
	if amount >= available {
		c.values[key] = "0"
		return true, nil
	}
	c.values[key] = strconv.FormatUint(available-amount, 10)
	return true, nil
}
