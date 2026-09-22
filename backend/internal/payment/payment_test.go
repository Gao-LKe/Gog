package payment

import (
	"context"
	"errors"
	"testing"

	"github.com/goblog/backend/internal/store"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCreatePaymentPersistsAttemptAndOutbox(t *testing.T) {
	db := paymentTestDB(t)
	buyer := store.User{DisplayName: "buyer", Status: "active"}
	if err := db.Create(&buyer).Error; err != nil {
		t.Fatal(err)
	}
	order := store.Order{OrderNo: "ORDER-001", BuyerUserID: buyer.ID, TotalAmountFen: 19900, Status: "pending_payment"}
	if err := db.Create(&order).Error; err != nil {
		t.Fatal(err)
	}

	service := NewService(db, nil)
	first, err := service.Create(context.Background(), buyer.ID, "payment-key-001", CreateRequest{OrderNo: order.OrderNo})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Create(context.Background(), buyer.ID, "payment-key-001", CreateRequest{OrderNo: order.OrderNo})
	if err != nil {
		t.Fatal(err)
	}
	if first.PaymentNo == "" || first.PaymentNo != second.PaymentNo || first.AmountFen != order.TotalAmountFen || first.Status != "pending" {
		t.Fatalf("unexpected payment results: first=%+v second=%+v", first, second)
	}

	var attempts, events int64
	db.Model(&store.PaymentAttempt{}).Count(&attempts)
	db.Model(&store.OutboxEvent{}).Count(&events)
	if attempts != 1 || events != 1 {
		t.Fatalf("attempts=%d events=%d, want one of each", attempts, events)
	}
}

func TestIdempotencyKeyCannotBeReusedForAnotherOrder(t *testing.T) {
	db := paymentTestDB(t)
	buyer := store.User{DisplayName: "buyer", Status: "active"}
	if err := db.Create(&buyer).Error; err != nil {
		t.Fatal(err)
	}
	firstOrder := store.Order{OrderNo: "ORDER-001", BuyerUserID: buyer.ID, TotalAmountFen: 100, Status: "pending_payment"}
	secondOrder := store.Order{OrderNo: "ORDER-002", BuyerUserID: buyer.ID, TotalAmountFen: 200, Status: "pending_payment"}
	if err := db.Create(&firstOrder).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&secondOrder).Error; err != nil {
		t.Fatal(err)
	}

	service := NewService(db, nil)
	if _, err := service.Create(context.Background(), buyer.ID, "shared-key", CreateRequest{OrderNo: firstOrder.OrderNo}); err != nil {
		t.Fatal(err)
	}
	_, err := service.Create(context.Background(), buyer.ID, "shared-key", CreateRequest{OrderNo: secondOrder.OrderNo})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error=%v, want ErrIdempotencyConflict", err)
	}
}

func paymentTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&store.User{}, &store.Order{}, &store.PaymentAttempt{}, &store.OutboxEvent{}); err != nil {
		t.Fatal(err)
	}
	return db
}
