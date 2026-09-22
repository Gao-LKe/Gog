package payment

import (
	"context"
	"errors"
	"testing"

	"github.com/goblog/backend/internal/store"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestBalancePaymentDebitsOnceAndMarksOrderPaid(t *testing.T) {
	db := paymentTestDB(t)
	buyer := store.User{DisplayName: "buyer", Status: "active"}
	if err := db.Create(&buyer).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&store.UserBalance{UserID: buyer.ID, AvailableFen: 50_000}).Error; err != nil {
		t.Fatal(err)
	}
	order := store.Order{OrderNo: "ORDER-001", BuyerUserID: buyer.ID, TotalAmountFen: 19_900, Status: "pending_payment"}
	if err := db.Create(&order).Error; err != nil {
		t.Fatal(err)
	}

	service := NewService(db)
	first, err := service.Create(context.Background(), buyer.ID, "payment-key-001", CreateRequest{OrderNo: order.OrderNo})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Create(context.Background(), buyer.ID, "payment-key-001", CreateRequest{OrderNo: order.OrderNo})
	if err != nil {
		t.Fatal(err)
	}
	if first.PaymentNo == "" || first.PaymentNo != second.PaymentNo || first.AmountFen != order.TotalAmountFen || first.Status != "succeeded" {
		t.Fatalf("unexpected payment results: first=%+v second=%+v", first, second)
	}

	var balance store.UserBalance
	var storedOrder store.Order
	var attempts, ledgerEntries, events int64
	db.First(&balance, "user_id = ?", buyer.ID)
	db.First(&storedOrder, "id = ?", order.ID)
	db.Model(&store.PaymentAttempt{}).Count(&attempts)
	db.Model(&store.BalanceTransaction{}).Count(&ledgerEntries)
	db.Model(&store.OutboxEvent{}).Count(&events)
	if balance.AvailableFen != 30_100 || storedOrder.Status != "paid" || storedOrder.PaidAt == nil || attempts != 1 || ledgerEntries != 1 || events != 1 {
		t.Fatalf("balance=%d order=%+v attempts=%d ledger=%d events=%d", balance.AvailableFen, storedOrder, attempts, ledgerEntries, events)
	}
}

func TestBalancePaymentRejectsInsufficientFundsWithoutMutation(t *testing.T) {
	db := paymentTestDB(t)
	buyer := store.User{DisplayName: "buyer", Status: "active"}
	if err := db.Create(&buyer).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&store.UserBalance{UserID: buyer.ID, AvailableFen: 100}).Error; err != nil {
		t.Fatal(err)
	}
	order := store.Order{OrderNo: "ORDER-001", BuyerUserID: buyer.ID, TotalAmountFen: 101, Status: "pending_payment"}
	if err := db.Create(&order).Error; err != nil {
		t.Fatal(err)
	}

	_, err := NewService(db).Create(context.Background(), buyer.ID, "payment-key-001", CreateRequest{OrderNo: order.OrderNo})
	if !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("error=%v, want ErrInsufficientBalance", err)
	}
	var balance store.UserBalance
	db.First(&balance, "user_id = ?", buyer.ID)
	if balance.AvailableFen != 100 {
		t.Fatalf("balance=%d, want 100", balance.AvailableFen)
	}
}

func paymentTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&store.User{}, &store.UserBalance{}, &store.BalanceTransaction{}, &store.Order{}, &store.PaymentAttempt{}, &store.OutboxEvent{}); err != nil {
		t.Fatal(err)
	}
	return db
}
