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
	buyer := store.User{ID: 803113126182420001, DisplayName: "buyer", Status: "active"}
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
	listing := store.LicenseListing{SourceType: "official", Title: "测试激活码", UnitPriceFen: 19_900, Status: "active"}
	if err := db.Create(&listing).Error; err != nil {
		t.Fatal(err)
	}
	item := store.OrderItem{OrderID: order.ID, ListingID: listing.ID, SourceType: listing.SourceType, TitleSnapshot: listing.Title, UnitPriceFen: listing.UnitPriceFen, Quantity: 1, LineAmountFen: listing.UnitPriceFen, Status: "pending_payment"}
	if err := db.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	code := store.ActivationCode{ListingID: listing.ID, SecretCiphertext: []byte("ciphertext"), SecretFingerprint: []byte("01234567890123456789012345678901"), EncryptionKeyVersion: "test", Status: "reserved", ReservedOrderID: &order.ID, ReservedOrderItemID: &item.ID}
	if err := db.Create(&code).Error; err != nil {
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
	var storedCode store.ActivationCode
	var attempts, ledgerEntries, bindings int64
	db.First(&balance, "user_id = ?", buyer.ID)
	db.First(&storedOrder, "id = ?", order.ID)
	db.Model(&store.PaymentAttempt{}).Count(&attempts)
	db.Model(&store.BalanceTransaction{}).Count(&ledgerEntries)
	db.First(&storedCode, "id = ?", code.ID)
	db.Model(&store.OrderItemActivationCode{}).Count(&bindings)
	if balance.AvailableFen != 30_100 || storedOrder.Status != "paid" || storedOrder.PaidAt == nil || storedCode.Status != "sold" || attempts != 1 || ledgerEntries != 1 || bindings != 1 {
		t.Fatalf("balance=%d order=%+v code=%+v attempts=%d ledger=%d bindings=%d", balance.AvailableFen, storedOrder, storedCode, attempts, ledgerEntries, bindings)
	}
}

func TestBalancePaymentRejectsInsufficientFundsWithoutMutation(t *testing.T) {
	db := paymentTestDB(t)
	buyer := store.User{ID: 803113126182420002, DisplayName: "buyer", Status: "active"}
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
	listing := store.LicenseListing{SourceType: "official", Title: "测试激活码", UnitPriceFen: 101, Status: "active"}
	if err := db.Create(&listing).Error; err != nil {
		t.Fatal(err)
	}
	item := store.OrderItem{OrderID: order.ID, ListingID: listing.ID, SourceType: listing.SourceType, TitleSnapshot: listing.Title, UnitPriceFen: listing.UnitPriceFen, Quantity: 1, LineAmountFen: listing.UnitPriceFen, Status: "pending_payment"}
	if err := db.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	code := store.ActivationCode{ListingID: listing.ID, SecretCiphertext: []byte("ciphertext"), SecretFingerprint: []byte("11234567890123456789012345678901"), EncryptionKeyVersion: "test", Status: "reserved", ReservedOrderID: &order.ID, ReservedOrderItemID: &item.ID}
	if err := db.Create(&code).Error; err != nil {
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
	if err := db.AutoMigrate(&store.User{}, &store.UserBalance{}, &store.BalanceTransaction{}, &store.LicenseListing{}, &store.ActivationCode{}, &store.Order{}, &store.OrderItem{}, &store.OrderItemActivationCode{}, &store.PaymentAttempt{}); err != nil {
		t.Fatal(err)
	}
	return db
}
