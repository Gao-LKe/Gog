package store

import (
	"os"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestAllTablesCRUD(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:marketplace-crud?mode=memory&cache=shared&_foreign_keys=on"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	runAllTablesCRUD(t, db)
}

func TestAllTablesCRUDMySQL(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN is not configured")
	}
	db, err := OpenMySQL(dsn)
	if err != nil {
		t.Fatal(err)
	}
	runAllTablesCRUD(t, db)
}

func runAllTablesCRUD(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := Migrate(db); err != nil {
		t.Fatal(err)
	}
	for _, model := range Models() {
		if !db.Migrator().HasTable(model) {
			t.Fatalf("table was not created for %T", model)
		}
	}
	if err := SeedMockData(db); err != nil {
		t.Fatal(err)
	}

	var seller, buyer User
	var sellerAccess, buyerAccess UserAccess
	var account ExternalAccount
	var product SoftwareProduct
	var listing LicenseListing
	var consignmentListing LicenseListing
	var code ActivationCode
	var consignmentCode ActivationCode
	var verification ActivationCodeVerification
	var cart ShoppingCartItem
	var order Order
	var item OrderItem
	var payment PaymentAttempt
	var callback PaymentCallbackEvent
	var binding OrderItemActivationCode
	var delivery ActivationCodeDelivery
	var settlement SellerSettlement
	var earning SellerEarning
	var event OutboxEvent
	var derivative DerivativeListing
	var conversation Conversation
	var message ConversationMessage

	readFirst(t, db, &seller, "display_name = ?", "寄售卖家")
	readFirst(t, db, &buyer, "display_name = ?", "购买用户")
	readFirst(t, db, &sellerAccess, "user_id = ?", seller.ID)
	readFirst(t, db, &buyerAccess, "user_id = ?", buyer.ID)
	readFirst(t, db, &account, "provider_subject = ?", "seller-001")
	readFirst(t, db, &product, "name = ?", "Mock OS Pro")
	readFirst(t, db, &listing, "title = ?", "Mock OS Pro 官方激活码")
	readFirst(t, db, &consignmentListing, "title = ?", "Mock OS Pro 寄售激活码")
	readFirst(t, db, &code, "status = ?", "available")
	readFirst(t, db, &consignmentCode, "id <> ?", code.ID)
	readFirst(t, db, &verification, "result = ?", "approved")
	readFirst(t, db, &cart, "quantity = ?", 1)
	readFirst(t, db, &order, "order_no = ?", "MOCK-ORDER-0001")
	readFirst(t, db, &item, "source_type = ?", "consignment")
	readFirst(t, db, &payment, "payment_no = ?", "MOCK-PAYMENT-0001")
	readFirst(t, db, &callback, "wechat_event_id = ?", "mock-wechat-event-0001")
	readFirst(t, db, &binding, "id > ?", 0)
	readFirst(t, db, &delivery, "order_item_activation_code_id = ?", binding.ID)
	readFirst(t, db, &settlement, "settlement_no = ?", "MOCK-SETTLEMENT-0001")
	readFirst(t, db, &earning, "settlement_id = ?", settlement.ID)
	readFirst(t, db, &event, "event_id = ?", mockEventID)
	readFirst(t, db, &derivative, "title = ?", "Mock 游戏道具信息")
	readFirst(t, db, &conversation, "derivative_listing_id = ?", derivative.ID)
	readFirst(t, db, &message, "conversation_id = ?", conversation.ID)

	updated := time.Now().UTC().Truncate(time.Millisecond)
	update(t, db, &seller, "display_name", "卖家已更新")
	update(t, db, &buyer, "display_name", "买家已更新")
	update(t, db, &account, "linked_at", updated)
	update(t, db, &product, "description", "更新后的软件说明")
	update(t, db, &listing, "description", "更新后的上架说明")
	update(t, db, &code, "status", "reserved")
	update(t, db, &verification, "note", "更新后的检验备注")
	update(t, db, &cart, "quantity", 2)
	update(t, db, &order, "status", "delivering")
	update(t, db, &item, "status", "delivering")
	update(t, db, &payment, "status", "confirmed")
	update(t, db, &callback, "processed_at", updated)
	update(t, db, &binding, "bound_at", updated)
	update(t, db, &delivery, "first_viewed_at", updated)
	update(t, db, &settlement, "status", "processing")
	update(t, db, &earning, "status", "paid")
	update(t, db, &event, "attempts", 1)
	update(t, db, &derivative, "status", "closed")
	update(t, db, &conversation, "updated_at", updated)
	update(t, db, &message, "content", "更新后的测试消息")

	deleteOne(t, db, &message)
	deleteOne(t, db, &conversation)
	deleteOne(t, db, &derivative)
	deleteOne(t, db, &event)
	deleteOne(t, db, &delivery)
	deleteOne(t, db, &binding)
	deleteOne(t, db, &callback)
	deleteOne(t, db, &payment)
	deleteOne(t, db, &earning)
	deleteOne(t, db, &settlement)
	deleteOne(t, db, &cart)
	deleteOne(t, db, &verification)
	deleteOne(t, db, &item)
	deleteOne(t, db, &order)
	deleteOne(t, db, &consignmentCode)
	deleteOne(t, db, &code)
	deleteOne(t, db, &consignmentListing)
	deleteOne(t, db, &listing)
	deleteOne(t, db, &product)
	deleteOne(t, db, &account)
	deleteOne(t, db, &buyerAccess)
	deleteOne(t, db, &sellerAccess)
	deleteOne(t, db, &buyer)
	deleteOne(t, db, &seller)
}

func readFirst(t *testing.T, db *gorm.DB, destination any, query string, args ...any) {
	t.Helper()
	if err := db.Where(query, args...).First(destination).Error; err != nil {
		t.Fatal(err)
	}
}

func update(t *testing.T, db *gorm.DB, model any, column string, value any) {
	t.Helper()
	if result := db.Model(model).Update(column, value); result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("update %T failed: rows=%d err=%v", model, result.RowsAffected, result.Error)
	}
}

func deleteOne(t *testing.T, db *gorm.DB, model any) {
	t.Helper()
	if result := db.Delete(model); result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("delete %T failed: rows=%d err=%v", model, result.RowsAffected, result.Error)
	}
}
