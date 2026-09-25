package store

import (
	"time"

	"gorm.io/gorm"
)

const mockEventID = "00000000-0000-0000-0000-000000000001"

// SeedMockData inserts one complete, non-production transaction graph.
func SeedMockData(db *gorm.DB) error {
	var existing OutboxEvent
	result := db.Where("event_id = ?", mockEventID).Limit(1).Find(&existing)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return nil
	}

	return db.Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC().Truncate(time.Millisecond)
		sellerPhone := "13800000001"
		buyerPhone := "13800000002"
		sellerEmail := "seller@example.test"
		buyerEmail := "buyer@example.test"
		seller := User{ID: 803113126182400001, DisplayName: "寄售卖家", Phone: &sellerPhone, PhoneVerifiedAt: &now, Email: &sellerEmail, Status: "active"}
		buyer := User{ID: 803113126182400002, DisplayName: "购买用户", Phone: &buyerPhone, PhoneVerifiedAt: &now, Email: &buyerEmail, Status: "active"}
		if err := tx.Create(&seller).Error; err != nil {
			return err
		}
		if err := tx.Create(&buyer).Error; err != nil {
			return err
		}
		account := ExternalAccount{UserID: seller.ID, Provider: "mock-oauth", ProviderSubject: "seller-001", LinkedAt: now}
		if err := tx.Create(&account).Error; err != nil {
			return err
		}
		if err := tx.Create(&UserAccess{UserID: seller.ID, CanBuy: true, CanChat: true, CanSell: true, CanHandleTicket: true, CanManageUser: true, CanManageSystem: true}).Error; err != nil {
			return err
		}
		if err := tx.Create(&UserAccess{UserID: buyer.ID, CanBuy: true, CanChat: true, CanSell: true, CanHandleTicket: true, CanManageUser: true, CanManageSystem: true}).Error; err != nil {
			return err
		}

		product := SoftwareProduct{Name: "Mock OS Pro", Publisher: "Mock Studio", Description: "用于数据库测试的软件", Status: "active"}
		if err := tx.Create(&product).Error; err != nil {
			return err
		}
		official := LicenseListing{SoftwareProductID: &product.ID, SourceType: "official", Title: "Mock OS Pro 官方激活码", UnitPriceFen: 19900, Status: "active"}
		consignment := LicenseListing{SoftwareProductID: &product.ID, SellerUserID: &seller.ID, SourceType: "consignment", Title: "Mock OS Pro 寄售激活码", UnitPriceFen: 15900, Status: "active"}
		if err := tx.Create(&official).Error; err != nil {
			return err
		}
		if err := tx.Create(&consignment).Error; err != nil {
			return err
		}
		officialCode := ActivationCode{ListingID: official.ID, SecretCiphertext: []byte("mock-ciphertext-official"), SecretFingerprint: []byte("12345678901234567890123456789001"), EncryptionKeyVersion: "mock-v1", Status: "available"}
		consignmentCode := ActivationCode{ListingID: consignment.ID, SecretCiphertext: []byte("mock-ciphertext-consignment"), SecretFingerprint: []byte("12345678901234567890123456789002"), EncryptionKeyVersion: "mock-v1", Status: "available"}
		if err := tx.Create(&officialCode).Error; err != nil {
			return err
		}
		if err := tx.Create(&consignmentCode).Error; err != nil {
			return err
		}
		verification := ActivationCodeVerification{ActivationCodeID: consignmentCode.ID, VerifierUserID: &seller.ID, Result: "approved", Note: "Mock 检验通过", VerifiedAt: now}
		if err := tx.Create(&verification).Error; err != nil {
			return err
		}
		cart := ShoppingCartItem{UserID: buyer.ID, ListingID: official.ID, Quantity: 1}
		if err := tx.Create(&cart).Error; err != nil {
			return err
		}

		paidAt := now
		order := Order{OrderNo: "MOCK-ORDER-0001", BuyerUserID: buyer.ID, TotalAmountFen: 15900, Status: "paid", ExpiresAt: &now, PaidAt: &paidAt}
		if err := tx.Create(&order).Error; err != nil {
			return err
		}
		consignmentItem := OrderItem{OrderID: order.ID, ListingID: consignment.ID, SellerUserID: &seller.ID, SourceType: "consignment", TitleSnapshot: consignment.Title, UnitPriceFen: consignment.UnitPriceFen, Quantity: 1, LineAmountFen: consignment.UnitPriceFen, Status: "delivered"}
		if err := tx.Create(&consignmentItem).Error; err != nil {
			return err
		}
		transactionID := "4200000000000000001"
		payment := PaymentAttempt{PaymentNo: "MOCK-PAYMENT-0001", OrderID: order.ID, BuyerUserID: buyer.ID, WeChatOutTradeNo: "MOCK-WX-OUT-0001", WeChatTransactionID: &transactionID, AmountFen: order.TotalAmountFen, IdempotencyKey: "mock-payment-key-0001", Status: "succeeded", ExpiresAt: &now}
		if err := tx.Create(&payment).Error; err != nil {
			return err
		}
		callback := PaymentCallbackEvent{WeChatEventID: "mock-wechat-event-0001", PaymentAttemptID: payment.ID, RawPayload: []byte("mock-encrypted-callback"), ReceivedAt: now, ProcessedAt: &now}
		if err := tx.Create(&callback).Error; err != nil {
			return err
		}
		binding := OrderItemActivationCode{OrderItemID: consignmentItem.ID, ActivationCodeID: consignmentCode.ID, BoundAt: now}
		if err := tx.Create(&binding).Error; err != nil {
			return err
		}
		delivery := ActivationCodeDelivery{OrderItemActivationCodeID: binding.ID, AvailableAt: now, FirstViewedAt: &now}
		if err := tx.Create(&delivery).Error; err != nil {
			return err
		}
		settlement := SellerSettlement{SettlementNo: "MOCK-SETTLEMENT-0001", SellerUserID: seller.ID, AmountFen: 14310, Status: "pending", RequestedAt: now}
		if err := tx.Create(&settlement).Error; err != nil {
			return err
		}
		earning := SellerEarning{OrderItemID: consignmentItem.ID, SellerUserID: seller.ID, SettlementID: &settlement.ID, GrossAmountFen: 15900, PlatformFeeFen: 1590, NetAmountFen: 14310, Status: "settling"}
		if err := tx.Create(&earning).Error; err != nil {
			return err
		}
		outbox := OutboxEvent{EventID: mockEventID, Topic: "payment.lifecycle.v1", AggregateType: "order", AggregateID: order.OrderNo, Payload: []byte(`{"type":"payment.succeeded"}`), OccurredAt: now}
		if err := tx.Create(&outbox).Error; err != nil {
			return err
		}

		derivative := DerivativeListing{PublisherUserID: seller.ID, Title: "Mock 游戏道具信息", Category: "game-item", Description: "仅用于交流撮合的测试信息", Status: "published"}
		if err := tx.Create(&derivative).Error; err != nil {
			return err
		}
		conversation := Conversation{DerivativeListingID: derivative.ID, PublisherUserID: seller.ID, InitiatorUserID: buyer.ID}
		if err := tx.Create(&conversation).Error; err != nil {
			return err
		}
		message := ConversationMessage{ConversationID: conversation.ID, SenderUserID: buyer.ID, RecipientUserID: seller.ID, ClientMessageID: "00000000-0000-0000-0000-000000000021", Content: "你好，还在吗？"}
		return tx.Create(&message).Error
	})
}
