package store

import "time"

//===========================================
//      身份与会话
//===========================================

type User struct {
	ID              uint64  `gorm:"primaryKey"`
	DisplayName     string  `gorm:"size:80;not null"`
	Phone           *string `gorm:"size:32;uniqueIndex:uk_users_phone"`
	PhoneVerifiedAt *time.Time
	Email           *string `gorm:"size:254;uniqueIndex:uk_users_email"`
	EmailVerifiedAt *time.Time
	Status          string    `gorm:"size:24;not null;default:active;index:idx_users_status_created,priority:1"`
	CreatedAt       time.Time `gorm:"index:idx_users_status_created,priority:2"`
	UpdatedAt       time.Time
}

type ExternalAccount struct {
	ID              uint64    `gorm:"primaryKey"`
	UserID          uint64    `gorm:"not null;index"`
	Provider        string    `gorm:"size:32;not null;uniqueIndex:uk_external_provider_subject,priority:1"`
	ProviderSubject string    `gorm:"size:191;not null;uniqueIndex:uk_external_provider_subject,priority:2"`
	LinkedAt        time.Time `gorm:"not null"`
	User            User      `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:UserID"`
}

type UserSession struct {
	ID               string    `gorm:"size:36;primaryKey"`
	UserID           uint64    `gorm:"not null;index:idx_user_sessions_user_expiry,priority:1"`
	RefreshTokenHash []byte    `gorm:"type:varbinary(32);not null;uniqueIndex:uk_user_sessions_refresh_token"`
	ExpiresAt        time.Time `gorm:"not null;index:idx_user_sessions_user_expiry,priority:2"`
	RevokedAt        *time.Time
	CreatedAt        time.Time `gorm:"not null"`
	User             User      `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:UserID"`
}

//===========================================
//      商品与激活码库存
//===========================================

type SoftwareProduct struct {
	ID          uint64    `gorm:"primaryKey"`
	Name        string    `gorm:"size:160;not null"`
	Publisher   string    `gorm:"size:160"`
	Description string    `gorm:"type:text"`
	Status      string    `gorm:"size:24;not null;default:active;index:idx_software_products_status_created,priority:1"`
	CreatedAt   time.Time `gorm:"index:idx_software_products_status_created,priority:2"`
	UpdatedAt   time.Time
}

type LicenseListing struct {
	ID                uint64    `gorm:"primaryKey"`
	SoftwareProductID *uint64   `gorm:"index:idx_listings_product_status_created,priority:1"`
	SellerUserID      *uint64   `gorm:"index:idx_listings_seller_status_created,priority:1"`
	SourceType        string    `gorm:"size:24;not null"`
	Title             string    `gorm:"size:200;not null"`
	Description       string    `gorm:"type:text"`
	UnitPriceFen      uint64    `gorm:"not null"`
	Status            string    `gorm:"size:32;not null;default:draft;index:idx_listings_status_created,priority:1;index:idx_listings_seller_status_created,priority:2;index:idx_listings_product_status_created,priority:2"`
	CreatedAt         time.Time `gorm:"index:idx_listings_status_created,priority:2;index:idx_listings_seller_status_created,priority:3;index:idx_listings_product_status_created,priority:3"`
	UpdatedAt         time.Time
	SoftwareProduct   *SoftwareProduct `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:SoftwareProductID"`
	Seller            *User            `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:SellerUserID"`
}

type ActivationCode struct {
	ID                   uint64 `gorm:"primaryKey"`
	ListingID            uint64 `gorm:"not null;index:idx_activation_codes_listing_status,priority:1"`
	SecretCiphertext     []byte `gorm:"type:mediumblob;not null"`
	SecretFingerprint    []byte `gorm:"type:varbinary(32);not null;uniqueIndex:uk_activation_codes_fingerprint"`
	EncryptionKeyVersion string `gorm:"size:32;not null"`
	Status               string `gorm:"size:32;not null;default:submitted;index:idx_activation_codes_listing_status,priority:2"`
	CreatedAt            time.Time
	UpdatedAt            time.Time
	Listing              LicenseListing `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:ListingID"`
}

type ActivationCodeVerification struct {
	ID               uint64 `gorm:"primaryKey"`
	ActivationCodeID uint64 `gorm:"not null;index:idx_code_verifications_code_time,priority:1"`
	VerifierUserID   *uint64
	Result           string         `gorm:"size:24;not null"`
	Note             string         `gorm:"size:1000"`
	VerifiedAt       time.Time      `gorm:"not null;index:idx_code_verifications_code_time,priority:2"`
	ActivationCode   ActivationCode `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:ActivationCodeID"`
	Verifier         *User          `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:VerifierUserID"`
}

//===========================================
//      订单、支付、交付与结算
//===========================================

type ShoppingCartItem struct {
	ID        uint64 `gorm:"primaryKey"`
	UserID    uint64 `gorm:"not null;uniqueIndex:uk_cart_user_listing,priority:1;index:idx_cart_user_updated,priority:1"`
	ListingID uint64 `gorm:"not null;uniqueIndex:uk_cart_user_listing,priority:2"`
	Quantity  uint   `gorm:"not null"`
	CreatedAt time.Time
	UpdatedAt time.Time      `gorm:"index:idx_cart_user_updated,priority:2"`
	User      User           `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:CASCADE;foreignKey:UserID"`
	Listing   LicenseListing `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:CASCADE;foreignKey:ListingID"`
}

type Order struct {
	ID             uint64     `gorm:"primaryKey"`
	OrderNo        string     `gorm:"size:40;not null;uniqueIndex:uk_orders_order_no"`
	BuyerUserID    uint64     `gorm:"not null;index:idx_orders_buyer_created,priority:1"`
	TotalAmountFen uint64     `gorm:"not null"`
	Status         string     `gorm:"size:32;not null;default:pending_payment;index:idx_orders_status_expiry,priority:1"`
	ExpiresAt      *time.Time `gorm:"index:idx_orders_status_expiry,priority:2"`
	PaidAt         *time.Time
	CreatedAt      time.Time `gorm:"index:idx_orders_buyer_created,priority:2"`
	UpdatedAt      time.Time
	Buyer          User `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:BuyerUserID"`
}

type OrderItem struct {
	ID            uint64    `gorm:"primaryKey"`
	OrderID       uint64    `gorm:"not null;index"`
	ListingID     uint64    `gorm:"not null;index"`
	SellerUserID  *uint64   `gorm:"index:idx_order_items_seller_status_created,priority:1"`
	SourceType    string    `gorm:"size:24;not null"`
	TitleSnapshot string    `gorm:"size:200;not null"`
	UnitPriceFen  uint64    `gorm:"not null"`
	Quantity      uint      `gorm:"not null"`
	LineAmountFen uint64    `gorm:"not null"`
	Status        string    `gorm:"size:32;not null;default:pending_payment;index:idx_order_items_seller_status_created,priority:2"`
	CreatedAt     time.Time `gorm:"index:idx_order_items_seller_status_created,priority:3"`
	UpdatedAt     time.Time
	Order         Order          `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:OrderID"`
	Listing       LicenseListing `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:ListingID"`
	Seller        *User          `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:SellerUserID"`
}

type PaymentAttempt struct {
	ID                  uint64  `gorm:"primaryKey"`
	PaymentNo           string  `gorm:"size:40;not null;uniqueIndex:uk_payment_attempts_payment_no"`
	OrderID             uint64  `gorm:"not null;index"`
	BuyerUserID         uint64  `gorm:"not null;uniqueIndex:uk_payment_attempts_buyer_idempotency,priority:1"`
	WeChatOutTradeNo    string  `gorm:"column:wechat_out_trade_no;size:64;not null;uniqueIndex:uk_payment_attempts_wechat_out_trade_no"`
	WeChatTransactionID *string `gorm:"column:wechat_transaction_id;size:64;uniqueIndex:uk_payment_attempts_wechat_transaction"`
	AmountFen           uint64  `gorm:"not null"`
	IdempotencyKey      string  `gorm:"size:128;not null;uniqueIndex:uk_payment_attempts_buyer_idempotency,priority:2"`
	Status              string  `gorm:"size:32;not null;default:created"`
	ExpiresAt           *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
	Order               Order `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:OrderID"`
	Buyer               User  `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:BuyerUserID"`
}

type PaymentCallbackEvent struct {
	ID               uint64    `gorm:"primaryKey"`
	WeChatEventID    string    `gorm:"column:wechat_event_id;size:64;not null;uniqueIndex:uk_payment_callback_wechat_event"`
	PaymentAttemptID uint64    `gorm:"not null;index"`
	RawPayload       []byte    `gorm:"type:blob;not null"`
	ReceivedAt       time.Time `gorm:"not null"`
	ProcessedAt      *time.Time
	PaymentAttempt   PaymentAttempt `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:PaymentAttemptID"`
}

type OrderItemActivationCode struct {
	ID               uint64         `gorm:"primaryKey"`
	OrderItemID      uint64         `gorm:"not null;index"`
	ActivationCodeID uint64         `gorm:"not null;uniqueIndex:uk_order_item_activation_code"`
	BoundAt          time.Time      `gorm:"not null"`
	OrderItem        OrderItem      `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:OrderItemID"`
	ActivationCode   ActivationCode `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:ActivationCodeID"`
}

type ActivationCodeDelivery struct {
	ID                        uint64    `gorm:"primaryKey"`
	OrderItemActivationCodeID uint64    `gorm:"not null;uniqueIndex:uk_activation_code_deliveries_binding"`
	AvailableAt               time.Time `gorm:"not null"`
	FirstViewedAt             *time.Time
	OrderItemActivationCode   OrderItemActivationCode `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:OrderItemActivationCodeID"`
}

type SellerSettlement struct {
	ID           uint64    `gorm:"primaryKey"`
	SettlementNo string    `gorm:"size:40;not null;uniqueIndex:uk_seller_settlements_no"`
	SellerUserID uint64    `gorm:"not null;index:idx_seller_settlements_seller_status,priority:1"`
	AmountFen    uint64    `gorm:"not null"`
	Status       string    `gorm:"size:32;not null;default:pending;index:idx_seller_settlements_seller_status,priority:2"`
	RequestedAt  time.Time `gorm:"not null"`
	CompletedAt  *time.Time
	Seller       User `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:SellerUserID"`
}

type SellerEarning struct {
	ID             uint64            `gorm:"primaryKey"`
	OrderItemID    uint64            `gorm:"not null;uniqueIndex:uk_seller_earnings_order_item"`
	SellerUserID   uint64            `gorm:"not null;index:idx_seller_earnings_seller_status,priority:1"`
	SettlementID   *uint64           `gorm:"index"`
	GrossAmountFen uint64            `gorm:"not null"`
	PlatformFeeFen uint64            `gorm:"not null"`
	NetAmountFen   uint64            `gorm:"not null"`
	Status         string            `gorm:"size:32;not null;default:pending;index:idx_seller_earnings_seller_status,priority:2"`
	CreatedAt      time.Time         `gorm:"index:idx_seller_earnings_seller_status,priority:3"`
	OrderItem      OrderItem         `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:OrderItemID"`
	Seller         User              `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:SellerUserID"`
	Settlement     *SellerSettlement `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:SettlementID"`
}

type OutboxEvent struct {
	ID            uint64     `gorm:"primaryKey"`
	EventID       string     `gorm:"size:36;not null;uniqueIndex:uk_outbox_events_event_id"`
	Topic         string     `gorm:"size:128;not null"`
	AggregateType string     `gorm:"size:64;not null"`
	AggregateID   string     `gorm:"size:64;not null"`
	Payload       []byte     `gorm:"type:blob;not null"`
	OccurredAt    time.Time  `gorm:"not null;index:idx_outbox_events_unpublished,priority:2"`
	PublishedAt   *time.Time `gorm:"index:idx_outbox_events_unpublished,priority:1"`
	Attempts      uint       `gorm:"not null;default:0"`
}

//===========================================
//      软件衍生产品信息撮合
//===========================================

type DerivativeListing struct {
	ID              uint64    `gorm:"primaryKey"`
	PublisherUserID uint64    `gorm:"not null;index:idx_derivative_listings_publisher_status,priority:1"`
	Title           string    `gorm:"size:200;not null"`
	Category        string    `gorm:"size:80;not null"`
	Description     string    `gorm:"type:text;not null"`
	Status          string    `gorm:"size:32;not null;default:published;index:idx_derivative_listings_status_created,priority:1;index:idx_derivative_listings_publisher_status,priority:2"`
	CreatedAt       time.Time `gorm:"index:idx_derivative_listings_status_created,priority:2;index:idx_derivative_listings_publisher_status,priority:3"`
	UpdatedAt       time.Time
	Publisher       User `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:PublisherUserID"`
}

type Conversation struct {
	ID                  uint64 `gorm:"primaryKey"`
	DerivativeListingID uint64 `gorm:"not null;uniqueIndex:uk_conversations_listing_initiator,priority:1"`
	PublisherUserID     uint64 `gorm:"not null;index:idx_conversations_publisher_updated,priority:1"`
	InitiatorUserID     uint64 `gorm:"not null;uniqueIndex:uk_conversations_listing_initiator,priority:2"`
	CreatedAt           time.Time
	UpdatedAt           time.Time         `gorm:"index:idx_conversations_publisher_updated,priority:2"`
	DerivativeListing   DerivativeListing `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:DerivativeListingID"`
	Publisher           User              `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:PublisherUserID"`
	Initiator           User              `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:InitiatorUserID"`
}

type ConversationMessage struct {
	ID              uint64       `gorm:"primaryKey"`
	ConversationID  uint64       `gorm:"not null;uniqueIndex:uk_messages_client,priority:1;index:idx_messages_conversation_id,priority:1"`
	SenderUserID    uint64       `gorm:"not null;uniqueIndex:uk_messages_client,priority:2"`
	ClientMessageID string       `gorm:"size:36;not null;uniqueIndex:uk_messages_client,priority:3"`
	Content         string       `gorm:"type:text;not null"`
	CreatedAt       time.Time    `gorm:"index:idx_messages_conversation_id,priority:2"`
	Conversation    Conversation `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:ConversationID"`
	Sender          User         `gorm:"constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT;foreignKey:SenderUserID"`
}

func Models() []any {
	return []any{
		&User{}, &ExternalAccount{}, &UserSession{},
		&SoftwareProduct{}, &LicenseListing{}, &ActivationCode{}, &ActivationCodeVerification{},
		&ShoppingCartItem{}, &Order{}, &OrderItem{}, &PaymentAttempt{}, &PaymentCallbackEvent{},
		&OrderItemActivationCode{}, &ActivationCodeDelivery{}, &SellerSettlement{}, &SellerEarning{}, &OutboxEvent{},
		&DerivativeListing{}, &Conversation{}, &ConversationMessage{},
	}
}
