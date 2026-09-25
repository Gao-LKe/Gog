package contracts

const PaymentLifecycleTopic = "payment.lifecycle.v1"
const OrderCreationTopic = "order.create.requested.v1"

type PaymentLifecycleEvent struct {
	EventID     string `json:"event_id"`
	PaymentID   string `json:"payment_id"`
	OrderID     string `json:"order_id"`
	Type        string `json:"type"`
	AmountCents int64  `json:"amount_cents"`
}

type OrderCreationRequestedEvent struct {
	RequestID string                       `json:"request_id"`
	BuyerID   uint64                       `json:"buyer_id"`
	Items     []OrderCreationRequestedItem `json:"items"`
}

type OrderCreationRequestedItem struct {
	ListingID uint64 `json:"listing_id"`
	Quantity  uint   `json:"quantity"`
}
