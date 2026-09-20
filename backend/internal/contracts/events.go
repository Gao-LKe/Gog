package contracts

const PaymentLifecycleTopic = "payment.lifecycle.v1"

type PaymentLifecycleEvent struct {
	EventID     string `json:"event_id"`
	PaymentID   string `json:"payment_id"`
	OrderID     string `json:"order_id"`
	Type        string `json:"type"`
	AmountCents int64  `json:"amount_cents"`
}
