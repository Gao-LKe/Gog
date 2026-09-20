package payment

import (
	"context"
	"testing"
)

func TestCreatePaymentIntent(t *testing.T) {
	s := NewService(nil, nil)
	id, err := s.Create(context.Background(), CreateRequest{OrderID: "order-1", AmountCents: 100})
	if err != nil || id == "" {
		t.Fatalf("create failed: %q, %v", id, err)
	}
}
