package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/contracts"
	"github.com/goblog/backend/internal/platform"
)

type CreateRequest struct {
	OrderID     string `json:"order_id" binding:"required"`
	AmountCents int64  `json:"amount_cents" binding:"required,gt=0"`
}

type Provider interface {
	Create(context.Context, CreateRequest) (string, error)
}

type StubProvider struct {
	sequence uint64
}

func (p *StubProvider) Create(context.Context, CreateRequest) (string, error) {
	id := atomic.AddUint64(&p.sequence, 1)
	return fmt.Sprintf("stub-pay-%d", id), nil
}

type Service struct {
	provider Provider
	kafka    platform.KafkaProducer
}

func NewService(provider Provider, kafka platform.KafkaProducer) *Service {
	if provider == nil {
		provider = &StubProvider{}
	}
	return &Service{provider: provider, kafka: kafka}
}

func (s *Service) Create(ctx context.Context, req CreateRequest) (string, error) {
	if req.OrderID == "" || req.AmountCents <= 0 {
		return "", errors.New("invalid payment request")
	}
	id, err := s.provider.Create(ctx, req)
	if err != nil {
		return "", err
	}
	return id, s.publish(ctx, contracts.PaymentLifecycleEvent{EventID: id, PaymentID: id, OrderID: req.OrderID, Type: "created", AmountCents: req.AmountCents})
}

func (s *Service) publish(ctx context.Context, event contracts.PaymentLifecycleEvent) error {
	if s.kafka == nil {
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return s.kafka.Publish(ctx, contracts.PaymentLifecycleTopic, event.PaymentID, payload)
}

func RegisterRoutes(r *gin.RouterGroup, service *Service) {
	r.POST("/payments", func(c *gin.Context) {
		var req CreateRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		id, err := service.Create(c.Request.Context(), req)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"payment_id": id})
	})
}
