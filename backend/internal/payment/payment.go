package payment

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/contracts"
	"github.com/goblog/backend/internal/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrInvalidRequest      = errors.New("invalid payment request")
	ErrOrderUnavailable    = errors.New("order is not available for payment")
	ErrIdempotencyConflict = errors.New("idempotency key belongs to another order")
	ErrPaymentInProgress   = errors.New("order already has an active payment")
)

type CreateRequest struct {
	OrderNo string `json:"order_no" binding:"required"`
}

type CreateResult struct {
	PaymentNo string `json:"payment_no"`
	AmountFen uint64 `json:"amount_fen"`
	Status    string `json:"status"`
}

// Provider only creates a channel-side payment object. Confirmation remains
// channel-driven, so a browser response can never mark an order as paid.
type Provider interface {
	Create(context.Context, string, uint64) error
}

type StubProvider struct{ sequence uint64 }

func (p *StubProvider) Create(_ context.Context, _ string, _ uint64) error {
	atomic.AddUint64(&p.sequence, 1)
	return nil
}

type Service struct {
	db       *gorm.DB
	provider Provider
}

func NewService(db *gorm.DB, provider Provider) *Service {
	if provider == nil {
		provider = &StubProvider{}
	}
	return &Service{db: db, provider: provider}
}

// Create persists the payment attempt before communicating with a channel.
// The amount comes exclusively from the order snapshot, never from the client.
func (s *Service) Create(ctx context.Context, buyerID uint64, idempotencyKey string, req CreateRequest) (CreateResult, error) {
	if s.db == nil || buyerID == 0 || strings.TrimSpace(req.OrderNo) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return CreateResult{}, ErrInvalidRequest
	}

	attempt, existing, err := s.createAttempt(ctx, buyerID, strings.TrimSpace(idempotencyKey), strings.TrimSpace(req.OrderNo))
	if err != nil {
		return CreateResult{}, err
	}
	if existing {
		return resultFromAttempt(attempt), nil
	}

	if err := s.provider.Create(ctx, attempt.WeChatOutTradeNo, attempt.AmountFen); err != nil {
		_ = s.db.WithContext(ctx).Model(&store.PaymentAttempt{}).Where("id = ? AND status = ?", attempt.ID, "creating").Update("status", "provider_failed").Error
		return CreateResult{}, err
	}
	if err := s.markChannelCreated(ctx, attempt.ID); err != nil {
		return CreateResult{}, err
	}
	attempt.Status = "pending"
	return resultFromAttempt(attempt), nil
}

func (s *Service) createAttempt(ctx context.Context, buyerID uint64, idempotencyKey, orderNo string) (store.PaymentAttempt, bool, error) {
	var attempt store.PaymentAttempt
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var order store.Order
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("order_no = ? AND buyer_user_id = ?", orderNo, buyerID).First(&order).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrOrderUnavailable
			}
			return err
		}
		if order.Status != "pending_payment" {
			return ErrOrderUnavailable
		}

		var keyed store.PaymentAttempt
		if err := tx.Where("buyer_user_id = ? AND idempotency_key = ?", buyerID, idempotencyKey).First(&keyed).Error; err == nil {
			if keyed.OrderID != order.ID {
				return ErrIdempotencyConflict
			}
			attempt = keyed
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		// The order row lock serializes different idempotency keys for one order.
		var active store.PaymentAttempt
		if err := tx.Where("order_id = ? AND status IN ?", order.ID, []string{"creating", "pending"}).Order("id DESC").First(&active).Error; err == nil {
			return ErrPaymentInProgress
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		paymentNo, err := newReference("pay")
		if err != nil {
			return err
		}
		attempt = store.PaymentAttempt{PaymentNo: paymentNo, OrderID: order.ID, BuyerUserID: buyerID, WeChatOutTradeNo: paymentNo, AmountFen: order.TotalAmountFen, IdempotencyKey: idempotencyKey, Status: "creating", ExpiresAt: order.ExpiresAt}
		return tx.Create(&attempt).Error
	})
	if err != nil {
		return store.PaymentAttempt{}, false, err
	}
	return attempt, attempt.Status != "creating", nil
}

func (s *Service) markChannelCreated(ctx context.Context, paymentAttemptID uint64) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var attempt store.PaymentAttempt
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&attempt, paymentAttemptID).Error; err != nil {
			return err
		}
		if attempt.Status != "creating" {
			return nil
		}
		payload, err := json.Marshal(contracts.PaymentLifecycleEvent{EventID: attempt.PaymentNo, PaymentID: attempt.PaymentNo, OrderID: fmt.Sprint(attempt.OrderID), Type: "payment.created", AmountCents: int64(attempt.AmountFen)})
		if err != nil {
			return err
		}
		if err := tx.Model(&attempt).Update("status", "pending").Error; err != nil {
			return err
		}
		return tx.Create(&store.OutboxEvent{EventID: attempt.PaymentNo, Topic: contracts.PaymentLifecycleTopic, AggregateType: "payment_attempt", AggregateID: attempt.PaymentNo, Payload: payload, OccurredAt: time.Now().UTC()}).Error
	})
}

func resultFromAttempt(attempt store.PaymentAttempt) CreateResult {
	return CreateResult{PaymentNo: attempt.PaymentNo, AmountFen: attempt.AmountFen, Status: attempt.Status}
}

func newReference(prefix string) (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(bytes), nil
}

func RegisterRoutes(r *gin.RouterGroup, service *Service) {
	r.POST("/payments", func(c *gin.Context) {
		var req CreateRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		user, ok := c.Get("auth.user")
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			return
		}
		result, err := service.Create(c.Request.Context(), user.(store.User).ID, c.GetHeader("Idempotency-Key"), req)
		if err != nil {
			switch {
			case errors.Is(err, ErrInvalidRequest), errors.Is(err, ErrIdempotencyConflict):
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			case errors.Is(err, ErrOrderUnavailable), errors.Is(err, ErrPaymentInProgress):
				c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			default:
				c.JSON(http.StatusBadGateway, gin.H{"error": "payment provider is unavailable"})
			}
			return
		}
		c.JSON(http.StatusAccepted, result)
	})
}
