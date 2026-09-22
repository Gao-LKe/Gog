package payment

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
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
	ErrInsufficientBalance = errors.New("insufficient balance")
)

type CreateRequest struct {
	OrderNo string `json:"order_no" binding:"required"`
}

type CreateResult struct {
	PaymentNo string `json:"payment_no"`
	AmountFen uint64 `json:"amount_fen"`
	Status    string `json:"status"`
}

type BalanceView struct {
	AvailableFen uint64 `json:"available_fen"`
}

type Service struct{ db *gorm.DB }

// The optional argument keeps the previous constructor call site source-compatible
// while the external-channel adapter is intentionally disabled for balance payment.
func NewService(db *gorm.DB, _ ...any) *Service { return &Service{db: db} }

// Create pays an order with the buyer's stored balance. Balance deduction,
// payment confirmation and the lifecycle Outbox record share one transaction.
func (s *Service) Create(ctx context.Context, buyerID uint64, idempotencyKey string, req CreateRequest) (CreateResult, error) {
	if s.db == nil || buyerID == 0 || strings.TrimSpace(req.OrderNo) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return CreateResult{}, ErrInvalidRequest
	}

	var attempt store.PaymentAttempt
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var order store.Order
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("order_no = ? AND buyer_user_id = ?", strings.TrimSpace(req.OrderNo), buyerID).First(&order).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrOrderUnavailable
			}
			return err
		}

		var keyed store.PaymentAttempt
		if err := tx.Where("buyer_user_id = ? AND idempotency_key = ?", buyerID, strings.TrimSpace(idempotencyKey)).First(&keyed).Error; err == nil {
			if keyed.OrderID != order.ID {
				return ErrIdempotencyConflict
			}
			attempt = keyed
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if order.Status != "pending_payment" {
			return ErrOrderUnavailable
		}

		var balance store.UserBalance
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&balance, "user_id = ?", buyerID).Error; err != nil {
			return err
		}
		if balance.AvailableFen < order.TotalAmountFen {
			return ErrInsufficientBalance
		}

		paymentNo, err := newReference("bal")
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		balance.AvailableFen -= order.TotalAmountFen
		if err := tx.Model(&balance).Updates(map[string]any{"available_fen": balance.AvailableFen, "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Create(&store.BalanceTransaction{
			UserID: buyerID, Type: "order_payment", AmountFen: -int64(order.TotalAmountFen), BalanceAfterFen: balance.AvailableFen,
			ReferenceType: "order", ReferenceID: strconv.FormatUint(order.ID, 10), CreatedAt: now,
		}).Error; err != nil {
			return err
		}
		attempt = store.PaymentAttempt{
			PaymentNo: paymentNo, PaymentMethod: "balance", OrderID: order.ID, BuyerUserID: buyerID, WeChatOutTradeNo: paymentNo,
			AmountFen: order.TotalAmountFen, IdempotencyKey: strings.TrimSpace(idempotencyKey), Status: "succeeded", ExpiresAt: order.ExpiresAt,
		}
		if err := tx.Create(&attempt).Error; err != nil {
			return err
		}
		if err := tx.Model(&order).Updates(map[string]any{"status": "paid", "paid_at": now}).Error; err != nil {
			return err
		}
		payload, err := json.Marshal(contracts.PaymentLifecycleEvent{EventID: paymentNo, PaymentID: paymentNo, OrderID: order.OrderNo, Type: "payment.succeeded", AmountCents: int64(order.TotalAmountFen)})
		if err != nil {
			return err
		}
		return tx.Create(&store.OutboxEvent{EventID: paymentNo, Topic: contracts.PaymentLifecycleTopic, AggregateType: "payment_attempt", AggregateID: paymentNo, Payload: payload, OccurredAt: now}).Error
	})
	if err != nil {
		return CreateResult{}, err
	}
	return resultFromAttempt(attempt), nil
}

func (s *Service) Balance(ctx context.Context, userID uint64) (BalanceView, error) {
	if s.db == nil || userID == 0 {
		return BalanceView{}, ErrInvalidRequest
	}
	var balance store.UserBalance
	if err := s.db.WithContext(ctx).First(&balance, "user_id = ?", userID).Error; err != nil {
		return BalanceView{}, err
	}
	return BalanceView{AvailableFen: balance.AvailableFen}, nil
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
	r.GET("/balance", func(c *gin.Context) {
		user, ok := c.Get("auth.user")
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			return
		}
		balance, err := service.Balance(c.Request.Context(), user.(store.User).ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "read balance failed"})
			return
		}
		c.JSON(http.StatusOK, balance)
	})

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
			case errors.Is(err, ErrOrderUnavailable), errors.Is(err, ErrInsufficientBalance):
				c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			default:
				c.JSON(http.StatusInternalServerError, gin.H{"error": "balance payment failed"})
			}
			return
		}
		c.JSON(http.StatusOK, result)
	})
}
