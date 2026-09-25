package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/platform"
	"github.com/goblog/backend/internal/store"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

const accessTokenType = "access"
const registerPurpose = "register"
const loginPurpose = "login"
const emailCooldown = time.Minute
const emailDailyLimit = 10
const verificationMaxAttempts = 5
const passwordMaxFailures = 5
const passwordLockDuration = 15 * time.Minute

const (
	RoleUser     = "user"
	RoleSupport  = "support"
	RoleAdmin    = "admin"
	DevicePC     = "pc"
	DeviceMobile = "mobile"
)

const (
	PermissionBuy          = "buy"
	PermissionChat         = "chat"
	PermissionSell         = "sell"
	PermissionHandleTicket = "handle_ticket"
	PermissionManageUser   = "manage_user"
	PermissionManageSystem = "manage_system"
)

var rolePermissions = map[string][]string{
	RoleUser:    {PermissionBuy, PermissionChat, PermissionSell},
	RoleSupport: {PermissionChat, PermissionHandleTicket},
	RoleAdmin:   {PermissionManageUser, PermissionManageSystem},
}

// DefaultEndpointAuthorizer is the shared vocabulary for the handler-entry
// check. Resource services use the same permission names in their own policy.
func DefaultEndpointAuthorizer() EndpointAuthorizer {
	return EndpointAuthorizer{
		KnownRoles: map[string]struct{}{RoleUser: {}, RoleSupport: {}, RoleAdmin: {}},
		KnownPermissions: map[string]struct{}{
			PermissionBuy: {}, PermissionChat: {}, PermissionSell: {}, PermissionHandleTicket: {}, PermissionManageUser: {}, PermissionManageSystem: {},
		},
	}
}

var codePattern = regexp.MustCompile(`^\d{6}$`)

var ErrInvalidCode = errors.New("invalid or expired verification code")
var ErrEmailNotRegistered = errors.New("email is not registered")
var ErrAccountUnavailable = errors.New("account is unavailable")
var ErrInvalidRefresh = errors.New("invalid or expired refresh token")
var ErrInvalidPassword = errors.New("invalid password")
var ErrSessionBusy = errors.New("another sign-in for this device is in progress")

var passwordTimingHash = []byte("$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy")

// Claims deliberately excludes roles and mutable profile fields.
type Claims struct {
	Subject   string
	SessionID string
	TokenType string
}

type jwtClaims struct {
	SessionID string `json:"sid"`
	TokenType string `json:"typ"`
	jwt.RegisteredClaims
}

type Verifier interface {
	Verify(token string) (Claims, error)
}

type JWTVerifier struct {
	secret   []byte
	issuer   string
	audience string
}

func NewJWTVerifier(secret string) *JWTVerifier {
	return NewJWTVerifierWithScope(secret, "goblog", "goblog-api")
}
func NewJWTVerifierWithScope(secret, issuer, audience string) *JWTVerifier {
	return &JWTVerifier{secret: []byte(secret), issuer: issuer, audience: audience}
}

func (v *JWTVerifier) Verify(raw string) (Claims, error) {
	claims := new(jwtClaims)
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, errors.New("unexpected signing method")
		}
		return v.secret, nil
	}, jwt.WithIssuer(v.issuer), jwt.WithAudience(v.audience), jwt.WithExpirationRequired())
	if err != nil || !token.Valid || claims.Subject == "" || claims.SessionID == "" || claims.TokenType != accessTokenType || claims.ID == "" || claims.IssuedAt == nil {
		return Claims{}, errors.New("invalid access token")
	}
	return Claims{Subject: claims.Subject, SessionID: claims.SessionID, TokenType: claims.TokenType}, nil
}

type ServiceConfig struct {
	Secret              string
	Issuer              string
	Audience            string
	AccessTokenTTL      time.Duration
	RefreshTokenTTL     time.Duration
	EmailCodeTTL        time.Duration
	BootstrapAdminEmail string
	SnowflakeNodeID     int64
}

type Service struct {
	db          *gorm.DB
	redis       platform.RedisClient
	redisAtomic platform.RedisAtomicClient
	emailSender EmailSender
	cfg         ServiceConfig
	signer      *JWTVerifier
	ids         *Snowflake
	idError     error
}

type TokenPair struct {
	AccessToken      string    `json:"access_token"`
	RefreshToken     string    `json:"refresh_token"`
	AccessExpiresAt  time.Time `json:"access_expires_at"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
}

// AuthenticatedIdentity is server-derived request identity. Handlers receive
// it through Gin context; they must never reconstruct it from request fields.
type AuthenticatedIdentity struct {
	User      store.User
	Principal Principal
}

func NewService(db *gorm.DB, redis platform.RedisClient, redisAtomic platform.RedisAtomicClient, emailSender EmailSender, cfg ServiceConfig) *Service {
	ids, err := NewSnowflake(cfg.SnowflakeNodeID)
	return &Service{db: db, redis: redis, redisAtomic: redisAtomic, emailSender: emailSender, cfg: cfg, signer: NewJWTVerifierWithScope(cfg.Secret, cfg.Issuer, cfg.Audience), ids: ids, idError: err}
}

// SendEmailCode writes only a keyed hash. Until a real provider is configured,
// the development adapter writes the code to the server log.
func (s *Service) SendEmailCode(ctx context.Context, rawEmail, purpose string) error {
	email, err := NormalizeEmail(rawEmail)
	if err != nil {
		return err
	}
	if purpose != registerPurpose && purpose != loginPurpose {
		return errors.New("unsupported verification purpose")
	}
	allowed, err := s.redis.SetIfNotExists(ctx, emailCooldownKey(email, purpose), "1", emailCooldown)
	if err != nil {
		return fmt.Errorf("check email cooldown: %w", err)
	}
	if !allowed {
		return errors.New("verification code requested too frequently")
	}
	if s.redisAtomic == nil {
		return errors.New("Redis atomic operations are unavailable")
	}
	count, err := s.redisAtomic.IncrementWithTTL(ctx, emailDailyKey(email, purpose), 24*time.Hour)
	if err != nil {
		return fmt.Errorf("check email daily limit: %w", err)
	}
	if count > emailDailyLimit {
		return errors.New("verification code daily limit reached")
	}
	code, err := randomDigits(6)
	if err != nil {
		return err
	}
	if err := s.redis.Set(ctx, verificationKey(email, purpose), s.codeHash(email, purpose, code), s.cfg.EmailCodeTTL); err != nil {
		return fmt.Errorf("store verification code: %w", err)
	}
	if err := s.redis.Del(ctx, verificationAttemptKey(email, purpose)); err != nil {
		return fmt.Errorf("reset verification attempts: %w", err)
	}
	if err := s.emailSender.SendVerificationCode(ctx, email, code); err != nil {
		_ = s.redis.Del(ctx, verificationKey(email, purpose), emailCooldownKey(email, purpose))
		return fmt.Errorf("send verification code: %w", err)
	}
	log.Printf("email verification code sent purpose=%s email=%s", purpose, maskEmail(email))
	return nil
}

func (s *Service) Register(ctx context.Context, rawEmail, code, displayName, password, rawDeviceType string) (TokenPair, error) {
	email, err := NormalizeEmail(rawEmail)
	if err != nil {
		return TokenPair{}, err
	}
	if err := s.consumeCode(ctx, email, registerPurpose, code); err != nil {
		return TokenPair{}, err
	}
	passwordHash, err := hashPassword(password)
	if err != nil {
		return TokenPair{}, err
	}
	deviceType, err := NormalizeDeviceType(rawDeviceType)
	if err != nil {
		return TokenPair{}, err
	}
	name := strings.TrimSpace(displayName)
	if name == "" {
		name = "用户" + email[:min(4, len(email))]
	}
	now := time.Now().UTC()
	role := RoleUser
	if cfgEmail, err := NormalizeEmail(s.cfg.BootstrapAdminEmail); err == nil && cfgEmail == email {
		role = RoleAdmin
	}
	if s.idError != nil {
		return TokenPair{}, fmt.Errorf("initialize user id generator: %w", s.idError)
	}
	userID, err := s.ids.NextID()
	if err != nil {
		return TokenPair{}, fmt.Errorf("generate user id: %w", err)
	}
	user := store.User{ID: userID, DisplayName: name, Email: &email, EmailVerifiedAt: &now, PasswordHash: passwordHash, Role: role, Status: "active"}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&user).Error; err != nil {
			return err
		}
		access := newFullAccess(user.ID)
		return tx.Create(&access).Error
	}); err != nil {
		return TokenPair{}, fmt.Errorf("create user: %w", err)
	}
	return s.createSession(ctx, user, deviceType, "register")
}

func (s *Service) LoginByEmail(ctx context.Context, rawEmail, code, rawDeviceType string) (TokenPair, error) {
	email, err := NormalizeEmail(rawEmail)
	if err != nil {
		return TokenPair{}, err
	}
	if err := s.consumeCode(ctx, email, loginPurpose, code); err != nil {
		return TokenPair{}, err
	}
	deviceType, err := NormalizeDeviceType(rawDeviceType)
	if err != nil {
		return TokenPair{}, err
	}
	var user store.User
	if err := s.db.WithContext(ctx).Where("email = ?", email).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return TokenPair{}, ErrEmailNotRegistered
		}
		return TokenPair{}, fmt.Errorf("find user: %w", err)
	}
	return s.createSession(ctx, user, deviceType, "email_code")
}

func (s *Service) LoginByPassword(ctx context.Context, rawEmail, password, rawDeviceType string) (TokenPair, error) {
	email, err := NormalizeEmail(rawEmail)
	if err != nil {
		return TokenPair{}, err
	}
	deviceType, err := NormalizeDeviceType(rawDeviceType)
	if err != nil {
		return TokenPair{}, err
	}
	locked, err := s.redis.Has(ctx, passwordLockKey(email))
	if err != nil || locked {
		return TokenPair{}, ErrInvalidPassword
	}
	var user store.User
	if err := s.db.WithContext(ctx).Where("email = ?", email).First(&user).Error; err != nil {
		_ = bcrypt.CompareHashAndPassword(passwordTimingHash, []byte(password))
		_ = s.recordPasswordFailure(ctx, email)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return TokenPair{}, ErrEmailNotRegistered
		}
		return TokenPair{}, fmt.Errorf("find user: %w", err)
	}
	if len(user.PasswordHash) == 0 || bcrypt.CompareHashAndPassword(user.PasswordHash, []byte(password)) != nil {
		_ = s.recordPasswordFailure(ctx, email)
		return TokenPair{}, ErrInvalidPassword
	}
	if err := s.redis.Del(ctx, passwordFailureKey(email), passwordLockKey(email)); err != nil {
		return TokenPair{}, ErrInvalidPassword
	}
	return s.createSession(ctx, user, deviceType, "password")
}

func (s *Service) Refresh(ctx context.Context, refreshToken string) (TokenPair, error) {
	hash := tokenHashHex(refreshToken)
	sessionID, err := s.redis.Get(ctx, refreshIndexKey(hash))
	if err != nil || sessionID == "" {
		return TokenPair{}, ErrInvalidRefresh
	}
	session, err := s.getSession(ctx, sessionID)
	if err != nil || time.Now().UTC().Unix() >= session.ExpiresAt {
		return TokenPair{}, ErrInvalidRefresh
	}
	newRefresh, err := randomToken(32)
	if err != nil {
		return TokenPair{}, err
	}
	newHash := tokenHashHex(newRefresh)
	ttl := time.Until(time.Unix(session.ExpiresAt, 0))
	if ttl <= 0 {
		return TokenPair{}, ErrInvalidRefresh
	}
	result, replaySID, err := s.redisAtomic.RotateRefreshHash(ctx, platform.RedisRefreshKeys{
		SessionKey: sessionKey(sessionID), ReplayKey: refreshReplayKey(hash), NewRefreshIndex: refreshIndexKey(newHash), SID: sessionID,
	}, hash, newHash, ttl)
	if err != nil {
		return TokenPair{}, fmt.Errorf("rotate refresh token: %w", err)
	}
	if result == platform.RefreshRotationReplay {
		if err := s.revokeSession(ctx, replaySID); err != nil {
			return TokenPair{}, fmt.Errorf("revoke replayed session: %w", err)
		}
		return TokenPair{}, ErrInvalidRefresh
	}
	if result != platform.RefreshRotationSucceeded {
		return TokenPair{}, ErrInvalidRefresh
	}
	return s.tokenPair(session.UserID, sessionID, newRefresh, time.Unix(session.ExpiresAt, 0).UTC())
}

func (s *Service) revokeSession(ctx context.Context, sessionID string) error {
	if sessionID == "" || s.redisAtomic == nil {
		return errors.New("session storage is unavailable")
	}
	session, found, err := s.redisAtomic.GetSession(ctx, sessionKey(sessionID))
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if _, err := NormalizeDeviceType(session.DeviceType); err != nil || session.UserID == 0 {
		return errors.New("invalid session record")
	}
	_, err = s.redisAtomic.DeleteSessionIfCurrent(ctx, redisSessionKeys(session.UserID, session.DeviceType, sessionID, ""))
	return err
}

func (s *Service) Logout(ctx context.Context, claims Claims) error {
	return s.RevokeSession(ctx, claims, claims.SessionID)
}

// RevokeSession removes one of the caller's Redis sessions. The user ID is
// derived from the verified access token, never from a path parameter alone.
func (s *Service) RevokeSession(ctx context.Context, claims Claims, targetSessionID string) error {
	userID, err := strconv.ParseUint(claims.Subject, 10, 64)
	if err != nil {
		return ErrAccountUnavailable
	}
	session, err := s.getSession(ctx, targetSessionID)
	if err != nil || session.UserID != userID {
		return ErrAccountUnavailable
	}
	_, err = s.redisAtomic.DeleteSessionIfCurrent(ctx, redisSessionKeys(userID, session.DeviceType, targetSessionID, ""))
	return err
}

type SessionView struct {
	ID         string    `json:"id"`
	DeviceType string    `json:"device_type"`
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Current    bool      `json:"current"`
}

func (s *Service) Sessions(ctx context.Context, claims Claims) ([]SessionView, error) {
	userID, err := strconv.ParseUint(claims.Subject, 10, 64)
	if err != nil {
		return nil, ErrAccountUnavailable
	}
	views := make([]SessionView, 0, 2)
	for _, deviceType := range []string{DevicePC, DeviceMobile} {
		slotKey := deviceSlotKey(userID, deviceType)
		exists, err := s.redis.Has(ctx, slotKey)
		if err != nil {
			return nil, fmt.Errorf("read %s session slot: %w", deviceType, err)
		}
		if !exists {
			continue
		}
		sessionID, err := s.redis.Get(ctx, slotKey)
		if err != nil {
			return nil, fmt.Errorf("read %s session id: %w", deviceType, err)
		}
		session, found, err := s.redisAtomic.GetSession(ctx, sessionKey(sessionID))
		if err != nil {
			return nil, fmt.Errorf("read %s session: %w", deviceType, err)
		}
		if !found || session.ExpiresAt <= time.Now().UTC().Unix() || session.UserID != userID || session.DeviceType != deviceType {
			continue
		}
		views = append(views, SessionView{ID: sessionID, DeviceType: deviceType, CreatedAt: time.Unix(session.CreatedAt, 0).UTC(), ExpiresAt: time.Unix(session.ExpiresAt, 0).UTC(), Current: sessionID == claims.SessionID})
	}
	return views, nil
}

func (s *Service) LogoutAll(ctx context.Context, claims Claims) error {
	userID, err := strconv.ParseUint(claims.Subject, 10, 64)
	if err != nil {
		return ErrAccountUnavailable
	}
	return s.revokeUserSessions(ctx, userID)
}

func (s *Service) Me(ctx context.Context, claims Claims) (store.User, error) {
	var user store.User
	if err := s.db.WithContext(ctx).First(&user, claims.Subject).Error; err != nil {
		return store.User{}, err
	}
	if user.Status != "active" {
		return store.User{}, ErrAccountUnavailable
	}
	return user, nil
}

func (s *Service) SetRole(ctx context.Context, userID uint64, role string) error {
	if role != RoleUser && role != RoleSupport && role != RoleAdmin {
		return errors.New("invalid role")
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&store.User{}).Where("id = ?", userID).Update("role", role)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return tx.Create(&store.UserAccountEvent{UserID: userID, EventType: "role_changed", Detail: role, CreatedAt: time.Now().UTC()}).Error
	}); err != nil {
		return err
	}
	return s.revokeUserSessions(ctx, userID)
}

func (s *Service) SetAccountStatus(ctx context.Context, userID uint64, status string) error {
	if status != "active" && status != "frozen" {
		return errors.New("invalid account status")
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&store.User{}).Where("id = ?", userID).Update("status", status)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return tx.Create(&store.UserAccountEvent{UserID: userID, EventType: "status_changed", Detail: status, CreatedAt: time.Now().UTC()}).Error
	}); err != nil {
		return err
	}
	return s.revokeUserSessions(ctx, userID)
}

// BootstrapAdmin promotes the configured bootstrap account and invalidates
// sessions when its role actually changes. It is called during process start
// so a surviving Redis instance cannot retain a stale ordinary-user session.
func BootstrapAdmin(ctx context.Context, db *gorm.DB, redis platform.RedisClient, redisAtomic platform.RedisAtomicClient, rawEmail string) error {
	if strings.TrimSpace(rawEmail) == "" {
		return nil
	}
	email, err := NormalizeEmail(rawEmail)
	if err != nil {
		return fmt.Errorf("bootstrap admin email: %w", err)
	}
	var user store.User
	if err := db.WithContext(ctx).Where("email = ?", email).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	if user.Role == RoleAdmin {
		return nil
	}
	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&store.User{}).Where("id = ?", user.ID).Update("role", RoleAdmin).Error; err != nil {
			return err
		}
		return tx.Create(&store.UserAccountEvent{UserID: user.ID, EventType: "role_changed", Detail: RoleAdmin, CreatedAt: time.Now().UTC()}).Error
	}); err != nil {
		return err
	}
	return (&Service{redis: redis, redisAtomic: redisAtomic}).revokeUserSessions(ctx, user.ID)
}

func (s *Service) Authenticate(ctx context.Context, claims Claims) (store.User, error) {
	identity, err := s.authenticateIdentity(ctx, claims)
	if err != nil {
		return store.User{}, err
	}
	return identity.User, nil
}

// AuthenticatePrincipal returns the authenticated account and immutable
// authorization context built only from the Redis session record.
func (s *Service) AuthenticatePrincipal(ctx context.Context, claims Claims) (AuthenticatedIdentity, error) {
	return s.authenticateIdentity(ctx, claims)
}

func (s *Service) authenticateIdentity(ctx context.Context, claims Claims) (AuthenticatedIdentity, error) {
	userID, err := strconv.ParseUint(claims.Subject, 10, 64)
	if err != nil {
		return AuthenticatedIdentity{}, ErrAccountUnavailable
	}
	session, err := s.getSession(ctx, claims.SessionID)
	if err != nil || session.UserID != userID {
		return AuthenticatedIdentity{}, ErrAccountUnavailable
	}
	principal, err := newPrincipal(claims.Subject, session.Role, claims.SessionID, session.Permissions)
	if err != nil {
		return AuthenticatedIdentity{}, ErrAccountUnavailable
	}
	return AuthenticatedIdentity{User: store.User{ID: session.UserID, Role: session.Role, Status: "active"}, Principal: principal}, nil
}

func (s *Service) createSession(ctx context.Context, user store.User, deviceType, method string) (TokenPair, error) {
	if user.Status != "active" {
		return TokenPair{}, ErrAccountUnavailable
	}
	permissions, err := s.compilePermissions(ctx, user)
	if err != nil {
		return TokenPair{}, err
	}
	refresh, err := randomToken(32)
	if err != nil {
		return TokenPair{}, err
	}
	id, err := newUUIDv4()
	if err != nil {
		return TokenPair{}, err
	}
	now := time.Now().UTC()
	expiresAt := now.Add(s.cfg.RefreshTokenTTL)
	refreshHash := tokenHashHex(refresh)
	if _, err := s.redisAtomic.ReplaceSession(ctx, redisSessionKeys(user.ID, deviceType, id, refreshHash), platform.RedisSessionRecord{
		UserID: user.ID, DeviceType: deviceType, Role: user.Role, Permissions: permissions, RefreshHash: refreshHash, CreatedAt: now.Unix(), ExpiresAt: expiresAt.Unix(),
	}, s.cfg.RefreshTokenTTL); err != nil {
		return TokenPair{}, fmt.Errorf("create Redis session: %w", err)
	}
	if err := s.db.WithContext(ctx).Create(&store.UserLoginEvent{UserID: user.ID, Method: method, DeviceType: deviceType, CreatedAt: now}).Error; err != nil {
		_ = s.revokeSession(ctx, id)
		return TokenPair{}, fmt.Errorf("record login event: %w", err)
	}
	return s.tokenPair(user.ID, id, refresh, expiresAt)
}

func (s *Service) getSession(ctx context.Context, sessionID string) (platform.RedisSessionRecord, error) {
	if sessionID == "" || s.redisAtomic == nil {
		return platform.RedisSessionRecord{}, errors.New("session storage is unavailable")
	}
	session, found, err := s.redisAtomic.GetSession(ctx, sessionKey(sessionID))
	if err != nil || !found || session.ExpiresAt <= time.Now().UTC().Unix() {
		return platform.RedisSessionRecord{}, errors.New("invalid or expired session")
	}
	if _, err := NormalizeDeviceType(session.DeviceType); err != nil || !validRole(session.Role) {
		return platform.RedisSessionRecord{}, errors.New("invalid session identity")
	}
	return session, nil
}

func (s *Service) revokeUserSessions(ctx context.Context, userID uint64) error {
	for _, deviceType := range []string{DevicePC, DeviceMobile} {
		slotKey := deviceSlotKey(userID, deviceType)
		exists, err := s.redis.Has(ctx, slotKey)
		if err != nil {
			return fmt.Errorf("read %s session slot: %w", deviceType, err)
		}
		if !exists {
			continue
		}
		sessionID, err := s.redis.Get(ctx, slotKey)
		if err != nil {
			return fmt.Errorf("read %s session id: %w", deviceType, err)
		}
		session, found, err := s.redisAtomic.GetSession(ctx, sessionKey(sessionID))
		if err != nil {
			return fmt.Errorf("read %s session: %w", deviceType, err)
		}
		if !found {
			// The slot is stale only when it still points at this ID. The
			// conditional delete cannot remove a concurrently-created session.
			if _, deleteErr := s.redisAtomic.DeleteSessionIfCurrent(ctx, redisSessionKeys(userID, deviceType, sessionID, "")); deleteErr != nil {
				return fmt.Errorf("remove stale %s session: %w", deviceType, deleteErr)
			}
			continue
		}
		if _, err := NormalizeDeviceType(session.DeviceType); err != nil || !validRole(session.Role) {
			return fmt.Errorf("%s session slot has invalid record", deviceType)
		}
		if session.UserID != userID || session.DeviceType != deviceType {
			return fmt.Errorf("%s session slot has inconsistent identity", deviceType)
		}
		if _, err := s.redisAtomic.DeleteSessionIfCurrent(ctx, redisSessionKeys(userID, deviceType, sessionID, "")); err != nil {
			return fmt.Errorf("remove %s session: %w", deviceType, err)
		}
	}
	return nil
}

func newFullAccess(userID uint64) store.UserAccess {
	return store.UserAccess{
		UserID: userID, CanBuy: true, CanChat: true, CanSell: true,
		CanHandleTicket: true, CanManageUser: true, CanManageSystem: true,
	}
}

func validRole(role string) bool {
	_, ok := rolePermissions[role]
	return ok
}

func validPermission(code string) bool {
	switch code {
	case PermissionBuy, PermissionChat, PermissionSell, PermissionHandleTicket, PermissionManageUser, PermissionManageSystem:
		return true
	default:
		return false
	}
}

func accessAllows(access store.UserAccess, code string) bool {
	switch code {
	case PermissionBuy:
		return access.CanBuy
	case PermissionChat:
		return access.CanChat
	case PermissionSell:
		return access.CanSell
	case PermissionHandleTicket:
		return access.CanHandleTicket
	case PermissionManageUser:
		return access.CanManageUser
	case PermissionManageSystem:
		return access.CanManageSystem
	default:
		return false
	}
}

func accessColumn(code string) string {
	switch code {
	case PermissionBuy:
		return "can_buy"
	case PermissionChat:
		return "can_chat"
	case PermissionSell:
		return "can_sell"
	case PermissionHandleTicket:
		return "can_handle_ticket"
	case PermissionManageUser:
		return "can_manage_user"
	case PermissionManageSystem:
		return "can_manage_system"
	default:
		return ""
	}
}

func (s *Service) loadAccess(ctx context.Context, userID uint64) (store.UserAccess, error) {
	var access store.UserAccess
	err := s.db.WithContext(ctx).Where("user_id = ?", userID).First(&access).Error
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return access, err
	}
	// Existing accounts from before UserAccess was introduced receive the
	// default allow facts exactly once. A concurrent initializer reuses its row.
	created := newFullAccess(userID)
	if err := s.db.WithContext(ctx).Create(&created).Error; err == nil {
		return created, nil
	}
	if err := s.db.WithContext(ctx).Where("user_id = ?", userID).First(&access).Error; err != nil {
		return store.UserAccess{}, err
	}
	return access, nil
}

// compilePermissions resolves only fields meaningful to the user's current
// role. It runs while establishing a new Redis session, never per request.
func (s *Service) compilePermissions(ctx context.Context, user store.User) ([]string, error) {
	relevant, ok := rolePermissions[user.Role]
	if !ok {
		return nil, errors.New("user has an unknown role")
	}
	access, err := s.loadAccess(ctx, user.ID)
	if err != nil {
		return nil, fmt.Errorf("load user access: %w", err)
	}
	denied := make([]string, 0, len(relevant))
	allowed := make(map[string]bool, len(relevant))
	for _, permission := range relevant {
		if accessAllows(access, permission) {
			allowed[permission] = true
		} else {
			denied = append(denied, permission)
		}
	}
	if len(denied) == 0 {
		return relevant, nil
	}

	var restrictions []store.UserPermissionRestriction
	if err := s.db.WithContext(ctx).Where("user_id = ? AND active = ? AND permission_code IN ?", user.ID, true, denied).Find(&restrictions).Error; err != nil {
		return nil, fmt.Errorf("load permission restrictions: %w", err)
	}
	restrictionByCode := make(map[string]store.UserPermissionRestriction, len(restrictions))
	for _, restriction := range restrictions {
		restrictionByCode[restriction.PermissionCode] = restriction
	}
	now := time.Now().UTC()
	for _, permission := range denied {
		restriction, found := restrictionByCode[permission]
		if !found || restriction.ExpiresAt == nil || restriction.ExpiresAt.After(now) {
			continue // Missing evidence and permanent restrictions both fail closed.
		}
		lifted := false
		err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			result := tx.Model(&store.UserPermissionRestriction{}).Where("id = ? AND active = ? AND expires_at <= ?", restriction.ID, true, now).Update("active", false)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return nil
			}
			if err := tx.Model(&store.UserAccess{}).Where("user_id = ?", user.ID).Update(accessColumn(permission), true).Error; err != nil {
				return err
			}
			if err := tx.Create(&store.UserAccountEvent{UserID: user.ID, EventType: "restriction_removed", Detail: permission, CreatedAt: now}).Error; err != nil {
				return err
			}
			lifted = true
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("lift expired %s restriction: %w", permission, err)
		}
		if lifted {
			allowed[permission] = true
		}
	}
	permissions := make([]string, 0, len(relevant))
	for _, permission := range relevant {
		if allowed[permission] {
			permissions = append(permissions, permission)
		}
	}
	return permissions, nil
}

// SetPermissionRestriction changes durable permission facts, records the
// operator action, and forces both device sessions to establish fresh state.
func (s *Service) SetPermissionRestriction(ctx context.Context, userID uint64, permission, reason string, expiresAt *time.Time, operatorUserID *uint64) error {
	if !validPermission(permission) {
		return errors.New("unknown permission")
	}
	now := time.Now().UTC()
	if expiresAt != nil && !expiresAt.After(now) {
		return errors.New("restriction expiry must be in the future")
	}
	if _, err := s.loadAccess(ctx, userID); err != nil {
		return fmt.Errorf("load user access: %w", err)
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var access store.UserAccess
		if err := tx.Where("user_id = ?", userID).First(&access).Error; err != nil {
			return err
		}
		if err := tx.Model(&store.UserAccess{}).Where("user_id = ?", userID).Update(accessColumn(permission), false).Error; err != nil {
			return err
		}
		var current store.UserPermissionRestriction
		err := tx.Where("user_id = ? AND permission_code = ?", userID, permission).First(&current).Error
		restriction := store.UserPermissionRestriction{UserID: userID, PermissionCode: permission, Active: true, Reason: strings.TrimSpace(reason), OperatorUserID: operatorUserID, StartsAt: now, ExpiresAt: expiresAt}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if err := tx.Create(&restriction).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else if err := tx.Model(&current).Updates(map[string]any{"active": true, "reason": restriction.Reason, "operator_user_id": operatorUserID, "starts_at": now, "expires_at": expiresAt}).Error; err != nil {
			return err
		}
		return tx.Create(&store.UserAccountEvent{UserID: userID, EventType: "restriction_applied", OperatorUserID: operatorUserID, Detail: permission, CreatedAt: now}).Error
	}); err != nil {
		return err
	}
	return s.revokeUserSessions(ctx, userID)
}

func (s *Service) ClearPermissionRestriction(ctx context.Context, userID uint64, permission string, operatorUserID *uint64) error {
	if !validPermission(permission) {
		return errors.New("unknown permission")
	}
	now := time.Now().UTC()
	if _, err := s.loadAccess(ctx, userID); err != nil {
		return fmt.Errorf("load user access: %w", err)
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&store.UserAccess{}).Where("user_id = ?", userID).Update(accessColumn(permission), true).Error; err != nil {
			return err
		}
		if err := tx.Model(&store.UserPermissionRestriction{}).Where("user_id = ? AND permission_code = ?", userID, permission).Update("active", false).Error; err != nil {
			return err
		}
		return tx.Create(&store.UserAccountEvent{UserID: userID, EventType: "restriction_removed", OperatorUserID: operatorUserID, Detail: permission, CreatedAt: now}).Error
	}); err != nil {
		return err
	}
	return s.revokeUserSessions(ctx, userID)
}

func (s *Service) tokenPair(userID uint64, sessionID, refresh string, refreshExpiresAt time.Time) (TokenPair, error) {
	now := time.Now().UTC()
	accessExpiresAt := now.Add(s.cfg.AccessTokenTTL)
	access, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwtClaims{SessionID: sessionID, TokenType: accessTokenType, RegisteredClaims: jwt.RegisteredClaims{
		Issuer: s.cfg.Issuer, Subject: strconv.FormatUint(userID, 10), Audience: []string{s.cfg.Audience}, ExpiresAt: jwt.NewNumericDate(accessExpiresAt), IssuedAt: jwt.NewNumericDate(now), ID: mustRandomID(),
	}}).SignedString([]byte(s.cfg.Secret))
	if err != nil {
		return TokenPair{}, err
	}
	return TokenPair{AccessToken: access, RefreshToken: refresh, AccessExpiresAt: accessExpiresAt, RefreshExpiresAt: refreshExpiresAt}, nil
}

func (s *Service) consumeCode(ctx context.Context, identifier, purpose, code string) error {
	if !codePattern.MatchString(code) {
		return s.recordInvalidCode(ctx, identifier, purpose)
	}
	ok, err := s.redis.ConsumeIfEqual(ctx, verificationKey(identifier, purpose), s.codeHash(identifier, purpose, code))
	if err != nil || !ok {
		return s.recordInvalidCode(ctx, identifier, purpose)
	}
	if err := s.redis.Del(ctx, verificationAttemptKey(identifier, purpose)); err != nil {
		return ErrInvalidCode
	}
	return nil
}

func (s *Service) recordInvalidCode(ctx context.Context, email, purpose string) error {
	if s.redisAtomic == nil {
		return ErrInvalidCode
	}
	attempts, err := s.redisAtomic.IncrementWithTTL(ctx, verificationAttemptKey(email, purpose), s.cfg.EmailCodeTTL)
	if err != nil {
		return ErrInvalidCode
	}
	if attempts >= verificationMaxAttempts {
		_ = s.redis.Del(ctx, verificationKey(email, purpose), verificationAttemptKey(email, purpose))
	}
	return ErrInvalidCode
}

func (s *Service) recordPasswordFailure(ctx context.Context, email string) error {
	if s.redisAtomic == nil {
		return errors.New("Redis atomic operations are unavailable")
	}
	failures, err := s.redisAtomic.IncrementWithTTL(ctx, passwordFailureKey(email), passwordLockDuration)
	if err != nil {
		return err
	}
	if failures >= passwordMaxFailures {
		return s.redis.Set(ctx, passwordLockKey(email), "1", passwordLockDuration)
	}
	return nil
}

func (s *Service) codeHash(identifier, purpose, code string) string {
	mac := hmac.New(sha256.New, []byte(s.cfg.Secret))
	_, _ = mac.Write([]byte(identifier + ":" + purpose + ":" + code))
	return hex.EncodeToString(mac.Sum(nil))
}

func NormalizeEmail(raw string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if len(value) == 0 || len(value) > 254 {
		return "", errors.New("invalid email address")
	}
	parsed, err := mail.ParseAddress(value)
	if err != nil || parsed.Address != value {
		return "", errors.New("invalid email address")
	}
	return value, nil
}

func NormalizeDeviceType(raw string) (string, error) {
	deviceType := strings.ToLower(strings.TrimSpace(raw))
	if deviceType != DevicePC && deviceType != DeviceMobile {
		return "", errors.New("device type must be pc or mobile")
	}
	return deviceType, nil
}

type SessionAuthorizer interface {
	Authenticate(context.Context, Claims) (store.User, error)
}

type principalSessionAuthorizer interface {
	AuthenticatePrincipal(context.Context, Claims) (AuthenticatedIdentity, error)
}

func Require(v Verifier, authorizers ...SessionAuthorizer) gin.HandlerFunc {
	return func(c *gin.Context) {
		parts := strings.Fields(c.GetHeader("Authorization"))
		if len(parts) != 2 && isWebSocketUpgrade(c.Request) {
			parts = strings.Split(c.GetHeader("Sec-WebSocket-Protocol"), ",")
			for index := range parts {
				parts[index] = strings.TrimSpace(parts[index])
			}
		}
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") || v == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			return
		}
		claims, err := v.Verify(parts[1])
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}
		if len(authorizers) > 0 {
			var user store.User
			var principal Principal
			var err error
			if principalAuthorizer, ok := authorizers[0].(principalSessionAuthorizer); ok {
				identity, identityErr := principalAuthorizer.AuthenticatePrincipal(c.Request.Context(), claims)
				user, principal, err = identity.User, identity.Principal, identityErr
			} else {
				user, err = authorizers[0].Authenticate(c.Request.Context(), claims)
			}
			if err != nil {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "session is no longer active"})
				return
			}
			c.Set("auth.user", user)
			if principal.UserID() != "" {
				c.Set("auth.principal", &principal)
			}
		}
		c.Set("auth.claims", claims)
		c.Next()
	}
}

// Browsers cannot attach Authorization to a native WebSocket handshake. The
// token is carried in the WebSocket subprotocol header, never in the URL.
func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

func RequireRole(roles ...string) gin.HandlerFunc {
	allowed := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		allowed[role] = struct{}{}
	}
	return func(c *gin.Context) {
		if value, ok := c.Get("auth.principal"); ok {
			principal, valid := value.(*Principal)
			if !valid || principal == nil {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
				return
			}
			if _, ok := allowed[principal.Role()]; !ok {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "permission denied"})
				return
			}
			c.Next()
			return
		}
		user, ok := c.Get("auth.user")
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			return
		}
		if _, ok := allowed[user.(store.User).Role]; !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "permission denied"})
			return
		}
		c.Next()
	}
}

// RequireEndpoint is the uniform handler-entry authorization layer. Services
// must still call ServiceAuthorizer before constructing their database query.
func RequireEndpoint(authorizer EndpointAuthorizer, rule EndpointRule) gin.HandlerFunc {
	return func(c *gin.Context) {
		value, ok := c.Get("auth.principal")
		principal, valid := value.(*Principal)
		if !ok || !valid || authorizer.Authorize(principal, &rule) != nil {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "permission denied"})
			return
		}
		c.Next()
	}
}

// PrincipalFromContext provides the immutable identity required by service
// authorization. It returns nil when authentication middleware was not used.
func PrincipalFromContext(c *gin.Context) *Principal {
	if c == nil {
		return nil
	}
	value, ok := c.Get("auth.principal")
	if !ok {
		return nil
	}
	principal, _ := value.(*Principal)
	return principal
}

func RegisterPublicRoutes(r *gin.RouterGroup, service *Service) {
	r.POST("/email/send", func(c *gin.Context) {
		var request struct {
			Email   string `json:"email"`
			Purpose string `json:"purpose"`
		}
		if err := c.ShouldBindJSON(&request); err != nil || service.SendEmailCode(c.Request.Context(), request.Email, request.Purpose) != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "unable to send verification code"})
			return
		}
		c.Status(http.StatusNoContent)
	})
	r.POST("/register", func(c *gin.Context) {
		var request struct {
			Email       string `json:"email"`
			Code        string `json:"code"`
			DisplayName string `json:"display_name"`
			Password    string `json:"password"`
			DeviceType  string `json:"device_type"`
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}
		pair, err := service.Register(c.Request.Context(), request.Email, request.Code, request.DisplayName, request.Password, request.DeviceType)
		writeTokenPair(c, pair, err)
	})
	r.POST("/login/email", func(c *gin.Context) {
		var request struct {
			Email      string `json:"email"`
			Code       string `json:"code"`
			DeviceType string `json:"device_type"`
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}
		pair, err := service.LoginByEmail(c.Request.Context(), request.Email, request.Code, request.DeviceType)
		writeTokenPair(c, pair, err)
	})
	r.POST("/login/password", func(c *gin.Context) {
		var request struct {
			Email      string `json:"email"`
			Password   string `json:"password"`
			DeviceType string `json:"device_type"`
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}
		pair, err := service.LoginByPassword(c.Request.Context(), request.Email, request.Password, request.DeviceType)
		writeTokenPair(c, pair, err)
	})
	r.POST("/refresh", func(c *gin.Context) {
		var request struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}
		pair, err := service.Refresh(c.Request.Context(), request.RefreshToken)
		writeTokenPair(c, pair, err)
	})
}

func RegisterRoutes(r *gin.RouterGroup, service *Service) {
	r.GET("/me", func(c *gin.Context) {
		claims, _ := c.Get("auth.claims")
		user, err := service.Me(c.Request.Context(), claims.(Claims))
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "account unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"id": strconv.FormatUint(user.ID, 10), "display_name": user.DisplayName, "role": user.Role, "email_verified": user.EmailVerifiedAt != nil, "phone_bound": user.PhoneVerifiedAt != nil})
	})
	r.POST("/logout", func(c *gin.Context) {
		claims, _ := c.Get("auth.claims")
		if err := service.Logout(c.Request.Context(), claims.(Claims)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "logout failed"})
			return
		}
		c.Status(http.StatusNoContent)
	})
	r.POST("/logout-all", func(c *gin.Context) {
		claims, _ := c.Get("auth.claims")
		if err := service.LogoutAll(c.Request.Context(), claims.(Claims)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "logout failed"})
			return
		}
		c.Status(http.StatusNoContent)
	})
	r.GET("/sessions", func(c *gin.Context) {
		claims, _ := c.Get("auth.claims")
		sessions, err := service.Sessions(c.Request.Context(), claims.(Claims))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "unable to list sessions"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"sessions": sessions})
	})
	r.DELETE("/sessions/:id", func(c *gin.Context) {
		claims, _ := c.Get("auth.claims")
		if err := service.RevokeSession(c.Request.Context(), claims.(Claims), c.Param("id")); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "session revocation failed"})
			return
		}
		c.Status(http.StatusNoContent)
	})
	admin := r.Group("/admin")
	admin.Use(RequireEndpoint(DefaultEndpointAuthorizer(), EndpointRule{Role: RoleAdmin, Permission: PermissionManageUser}))
	admin.PUT("/users/:id/role", func(c *gin.Context) {
		userID, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user id"})
			return
		}
		var request struct {
			Role string `json:"role"`
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}
		if err := service.SetRole(c.Request.Context(), userID, request.Role); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "role update failed"})
			return
		}
		c.Status(http.StatusNoContent)
	})
}

func writeTokenPair(c *gin.Context, pair TokenPair, err error) {
	if err == nil {
		c.JSON(http.StatusOK, pair)
		return
	}
	if errors.Is(err, ErrEmailNotRegistered) || errors.Is(err, ErrInvalidCode) || errors.Is(err, ErrInvalidRefresh) || errors.Is(err, ErrInvalidPassword) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication failed"})
		return
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": "authentication request failed"})
}

func verificationKey(email, purpose string) string { return "auth:email:" + purpose + ":" + email }
func verificationAttemptKey(email, purpose string) string {
	return "auth:email-attempt:" + purpose + ":" + email
}
func emailCooldownKey(email, purpose string) string {
	return "auth:email-cooldown:" + purpose + ":" + email
}
func emailDailyKey(email, purpose string) string { return "auth:email-daily:" + purpose + ":" + email }
func passwordFailureKey(email string) string     { return "auth:password-failure:" + email }
func passwordLockKey(email string) string        { return "auth:password-lock:" + email }
func sessionKey(id string) string                { return "auth:session:" + id }
func deviceSlotKey(userID uint64, deviceType string) string {
	return "auth:user:" + strconv.FormatUint(userID, 10) + ":device:" + deviceType
}
func refreshIndexKey(hash string) string  { return "auth:refresh-index:" + hash }
func refreshReplayKey(hash string) string { return "auth:refresh-replay:" + hash }
func redisSessionKeys(userID uint64, deviceType, sessionID, refreshHash string) platform.RedisSessionKeys {
	return platform.RedisSessionKeys{
		SlotKey: deviceSlotKey(userID, deviceType), SessionKeyPrefix: "auth:session:", SessionKey: sessionKey(sessionID),
		RefreshIndexKey: refreshIndexKey(refreshHash), SID: sessionID,
	}
}
func tokenHashHex(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
func maskEmail(email string) string {
	if at := strings.LastIndexByte(email, '@'); at > 1 {
		return email[:1] + "***" + email[at:]
	}
	return "***"
}
func randomToken(bytes int) (string, error) {
	raw := make([]byte, bytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}
func newUUIDv4() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	encoded := hex.EncodeToString(raw)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}
func randomDigits(count int) (string, error) {
	raw := make([]byte, count)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	for i := range raw {
		raw[i] = '0' + raw[i]%10
	}
	return string(raw), nil
}
func mustRandomID() string {
	value, err := randomToken(12)
	if err != nil {
		panic(err)
	}
	return value
}

func hashPassword(password string) ([]byte, error) {
	if len(password) < 8 || len(password) > 72 {
		return nil, errors.New("password must be between 8 and 72 bytes")
	}
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}
