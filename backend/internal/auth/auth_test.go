package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goblog/backend/internal/platform"
	"github.com/goblog/backend/internal/store"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const testUserID uint64 = 803113126182410001

func TestJWTVerifier(t *testing.T) {
	secret := "test-secret"
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "user-1", "sid": "session-1", "typ": "access", "iss": "goblog", "aud": "goblog-api",
		"exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(), "jti": "token-1",
	})
	raw, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	claims, err := NewJWTVerifier(secret).Verify(raw)
	if err != nil || claims.Subject != "user-1" || claims.SessionID != "session-1" {
		t.Fatalf("unexpected claims: %#v, %v", claims, err)
	}
}

func TestVerificationCodeIsPurposeScopedAndSingleUse(t *testing.T) {
	service, cache := testService(t)
	email := "person@example.com"
	code := "123456"
	ctx := context.Background()
	if err := cache.Set(ctx, verificationKey(email, registerPurpose), service.codeHash(email, registerPurpose, code), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := service.consumeCode(ctx, email, loginPurpose, code); !errors.Is(err, ErrInvalidCode) {
		t.Fatalf("expected scoped code rejection, got %v", err)
	}
	if err := service.consumeCode(ctx, email, registerPurpose, code); err != nil {
		t.Fatal(err)
	}
	if err := service.consumeCode(ctx, email, registerPurpose, code); !errors.Is(err, ErrInvalidCode) {
		t.Fatalf("expected consumed code rejection, got %v", err)
	}
}

func TestRefreshTokenCanOnlyRotateOnce(t *testing.T) {
	service, cache := testService(t)
	ctx := context.Background()
	email := "refresh@example.com"
	now := time.Now().UTC()
	user := store.User{ID: testUserID, DisplayName: "测试用户", Email: &email, EmailVerifiedAt: &now, Role: RoleUser, Status: "active"}
	if err := service.db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	pair, err := service.createSession(ctx, user, DevicePC, "test")
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Refresh(ctx, pair.RefreshToken)
	if err != nil || first.RefreshToken == pair.RefreshToken {
		t.Fatalf("first rotation failed: %#v %v", first, err)
	}
	if _, err := service.Refresh(ctx, pair.RefreshToken); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("old refresh token was accepted: %v", err)
	}
	if _, active, err := cache.GetSession(ctx, sessionKeyFromAccess(t, service, first.AccessToken)); err != nil || active {
		t.Fatal("refresh token replay did not revoke the active session")
	}
}

func TestPasswordLoginRejectsIncorrectPassword(t *testing.T) {
	service, _ := testService(t)
	ctx := context.Background()
	email := "password@example.com"
	hash, err := hashPassword("correct-password")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	user := store.User{ID: testUserID, DisplayName: "测试用户", Email: &email, EmailVerifiedAt: &now, PasswordHash: hash, Role: RoleUser, Status: "active"}
	if err := service.db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.LoginByPassword(ctx, email, "wrong-password", DevicePC); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("expected invalid password, got %v", err)
	}
	if _, err := service.LoginByPassword(ctx, email, "correct-password", DevicePC); err != nil {
		t.Fatal(err)
	}
}

func TestPasswordFailuresLockTheEmailTemporarily(t *testing.T) {
	service, cache := testService(t)
	ctx := context.Background()
	email := "locked@example.com"
	hash, err := hashPassword("correct-password")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	user := store.User{ID: testUserID, DisplayName: "测试用户", Email: &email, EmailVerifiedAt: &now, PasswordHash: hash, Role: RoleUser, Status: "active"}
	if err := service.db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	for range passwordMaxFailures {
		if _, err := service.LoginByPassword(ctx, email, "wrong-password", DevicePC); !errors.Is(err, ErrInvalidPassword) {
			t.Fatalf("expected rejected password, got %v", err)
		}
	}
	if locked, _ := cache.Has(ctx, passwordLockKey(email)); !locked {
		t.Fatal("password lock was not created")
	}
	if _, err := service.LoginByPassword(ctx, email, "correct-password", DevicePC); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("locked account accepted correct password: %v", err)
	}
}

func TestNewSessionReplacesOnlyTheSameDeviceClass(t *testing.T) {
	service, _ := testService(t)
	ctx := context.Background()
	email := "devices@example.com"
	now := time.Now().UTC()
	user := store.User{ID: testUserID, DisplayName: "测试用户", Email: &email, EmailVerifiedAt: &now, Role: RoleUser, Status: "active"}
	if err := service.db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	firstPC, err := service.createSession(ctx, user, DevicePC, "test")
	if err != nil {
		t.Fatal(err)
	}
	mobile, err := service.createSession(ctx, user, DeviceMobile, "test")
	if err != nil {
		t.Fatal(err)
	}
	secondPC, err := service.createSession(ctx, user, DevicePC, "test")
	if err != nil {
		t.Fatal(err)
	}
	firstClaims, _ := service.signer.Verify(firstPC.AccessToken)
	mobileClaims, _ := service.signer.Verify(mobile.AccessToken)
	secondClaims, _ := service.signer.Verify(secondPC.AccessToken)
	if _, err := service.Authenticate(ctx, firstClaims); err == nil {
		t.Fatal("old PC session remains active")
	}
	if _, err := service.Authenticate(ctx, mobileClaims); err != nil {
		t.Fatalf("mobile session should stay active: %v", err)
	}
	if _, err := service.Authenticate(ctx, secondClaims); err != nil {
		t.Fatalf("latest PC session should stay active: %v", err)
	}
}

func TestRoleMiddlewareRejectsOtherRoles(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, testcase := range []struct {
		role   string
		status int
	}{{RoleSupport, http.StatusForbidden}, {RoleAdmin, http.StatusNoContent}} {
		router := gin.New()
		router.Use(func(c *gin.Context) { c.Set("auth.user", store.User{Role: testcase.role}) })
		router.GET("/admin", RequireRole(RoleAdmin), func(c *gin.Context) { c.Status(http.StatusNoContent) })
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin", nil))
		if response.Code != testcase.status {
			t.Fatalf("role %s: expected %d, got %d", testcase.role, testcase.status, response.Code)
		}
	}
}

func TestRoleChangeRevokesActiveSession(t *testing.T) {
	service, _ := testService(t)
	ctx := context.Background()
	email := "role-sync@example.com"
	now := time.Now().UTC()
	user := store.User{ID: testUserID, DisplayName: "测试用户", Email: &email, EmailVerifiedAt: &now, Role: RoleUser, Status: "active"}
	if err := service.db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	pair, err := service.createSession(ctx, user, DevicePC, "test")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := service.signer.Verify(pair.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetRole(ctx, user.ID, RoleSupport); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Authenticate(ctx, claims); err == nil {
		t.Fatal("role change did not revoke the old session")
	}
}

func TestNewSessionLazilyLiftsExpiredRelevantRestriction(t *testing.T) {
	service, cache := testService(t)
	ctx := context.Background()
	email := "restriction@example.com"
	now := time.Now().UTC()
	user := store.User{ID: testUserID, DisplayName: "测试用户", Email: &email, EmailVerifiedAt: &now, Role: RoleUser, Status: "active"}
	if err := service.db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	access := newFullAccess(user.ID)
	if err := service.db.Create(&access).Error; err != nil {
		t.Fatal(err)
	}
	if err := service.db.Model(&access).Update("can_sell", false).Error; err != nil {
		t.Fatal(err)
	}
	expiredAt := now.Add(-time.Minute)
	restriction := store.UserPermissionRestriction{UserID: user.ID, PermissionCode: PermissionSell, Active: true, StartsAt: now.Add(-time.Hour), ExpiresAt: &expiredAt}
	if err := service.db.Create(&restriction).Error; err != nil {
		t.Fatal(err)
	}
	pair, err := service.createSession(ctx, user, DevicePC, "test")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := service.signer.Verify(pair.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	session, active, err := cache.GetSession(ctx, sessionKey(claims.SessionID))
	if err != nil || !active || !containsPermission(session.Permissions, PermissionSell) {
		t.Fatalf("expired sell restriction was not lifted in new session: %#v, active=%v, err=%v", session, active, err)
	}
	if err := service.db.First(&access, user.ID).Error; err != nil || !access.CanSell {
		t.Fatalf("durable sell access was not restored: %#v, %v", access, err)
	}
	if err := service.db.First(&restriction, restriction.ID).Error; err != nil || restriction.Active {
		t.Fatalf("expired restriction remains active: %#v, %v", restriction, err)
	}
}

func TestNewSessionDoesNotResolveRoleExternalRestriction(t *testing.T) {
	service, _ := testService(t)
	ctx := context.Background()
	email := "external-restriction@example.com"
	now := time.Now().UTC()
	user := store.User{ID: testUserID, DisplayName: "测试用户", Email: &email, EmailVerifiedAt: &now, Role: RoleUser, Status: "active"}
	if err := service.db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	access := newFullAccess(user.ID)
	if err := service.db.Create(&access).Error; err != nil {
		t.Fatal(err)
	}
	if err := service.db.Model(&access).Update("can_manage_user", false).Error; err != nil {
		t.Fatal(err)
	}
	expiredAt := now.Add(-time.Minute)
	restriction := store.UserPermissionRestriction{UserID: user.ID, PermissionCode: PermissionManageUser, Active: true, StartsAt: now.Add(-time.Hour), ExpiresAt: &expiredAt}
	if err := service.db.Create(&restriction).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.createSession(ctx, user, DevicePC, "test"); err != nil {
		t.Fatal(err)
	}
	if err := service.db.First(&restriction, restriction.ID).Error; err != nil || !restriction.Active {
		t.Fatalf("role-external restriction was unexpectedly resolved: %#v, %v", restriction, err)
	}
}

func TestSessionIDUsesUUIDv4(t *testing.T) {
	id, err := newUUIDv4()
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 36 || id[14] != '4' || id[19] != '8' && id[19] != '9' && id[19] != 'a' && id[19] != 'b' {
		t.Fatalf("not a UUIDv4: %q", id)
	}
}

func containsPermission(permissions []string, target string) bool {
	for _, permission := range permissions {
		if permission == target {
			return true
		}
	}
	return false
}

func testService(t *testing.T) (*Service, *memoryRedis) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:auth-"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&store.User{}, &store.UserAccess{}, &store.UserPermissionRestriction{}, &store.UserAccountEvent{}, &store.UserLoginEvent{}, &store.UserBalance{}, &store.BalanceTransaction{}); err != nil {
		t.Fatal(err)
	}
	cache := &memoryRedis{values: map[string]string{}, sessions: map[string]platform.RedisSessionRecord{}}
	return NewService(db, cache, cache, logEmailSender{}, ServiceConfig{Secret: "test-secret", Issuer: "goblog", Audience: "goblog-api", AccessTokenTTL: time.Minute, RefreshTokenTTL: time.Hour, EmailCodeTTL: time.Minute}), cache
}

func sessionKeyFromAccess(t *testing.T, service *Service, token string) string {
	t.Helper()
	claims, err := service.signer.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	return sessionKey(claims.SessionID)
}

type memoryRedis struct {
	mu       sync.Mutex
	values   map[string]string
	sessions map[string]platform.RedisSessionRecord
}

func (m *memoryRedis) Ping(context.Context) error { return nil }
func (m *memoryRedis) Set(_ context.Context, key string, value any, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = value.(string)
	return nil
}
func (m *memoryRedis) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[key]
	if !ok {
		return "", errors.New("missing")
	}
	return value, nil
}
func (m *memoryRedis) Has(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.values[key]
	return ok, nil
}
func (m *memoryRedis) Del(_ context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		delete(m.values, key)
	}
	return nil
}
func (m *memoryRedis) DeleteIfEqual(_ context.Context, key, expected string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values[key] != expected {
		return false, nil
	}
	delete(m.values, key)
	return true, nil
}
func (m *memoryRedis) ConsumeIfEqual(_ context.Context, key, expected string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values[key] != expected {
		return false, nil
	}
	delete(m.values, key)
	return true, nil
}

func (m *memoryRedis) SetIfNotExists(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.values[key]; exists {
		return false, nil
	}
	m.values[key] = value
	return true, nil
}

func (m *memoryRedis) DecrementIfExists(_ context.Context, key string, amount uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, exists := m.values[key]
	if !exists {
		return false, nil
	}
	available, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		delete(m.values, key)
		return false, nil
	}
	if amount >= available {
		m.values[key] = "0"
		return true, nil
	}
	m.values[key] = strconv.FormatUint(available-amount, 10)
	return true, nil
}

func (m *memoryRedis) Increment(_ context.Context, key string, _ time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, _ := strconv.ParseInt(m.values[key], 10, 64)
	value++
	m.values[key] = strconv.FormatInt(value, 10)
	return value, nil
}

func (m *memoryRedis) ReplaceSession(_ context.Context, keys platform.RedisSessionKeys, record platform.RedisSessionRecord, _ time.Duration) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.values[keys.SlotKey]
	if old != "" {
		delete(m.sessions, keys.SessionKeyPrefix+old)
	}
	m.values[keys.SlotKey] = keys.SID
	m.values[keys.RefreshIndexKey] = keys.SID
	m.sessions[keys.SessionKey] = record
	return old, nil
}

func (m *memoryRedis) GetSession(_ context.Context, key string) (platform.RedisSessionRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.sessions[key]
	return record, ok, nil
}

func (m *memoryRedis) DeleteSessionIfCurrent(_ context.Context, keys platform.RedisSessionKeys) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values[keys.SlotKey] != keys.SID {
		return false, nil
	}
	delete(m.values, keys.SlotKey)
	delete(m.sessions, keys.SessionKey)
	return true, nil
}

func (m *memoryRedis) RotateRefreshHash(_ context.Context, keys platform.RedisRefreshKeys, currentHash, newHash string, _ time.Duration) (platform.RefreshRotationResult, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.sessions[keys.SessionKey]
	if ok && record.RefreshHash == currentHash {
		record.RefreshHash = newHash
		m.sessions[keys.SessionKey] = record
		m.values[keys.ReplayKey] = keys.SID
		m.values[keys.NewRefreshIndex] = keys.SID
		return platform.RefreshRotationSucceeded, "", nil
	}
	if sid := m.values[keys.ReplayKey]; sid != "" {
		return platform.RefreshRotationReplay, sid, nil
	}
	return platform.RefreshRotationRejected, "", nil
}

func (m *memoryRedis) IncrementWithTTL(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	return m.Increment(ctx, key, ttl)
}
