package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestJWTVerifier(t *testing.T) {
	secret := "test-secret"
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "user-1", "exp": time.Now().Add(time.Minute).Unix()})
	raw, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	claims, err := NewJWTVerifier(secret).Verify(raw)
	if err != nil || claims.Subject != "user-1" {
		t.Fatalf("unexpected claims: %#v, %v", claims, err)
	}
}
