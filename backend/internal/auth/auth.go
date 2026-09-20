package auth

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	Subject string
	Roles   []string
}

type Verifier interface {
	Verify(token string) (Claims, error)
}

type JWTVerifier struct {
	secret []byte
}

func NewJWTVerifier(secret string) *JWTVerifier {
	return &JWTVerifier{secret: []byte(secret)}
}

func (v *JWTVerifier) Verify(raw string) (Claims, error) {
	token, err := jwt.Parse(raw, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, errors.New("unexpected signing method")
		}
		return v.secret, nil
	})
	if err != nil || !token.Valid {
		return Claims{}, errors.New("invalid token")
	}
	subject, err := token.Claims.GetSubject()
	if err != nil || subject == "" {
		return Claims{}, errors.New("missing subject")
	}
	return Claims{Subject: subject}, nil
}

func Require(v Verifier) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		parts := strings.Fields(header)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") || v == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			return
		}
		claims, err := v.Verify(parts[1])
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}
		c.Set("auth.claims", claims)
		c.Next()
	}
}

func RegisterRoutes(r *gin.RouterGroup) {
	r.GET("/me", func(c *gin.Context) {
		claims, _ := c.Get("auth.claims")
		c.JSON(http.StatusOK, gin.H{"claims": claims})
	})
}
