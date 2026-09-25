package monitoring

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHTTPMiddlewareCountsStatusAndRouteTemplate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	metrics := NewMetrics()
	router := gin.New()
	router.Use(metrics.HTTPMiddleware())
	router.GET("/items/:id", func(c *gin.Context) { c.Status(http.StatusOK) })
	router.GET("/limited", func(c *gin.Context) { c.Status(http.StatusTooManyRequests) })
	router.GET("/broken", func(c *gin.Context) { c.Status(http.StatusInternalServerError) })
	router.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	for _, path := range []string{"/items/secret-123", "/limited", "/broken", "/healthz"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", response.Code)
	}
	body := response.Body.String()
	for _, expected := range []string{
		`goblog_http_received_total 3`,
		`goblog_http_completed_total{method="GET",route="/items/:id",status_class="2xx"} 1`,
		`goblog_http_completed_total{method="GET",route="/limited",status_class="429"} 1`,
		`goblog_http_completed_total{method="GET",route="/broken",status_class="5xx"} 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("missing metric %q", expected)
		}
	}
	if strings.Contains(body, "secret-123") || strings.Contains(body, `route="/healthz"`) {
		t.Error("metrics exposed an identifier or health probe")
	}
}
