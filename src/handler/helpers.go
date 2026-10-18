package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Helpers shared by the lab and queue handlers.

// fail answers 500 with the raw error. Lab-only: a real service would log
// err and return a generic message instead of leaking driver details.
func fail(c *gin.Context, err error) {
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}

func parsePositiveInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, strconv.ErrSyntax
	}
	return n, nil
}

func leaseSeconds(c *gin.Context) int {
	if v := c.Query("lease_seconds"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			return n
		}
	}
	return defaultLeaseSeconds
}
