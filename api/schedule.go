package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gaucho-racing/foreman/model"
	"github.com/gaucho-racing/foreman/service"
	"github.com/gin-gonic/gin"
)

// scheduleRequest is the create + update body. Update is a full
// replace — every field is sent on PUT.
type scheduleRequest struct {
	Kind        string          `json:"kind" binding:"required"`
	Queue       string          `json:"queue"`
	Service     string          `json:"service"`
	Params      json.RawMessage `json:"params"`
	Priority    int             `json:"priority"`
	MaxAttempts int             `json:"max_attempts"`
	CronExpr    string          `json:"cron_expr" binding:"required"`
	Timezone    string          `json:"timezone"`
	Enabled     *bool           `json:"enabled"`
}

func (r scheduleRequest) toParams() service.ScheduleParams {
	p := service.ScheduleParams{
		Kind:        r.Kind,
		Queue:       r.Queue,
		Service:     r.Service,
		Params:      model.JSON(r.Params),
		Priority:    r.Priority,
		MaxAttempts: r.MaxAttempts,
		CronExpr:    r.CronExpr,
		Timezone:    r.Timezone,
		// Default Enabled=true so the common case (create + run) doesn't
		// require sending "enabled": true on every POST.
		Enabled: true,
	}
	if r.Enabled != nil {
		p.Enabled = *r.Enabled
	}
	return p
}

func CreateSchedule(c *gin.Context) {
	var req scheduleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s, err := service.CreateSchedule(req.toParams())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, s)
}

func GetSchedule(c *gin.Context) {
	s, err := service.GetSchedule(c.Param("id"))
	if respondScheduleErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, s)
}

func ListSchedules(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	f := service.ListSchedulesFilter{
		Kind:   c.Query("kind"),
		Limit:  limit,
		Cursor: c.Query("cursor"),
	}
	if e := c.Query("enabled"); e != "" {
		v := e == "true"
		f.Enabled = &v
	}
	out, err := service.ListSchedules(f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, out)
}

func UpdateSchedule(c *gin.Context) {
	var req scheduleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s, err := service.UpdateSchedule(c.Param("id"), req.toParams())
	if respondScheduleErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, s)
}

func DeleteSchedule(c *gin.Context) {
	err := service.DeleteSchedule(c.Param("id"))
	if respondScheduleErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": c.Param("id")})
}

// FireSchedule enqueues one Job using the schedule's recipe without
// disturbing NextFireAt. Useful for "run this now" from the dashboard.
func FireSchedule(c *gin.Context) {
	job, err := service.FireSchedule(c.Param("id"))
	if respondScheduleErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, job)
}

func respondScheduleErr(c *gin.Context, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, service.ErrScheduleNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	default:
		// Validation errors (bad cron, bad tz) surface as the service's
		// generic error path — pick 400 since they reflect bad input.
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	return true
}
