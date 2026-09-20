package handler

import (
	"strconv"
	"strings"

	"github.com/gao66666/GoBlog/models"
	"github.com/gao66666/GoBlog/service"
	"github.com/gao66666/GoBlog/tool"
	"github.com/gin-gonic/gin"
)

// InternalHandler 供后台脚本使用的内部 API（不经前端、不走 JWT）。
type InternalHandler struct {
	gameSvc  *service.GameService
	topicSvc *service.TopicService
}

func NewInternalHandler(gameSvc *service.GameService, topicSvc *service.TopicService) *InternalHandler {
	return &InternalHandler{
		gameSvc:  gameSvc,
		topicSvc: topicSvc,
	}
}

// CreateGameInternal 上新游戏并自动创建/绑定「游戏:名称」话题。
func (h *InternalHandler) CreateGameInternal(c *gin.Context) {
	var p models.ParamCreateGame
	if err := c.ShouldBindJSON(&p); err != nil {
		tool.ResponseError(c, ErrCodeInvalidParam)
		return
	}
	game, err := buildGameFromCreateParam(&p)
	if err != nil {
		tool.ResponseError(c, ErrCodeInvalidParam)
		return
	}
	topicID, err := h.gameSvc.CreateGameWithTopic(game)
	if err != nil {
		tool.ResponseError(c, err)
		return
	}
	tool.ResponseSuccess(c, gin.H{
		"game_id":  strconv.FormatUint(game.ID, 10),
		"topic_id": topicID,
	}, "创建成功")
}

// CreateTopicInternal 创建长期话题（非临时），用于后台上新。
func (h *InternalHandler) CreateTopicInternal(c *gin.Context) {
	var body struct {
		Name string `json:"name" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		tool.ResponseError(c, ErrCodeInvalidParam)
		return
	}
	t, err := h.topicSvc.CreateTopic(strings.TrimSpace(body.Name), false)
	if err != nil {
		tool.ResponseError(c, err)
		return
	}
	tool.ResponseSuccess(c, gin.H{"topic_id": t.ID, "name": t.Name}, "创建成功")
}

func buildGameFromCreateParam(p *models.ParamCreateGame) (*models.Game, error) {
	releaseAt, err := parseDate(p.ReleaseAt)
	if err != nil {
		return nil, err
	}
	priceCents := int64(-1)
	if p.PriceCents != nil {
		priceCents = *p.PriceCents
	}
	return &models.Game{
		ID:          tool.GenerateID(),
		Name:        p.Name,
		Description: p.Description,
		ReleaseAt:   releaseAt,
		Publisher:   p.Publisher,
		Developer:   p.Developer,
		CoverURL:    strings.TrimSpace(p.CoverURL),
		PriceCents:  priceCents,
		Tags:        encodeGameTags(p.Tags),
	}, nil
}
