package handler

import (
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
)

const vueDistIndex = "webapp/dist/index.html"

// VueSPAReady 是否已构建 Vue 前端（webapp/dist/index.html 存在）。
func VueSPAReady() bool {
	_, err := os.Stat(vueDistIndex)
	return err == nil
}

// ServeVueSPA 返回 Vue 构建产物入口（需先 cd webapp && npm run build）。
func ServeVueSPA(c *gin.Context) {
	if !VueSPAReady() {
		c.String(http.StatusServiceUnavailable, "Vue 前端未构建：请在 webapp 目录执行 npm install && npm run build")
		return
	}
	c.File(vueDistIndex)
}

func servePage(c *gin.Context, tmpl string, data gin.H) {
	if VueSPAReady() {
		ServeVueSPA(c)
		return
	}
	c.HTML(http.StatusOK, tmpl, data)
}

func HomePage(c *gin.Context) {
	servePage(c, "home.tmpl", gin.H{})
}

func LoginPage(c *gin.Context) {
	servePage(c, "login.tmpl", gin.H{})
}

func MePage(c *gin.Context) {
	servePage(c, "me.tmpl", gin.H{})
}

func PointsMallPage(c *gin.Context) {
	servePage(c, "me.tmpl", gin.H{})
}

func UserPage(c *gin.Context) {
	servePage(c, "user.tmpl", gin.H{"UserID": c.Param("id")})
}

func DMPage(c *gin.Context) {
	servePage(c, "dm.tmpl", gin.H{"PeerID": c.Query("peer_id")})
}

func ArticlePage(c *gin.Context) {
	servePage(c, "article.tmpl", gin.H{"ArticleID": c.Param("id")})
}

func EditorPage(c *gin.Context) {
	servePage(c, "editor.tmpl", gin.H{"ArticleID": c.Query("id")})
}

func TopicsPage(c *gin.Context) {
	servePage(c, "topics.tmpl", gin.H{})
}

func TopicPage(c *gin.Context) {
	servePage(c, "topic.tmpl", gin.H{"TopicID": c.Param("id")})
}

func TopicDiscussPage(c *gin.Context) {
	servePage(c, "topic_discuss.tmpl", gin.H{"TopicID": c.Param("id")})
}

func GamesLibraryPage(c *gin.Context) {
	servePage(c, "game_library.tmpl", gin.H{})
}

func GameDetailPage(c *gin.Context) {
	servePage(c, "game_detail.tmpl", gin.H{"GameID": c.Param("id")})
}

// RegisterVueAssets 挂载 Vite 构建的 /assets（若存在）。
func RegisterVueAssets(r *gin.Engine) {
	assetsDir := filepath.Join("webapp", "dist", "assets")
	if st, err := os.Stat(assetsDir); err != nil || !st.IsDir() {
		return
	}
	r.Static("/assets", assetsDir)
}
