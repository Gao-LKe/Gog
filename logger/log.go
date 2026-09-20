package logger

import (
	"os"

	"github.com/gao66666/GoBlog/setting"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Encoder 日志编码器,日志的格式
// WriteSyncer 日志写入器，写到哪里
//组合Encoder和WriteSyncer可以构建出Core，于此同时提供过滤逻辑，使得不同级别的日志可以被写入到不同的目的地

var lg *zap.Logger

// Init 初始化lg
func Init(cfg *setting.LogConfig, mode string) (err error) {
	var core zapcore.Core

	if mode == "dev" {
		// 开发模式：只输出到控制台，用友好的格式，记录所有Debug及以上日志
		core = zapcore.NewCore(
			getDevEncoder(),
			zapcore.Lock(os.Stdout),
			zapcore.DebugLevel, // 开发环境固定记录所有日志
		)
	} else {
		// 生产/其他模式：按级别分离存储
		core = getProductionCore(cfg)
	}

	lg = zap.New(core, zap.AddCaller())
	zap.ReplaceGlobals(lg)
	zap.L().Info("init logger success", zap.String("mode", mode))
	return
}
