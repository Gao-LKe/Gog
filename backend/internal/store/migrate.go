package store

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func OpenMySQL(dsn string) (*gorm.DB, error) {
	return gorm.Open(mysql.Open(dsn), &gorm.Config{})
}

func Migrate(db *gorm.DB) error {
	var err error
	if db.Dialector.Name() == "mysql" {
		err = db.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4").AutoMigrate(Models()...)
	} else {
		err = db.AutoMigrate(Models()...)
	}
	if err != nil {
		return err
	}
	return backfillConversationMessageRecipients(db)
}

// backfillConversationMessageRecipients preserves existing chat history when
// recipient_user_id is introduced after messages have already been stored.
func backfillConversationMessageRecipients(db *gorm.DB) error {
	for {
		var messages []ConversationMessage
		if err := db.Where("recipient_user_id = ?", 0).Limit(100).Find(&messages).Error; err != nil {
			return err
		}
		if len(messages) == 0 {
			return nil
		}
		for _, message := range messages {
			var conversation Conversation
			if err := db.First(&conversation, message.ConversationID).Error; err != nil {
				return err
			}
			recipientUserID := conversation.PublisherUserID
			if message.SenderUserID == recipientUserID {
				recipientUserID = conversation.InitiatorUserID
			}
			if err := db.Model(&ConversationMessage{}).Where("id = ?", message.ID).Update("recipient_user_id", recipientUserID).Error; err != nil {
				return err
			}
		}
	}
}
