package realtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goblog/backend/internal/store"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestPersistMessageStoresOnceAndRequiresParticipant(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:realtime-test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(db); err != nil {
		t.Fatal(err)
	}
	publisher := store.User{ID: 803113126182430001, DisplayName: "发布者", Status: "active"}
	initiator := store.User{ID: 803113126182430002, DisplayName: "咨询者", Status: "active"}
	outsider := store.User{ID: 803113126182430003, DisplayName: "无关用户", Status: "active"}
	for _, user := range []*store.User{&publisher, &initiator, &outsider} {
		if err := db.Create(user).Error; err != nil {
			t.Fatal(err)
		}
	}
	listing := store.DerivativeListing{PublisherUserID: publisher.ID, Title: "测试商品", Category: "test", Description: "测试"}
	if err := db.Create(&listing).Error; err != nil {
		t.Fatal(err)
	}
	conversation := store.Conversation{DerivativeListingID: listing.ID, PublisherUserID: publisher.ID, InitiatorUserID: initiator.ID}
	if err := db.Create(&conversation).Error; err != nil {
		t.Fatal(err)
	}
	hub := NewHub(db, Options{MaxConnections: 2, FallbackCooldown: time.Minute})
	input := incomingMessage{Type: "message.send", ConversationID: conversation.ID, ClientMessageID: "message-001", Content: "你好"}
	first, _, err := hub.persistMessage(context.Background(), initiator.ID, input)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := hub.persistMessage(context.Background(), initiator.ID, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == 0 || first.ID != second.ID {
		t.Fatalf("retry did not return the stored message: %d, %d", first.ID, second.ID)
	}
	if _, _, err := hub.persistMessage(context.Background(), outsider.ID, input); err == nil {
		t.Fatal("non-participant message was accepted")
	}
	var count int64
	if err := db.Model(&store.ConversationMessage{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("want one stored message, got %d", count)
	}
}

func TestRecentMessagesMergesSentAndReceivedByNewestTime(t *testing.T) {
	db, conversation, publisher, initiator := realtimeTestDatabase(t)
	hub := NewHub(db, Options{MaxConnections: 2, FallbackCooldown: time.Minute})
	base := time.Now().UTC().Add(-time.Hour)
	for index := 0; index < 18; index++ {
		sender, recipient := publisher.ID, initiator.ID
		if index%2 == 1 {
			sender, recipient = initiator.ID, publisher.ID
		}
		message := store.ConversationMessage{ConversationID: conversation.ID, SenderUserID: sender, RecipientUserID: recipient, ClientMessageID: fmt.Sprintf("recent-%02d", index), Content: "测试消息", CreatedAt: base.Add(time.Duration(index) * time.Second)}
		if err := db.Create(&message).Error; err != nil {
			t.Fatal(err)
		}
	}
	messages, err := hub.recentMessages(context.Background(), publisher.ID, recentMessageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != recentMessageLimit {
		t.Fatalf("want %d messages, got %d", recentMessageLimit, len(messages))
	}
	if messages[0].ClientMessageID != "recent-17" || messages[len(messages)-1].ClientMessageID != "recent-03" {
		t.Fatalf("messages are not the newest sorted slice: %#v", messages)
	}
}

func TestAcknowledgeMessageStoresReceivedTime(t *testing.T) {
	db, conversation, publisher, initiator := realtimeTestDatabase(t)
	hub := NewHub(db, Options{MaxConnections: 2, FallbackCooldown: time.Minute})
	message := store.ConversationMessage{ConversationID: conversation.ID, SenderUserID: publisher.ID, RecipientUserID: initiator.ID, ClientMessageID: "ack-001", Content: "已送达"}
	if err := db.Create(&message).Error; err != nil {
		t.Fatal(err)
	}
	acknowledged, err := hub.acknowledgeMessage(context.Background(), initiator.ID, message.ID)
	if err != nil {
		t.Fatal(err)
	}
	if acknowledged.ReceivedAt == nil {
		t.Fatal("received timestamp was not stored")
	}
	if _, err := hub.acknowledgeMessage(context.Background(), publisher.ID, message.ID); err == nil {
		t.Fatal("sender acknowledged a recipient-only message")
	}
}

func TestReserveEvictsLeastRecentlyDeliveredPeerAtCapacity(t *testing.T) {
	hub := NewHub(nil, Options{MaxConnections: 2, FallbackCooldown: time.Minute})
	old := &client{subject: "old", lastDelivered: time.Now().UTC().Add(-time.Minute)}
	newer := &client{subject: "newer", lastDelivered: time.Now().UTC()}
	hub.peers[old.subject] = old
	hub.peers[newer.subject] = newer
	evicted, err := hub.reserve("incoming")
	if err != nil {
		t.Fatal(err)
	}
	if evicted != old {
		t.Fatalf("want oldest peer evicted, got %#v", evicted)
	}
	if _, exists := hub.peers[old.subject]; exists {
		t.Fatal("old peer was not removed from routing")
	}
	if _, reserved := hub.pending["incoming"]; !reserved {
		t.Fatal("incoming peer did not reserve a seat")
	}
	if _, cooling := hub.cooldowns[old.subject]; !cooling {
		t.Fatal("evicted peer did not enter cooldown")
	}
}

func realtimeTestDatabase(t *testing.T) (*gorm.DB, store.Conversation, store.User, store.User) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:realtime-extra-%d?mode=memory&cache=shared", time.Now().UnixNano())), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(db); err != nil {
		t.Fatal(err)
	}
	publisher := store.User{ID: uint64(time.Now().UnixNano()), DisplayName: "发布者", Status: "active"}
	initiator := store.User{ID: publisher.ID + 1, DisplayName: "咨询者", Status: "active"}
	for _, user := range []*store.User{&publisher, &initiator} {
		if err := db.Create(user).Error; err != nil {
			t.Fatal(err)
		}
	}
	listing := store.DerivativeListing{PublisherUserID: publisher.ID, Title: "测试商品", Category: "test", Description: "测试"}
	if err := db.Create(&listing).Error; err != nil {
		t.Fatal(err)
	}
	conversation := store.Conversation{DerivativeListingID: listing.ID, PublisherUserID: publisher.ID, InitiatorUserID: initiator.ID}
	if err := db.Create(&conversation).Error; err != nil {
		t.Fatal(err)
	}
	return db, conversation, publisher, initiator
}
