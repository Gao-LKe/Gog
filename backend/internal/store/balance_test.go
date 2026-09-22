package store

import (
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestPasswordRegistrationGetsBonusBalance(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&User{}, &UserBalance{}, &BalanceTransaction{}); err != nil {
		t.Fatal(err)
	}
	user := User{DisplayName: "新用户", PasswordHash: []byte("password-hash"), Role: "user", Status: "active"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	var balance UserBalance
	var entry BalanceTransaction
	if err := db.First(&balance, "user_id = ?", user.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&entry, "user_id = ?", user.ID).Error; err != nil {
		t.Fatal(err)
	}
	if balance.AvailableFen != registrationBonusFen || entry.Type != "registration_bonus" || entry.AmountFen != int64(registrationBonusFen) {
		t.Fatalf("balance=%+v entry=%+v", balance, entry)
	}
}
