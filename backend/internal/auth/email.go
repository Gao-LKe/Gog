package auth

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/smtp"
	"strings"
	"time"
)

type EmailSender interface {
	SendVerificationCode(context.Context, string, string) error
}

// AlertEmailSender uses the configured mail transport for operational alerts.
// It intentionally carries no authentication or business identity.
type AlertEmailSender interface {
	SendAlert(context.Context, string, string, string) error
}

type EmailConfig struct {
	Mode     string
	Host     string
	Port     string
	Username string
	Password string
	From     string
}

func NewEmailSender(cfg EmailConfig) EmailSender {
	if strings.EqualFold(cfg.Mode, "smtp") {
		return smtpEmailSender{cfg: cfg}
	}
	return logEmailSender{}
}

func NewAlertEmailSender(cfg EmailConfig) AlertEmailSender {
	if strings.EqualFold(cfg.Mode, "smtp") {
		return smtpEmailSender{cfg: cfg}
	}
	return logEmailSender{}
}

type logEmailSender struct{}

func (logEmailSender) SendVerificationCode(_ context.Context, recipient, code string) error {
	log.Printf("development email code recipient=%s code=%s", maskEmail(recipient), code)
	return nil
}

func (logEmailSender) SendAlert(_ context.Context, recipient, subject, body string) error {
	log.Printf("development alert email recipient=%s subject=%q body=%q", maskEmail(recipient), subject, body)
	return nil
}

type smtpEmailSender struct{ cfg EmailConfig }

func (s smtpEmailSender) SendVerificationCode(ctx context.Context, recipient, code string) error {
	return s.sendPlainText(ctx, recipient, "Your verification code", fmt.Sprintf("Your verification code is %s. It expires in 5 minutes.", code))
}

func (s smtpEmailSender) SendAlert(ctx context.Context, recipient, subject, body string) error {
	return s.sendPlainText(ctx, recipient, subject, body)
}

func (s smtpEmailSender) sendPlainText(ctx context.Context, recipient, subject, body string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dialer := net.Dialer{Timeout: 10 * time.Second}
	connection, err := tls.DialWithDialer(&dialer, "tcp", net.JoinHostPort(s.cfg.Host, s.cfg.Port), &tls.Config{ServerName: s.cfg.Host, MinVersion: tls.VersionTLS12})
	if err != nil {
		return err
	}
	defer connection.Close()

	client, err := smtp.NewClient(connection, s.cfg.Host)
	if err != nil {
		return err
	}
	defer client.Quit()
	if err := client.Auth(smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)); err != nil {
		return err
	}
	if err := client.Mail(s.cfg.From); err != nil {
		return err
	}
	if err := client.Rcpt(recipient); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(writer, "From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n", s.cfg.From, recipient, sanitizeMailHeader(subject), body)
	closeErr := writer.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func sanitizeMailHeader(value string) string {
	return strings.NewReplacer("\r", "", "\n", "").Replace(value)
}
