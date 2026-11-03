package api

import (
	"strconv"
	"time"

	"sms-gateway/internal/service"
	"sms-gateway/internal/sms"
)

// View types define the public JSON shape. Keeping them separate from the
// domain types means internal changes never break the API by accident.

type customerView struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

func toCustomerView(c sms.Customer) customerView {
	return customerView{ID: c.ID, Name: c.Name, Balance: c.Balance, CreatedAt: c.CreatedAt}
}

type transactionView struct {
	ID           int64     `json:"id"`
	Kind         string    `json:"kind"`
	Amount       int64     `json:"amount"`
	BalanceAfter int64     `json:"balance_after"`
	MessageCount int       `json:"message_count"`
	CreatedAt    time.Time `json:"created_at"`
}

func toTransactionView(t sms.Transaction) transactionView {
	return transactionView{
		ID: t.ID, Kind: t.Kind, Amount: t.Amount, BalanceAfter: t.BalanceAfter,
		MessageCount: t.MessageCount, CreatedAt: t.CreatedAt,
	}
}

type messageView struct {
	ID          int64      `json:"id"`
	To          string     `json:"to"`
	Text        string     `json:"text"`
	Express     bool       `json:"express"`
	Status      sms.Status `json:"status"`
	Cost        int64      `json:"cost"`
	Attempts    int        `json:"attempts"`
	OperatorRef string     `json:"operator_ref,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	DeadlineAt  *time.Time `json:"deadline_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	SentAt      *time.Time `json:"sent_at,omitempty"`
}

func toMessageView(m sms.Message) messageView {
	return messageView{
		ID: m.ID, To: m.To, Text: m.Text, Express: m.Express, Status: m.Status,
		Cost: m.Cost, Attempts: m.Attempts, OperatorRef: m.OperatorRef, LastError: m.LastError,
		DeadlineAt: m.DeadlineAt, CreatedAt: m.CreatedAt, SentAt: m.SentAt,
	}
}

func toMessageViews(msgs []sms.Message) []messageView {
	out := make([]messageView, len(msgs))
	for i, m := range msgs {
		out[i] = toMessageView(m)
	}
	return out
}

type listResponse[T any] struct {
	Data       []T     `json:"data"`
	NextCursor *string `json:"next_cursor"`
}

func cursorString(next int64) *string {
	if next == 0 {
		return nil
	}
	s := strconv.FormatInt(next, 10)
	return &s
}

type bulkResponse struct {
	Count     int           `json:"count"`
	TotalCost int64         `json:"total_cost"`
	Data      []messageView `json:"data"`
}

type summaryView struct {
	From     time.Time            `json:"from"`
	To       time.Time            `json:"to"`
	Total    int64                `json:"total"`
	ByStatus map[sms.Status]int64 `json:"by_status"`
}

func toSummaryView(s service.Summary) summaryView {
	return summaryView{From: s.From, To: s.To, Total: s.Total, ByStatus: s.ByStatus}
}
