package api

import (
	"net/http"

	"sms-gateway/internal/service"
	"sms-gateway/internal/sms"
)

func (h *Handler) createCustomer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	c, err := h.svc.CreateCustomer(r.Context(), req.Name)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toCustomerView(c))
}

func (h *Handler) getCustomer(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	c, err := h.svc.GetCustomer(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCustomerView(c))
}

func (h *Handler) topUp(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var req struct {
		Amount int64 `json:"amount"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	c, err := h.svc.TopUp(r.Context(), id, req.Amount)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCustomerView(c))
}

func (h *Handler) listTransactions(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	limit, err := queryInt(r, "limit")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	txs, err := h.svc.ListTransactions(r.Context(), id, int(limit))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	views := make([]transactionView, len(txs))
	for i, t := range txs {
		views[i] = toTransactionView(t)
	}
	writeJSON(w, http.StatusOK, listResponse[transactionView]{Data: views})
}

// sendMessage accepts one SMS. 202 Accepted means: stored and charged,
// delivery happens asynchronously; poll the message to follow its status.
func (h *Handler) sendMessage(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var req struct {
		To      string `json:"to"`
		Text    string `json:"text"`
		Express bool   `json:"express"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	m, replayed, err := h.svc.Send(r.Context(), service.SendRequest{
		CustomerID:     id,
		To:             req.To,
		Text:           req.Text,
		Express:        req.Express,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusAccepted, toMessageView(m))
}

func (h *Handler) sendBulk(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var req struct {
		Messages []struct {
			To   string `json:"to"`
			Text string `json:"text"`
		} `json:"messages"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	items := make([]service.BulkItem, len(req.Messages))
	for i, m := range req.Messages {
		items[i] = service.BulkItem{To: m.To, Text: m.Text}
	}
	msgs, replayed, err := h.svc.SendBulk(r.Context(), service.BulkRequest{
		CustomerID:     id,
		Messages:       items,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	var total int64
	for _, m := range msgs {
		total += m.Cost
	}
	writeJSON(w, http.StatusAccepted, bulkResponse{Count: len(msgs), TotalCost: total, Data: toMessageViews(msgs)})
}

func (h *Handler) getMessage(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	messageID, err := pathID(r, "messageID")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	m, err := h.svc.GetMessage(r.Context(), id, messageID)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toMessageView(m))
}

// listMessages is the report endpoint:
// GET /v1/customers/{id}/messages?status=sent&from=...&to=...&limit=50&cursor=...
func (h *Handler) listMessages(w http.ResponseWriter, r *http.Request) {
	f, err := parseFilter(r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	msgs, next, err := h.svc.ListMessages(r.Context(), f)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[messageView]{Data: toMessageViews(msgs), NextCursor: cursorString(next)})
}

func parseFilter(r *http.Request) (sms.MessageFilter, error) {
	var f sms.MessageFilter
	var err error
	if f.CustomerID, err = pathID(r, "id"); err != nil {
		return f, err
	}
	if f.From, err = queryTime(r, "from"); err != nil {
		return f, err
	}
	if f.To, err = queryTime(r, "to"); err != nil {
		return f, err
	}
	if f.BeforeID, err = queryInt(r, "cursor"); err != nil {
		return f, err
	}
	limit, err := queryInt(r, "limit")
	f.Limit = int(limit)
	f.Status = sms.Status(r.URL.Query().Get("status"))
	return f, err
}

func (h *Handler) summary(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	from, err := queryTime(r, "from")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	to, err := queryTime(r, "to")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	sum, err := h.svc.Summarize(r.Context(), id, from, to)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toSummaryView(sum))
}
