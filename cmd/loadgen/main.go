// Command loadgen demonstrates the two hardest requirements against a
// running stack (make up):
//
//   - Fairness: a light customer is not delayed by a heavy customer's backlog.
//   - Express SLA: OTPs are handed over within the deadline even while the
//     normal lane is flooded.
//
// It creates three customers. "heavy" floods the normal lane with bulk
// requests, "light" sends a few single messages right after, and "otp"
// sends express messages at a steady rate. When everything is final it
// prints the hand-over latency (sent_at - created_at) per customer.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"slices"
	"sync"
	"text/tabwriter"
	"time"
)

func main() {
	api := flag.String("api", "http://localhost:8080", "API base URL")
	heavy := flag.Int("heavy", 20000, "normal messages sent by the heavy customer (bulk)")
	light := flag.Int("light", 50, "normal messages sent by the light customer (single)")
	express := flag.Int("express", 100, "express messages sent by the otp customer")
	expressRate := flag.Int("express-rate", 20, "express messages per second")
	timeout := flag.Duration("timeout", 5*time.Minute, "give up after this long")
	flag.Parse()

	c := &client{base: *api, http: &http.Client{Timeout: 30 * time.Second}}
	start := time.Now()

	heavyID := c.newCustomer("heavy-sender", int64(*heavy)*1000)
	lightID := c.newCustomer("light-sender", int64(*light)*1000)
	otpID := c.newCustomer("otp-sender", int64(*express)*1000)

	fmt.Printf("flooding the normal lane with %d messages from heavy-sender...\n", *heavy)
	c.flood(heavyID, *heavy)

	fmt.Printf("sending %d normal messages from light-sender and %d express from otp-sender...\n", *light, *express)
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range *light {
			c.send(lightID, phone(i), "your order has shipped", false)
		}
	})
	rejected := 0
	wg.Go(func() {
		tick := time.NewTicker(time.Second / time.Duration(*expressRate))
		defer tick.Stop()
		for i := range *express {
			<-tick.C
			if !c.send(otpID, phone(i), fmt.Sprintf("code %04d", i), true) {
				rejected++
			}
		}
	})
	wg.Wait()

	fmt.Println("waiting until every message is final...")
	deadline := time.Now().Add(*timeout)
	for _, id := range []int64{heavyID, lightID, otpID} {
		for !c.done(id, start) {
			if time.Now().After(deadline) {
				fail("timed out waiting for messages")
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	elapsed := time.Since(start)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "customer\tlane\tmessages\tsent\tfailed\tp50\tp95\tmax\tSLA missed\t")
	for _, row := range []struct {
		name string
		id   int64
	}{{"heavy-sender", heavyID}, {"light-sender", lightID}, {"otp-sender", otpID}} {
		r := c.report(row.id)
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t\n", row.name, r.lane, r.total, r.sent, r.failed,
			pct(r.latencies, 0.50), pct(r.latencies, 0.95), pct(r.latencies, 1), r.slaMissed)
	}
	w.Flush()
	fmt.Printf("\nall done in %s; express requests refused by admission control: %d\n", elapsed.Round(time.Millisecond), rejected)
}

type client struct {
	base string
	http *http.Client
}

func (c *client) do(method, path string, body, out any) int {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, c.base+path, &buf)
	resp, err := c.http.Do(req)
	if err != nil {
		fail(err.Error())
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func (c *client) newCustomer(name string, credit int64) int64 {
	var cust struct {
		ID int64 `json:"id"`
	}
	if c.do("POST", "/v1/customers", map[string]string{"name": name}, &cust) != http.StatusCreated {
		fail("could not create customer; is the stack running (make up)?")
	}
	c.do("POST", fmt.Sprintf("/v1/customers/%d/topup", cust.ID), map[string]int64{"amount": credit}, nil)
	return cust.ID
}

// flood sends n messages as bulk requests of 1000, four at a time.
func (c *client) flood(customerID int64, n int) {
	type item struct {
		To   string `json:"to"`
		Text string `json:"text"`
	}
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for start := 0; start < n; start += 1000 {
		batch := make([]item, 0, 1000)
		for i := start; i < min(start+1000, n); i++ {
			batch = append(batch, item{To: phone(i), Text: "big seasonal sale!"})
		}
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			path := fmt.Sprintf("/v1/customers/%d/messages/bulk", customerID)
			if code := c.do("POST", path, map[string]any{"messages": batch}, nil); code != http.StatusAccepted {
				fail(fmt.Sprintf("bulk request returned %d", code))
			}
		})
	}
	wg.Wait()
}

func (c *client) send(customerID int64, to, text string, express bool) bool {
	path := fmt.Sprintf("/v1/customers/%d/messages", customerID)
	return c.do("POST", path, map[string]any{"to": to, "text": text, "express": express}, nil) == http.StatusAccepted
}

// done reports whether none of the customer's messages is still in flight.
func (c *client) done(customerID int64, since time.Time) bool {
	var sum struct {
		ByStatus map[string]int64 `json:"by_status"`
	}
	path := fmt.Sprintf("/v1/customers/%d/messages/summary?from=%s", customerID,
		since.Add(-time.Minute).UTC().Format(time.RFC3339))
	c.do("GET", path, nil, &sum)
	return sum.ByStatus["pending"]+sum.ByStatus["queued"]+sum.ByStatus["sending"] == 0
}

type message struct {
	ID         int64      `json:"id"`
	Express    bool       `json:"express"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	SentAt     *time.Time `json:"sent_at"`
	DeadlineAt *time.Time `json:"deadline_at"`
}

type report struct {
	lane                string
	total, sent, failed int
	latencies           []time.Duration
	slaMissed           string
}

func (c *client) report(customerID int64) report {
	r := report{lane: "normal", slaMissed: "-"}
	missed := 0
	cursor := ""
	for {
		var page struct {
			Data       []message `json:"data"`
			NextCursor *string   `json:"next_cursor"`
		}
		c.do("GET", fmt.Sprintf("/v1/customers/%d/messages?limit=500&cursor=%s", customerID, cursor), nil, &page)
		for _, m := range page.Data {
			r.total++
			if m.Express {
				r.lane = "express"
			}
			switch m.Status {
			case "sent":
				r.sent++
				r.latencies = append(r.latencies, m.SentAt.Sub(m.CreatedAt))
				if m.DeadlineAt != nil && m.SentAt.After(*m.DeadlineAt) {
					missed++
				}
			case "failed":
				r.failed++
			}
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if r.lane == "express" {
		r.slaMissed = fmt.Sprint(missed)
	}
	return r
}

func pct(d []time.Duration, q float64) string {
	if len(d) == 0 {
		return "-"
	}
	slices.Sort(d)
	return d[int(q*float64(len(d)-1))].Round(time.Millisecond).String()
}

func phone(i int) string { return fmt.Sprintf("+98912%07d", i) }

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "loadgen:", msg)
	os.Exit(1)
}
