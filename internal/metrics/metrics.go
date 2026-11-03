// Package metrics defines the Prometheus metrics of the gateway.
//
// They answer the operational questions of the challenge: are we keeping
// the express promise (sms_handover_seconds, sms_express_sla_missed_total),
// and how much work is waiting (queue gauges).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Accepted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_accepted_total",
		Help: "Messages accepted and charged.",
	}, []string{"lane"})

	Rejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_rejected_total",
		Help: "Messages refused at the API, by reason.",
	}, []string{"lane", "reason"})

	Sent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_sent_total",
		Help: "Messages handed over to the operator.",
	}, []string{"lane"})

	Retried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_retried_total",
		Help: "Hand-over attempts that failed temporarily and were rescheduled.",
	}, []string{"lane"})

	Failed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_failed_total",
		Help: "Messages that failed permanently and were refunded.",
	}, []string{"lane"})

	Handover = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sms_handover_seconds",
		Help:    "Time from acceptance to hand-over to the operator.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 30, 60, 300, 900},
	}, []string{"lane"})

	ExpressSLAMissed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_express_sla_missed_total",
		Help: "Express messages handed over after their deadline.",
	})

	OutboxPending = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sms_outbox_pending",
		Help: "Messages in Postgres waiting for the relay.",
	}, []string{"lane"})

	ExpressQueued = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sms_express_queued",
		Help: "Express messages waiting in Redis.",
	})

	ActiveCustomers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sms_normal_active_customers",
		Help: "Customers with normal messages waiting in Redis.",
	})

	InFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sms_inflight",
		Help: "Messages popped from Redis but not yet claimed.",
	})
)
