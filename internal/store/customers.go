package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"sms-gateway/internal/sms"
)

const customerColumns = "id, name, balance, created_at"

func scanCustomer(row pgx.Row) (sms.Customer, error) {
	var c sms.Customer
	err := row.Scan(&c.ID, &c.Name, &c.Balance, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, sms.ErrCustomerNotFound
	}
	return c, err
}

// CreateCustomer inserts a customer with a zero balance.
func (s *Store) CreateCustomer(ctx context.Context, name string) (sms.Customer, error) {
	return scanCustomer(s.pool.QueryRow(ctx,
		"INSERT INTO customers (name) VALUES ($1) RETURNING "+customerColumns, name))
}

// GetCustomer returns one customer with its current balance.
func (s *Store) GetCustomer(ctx context.Context, id int64) (sms.Customer, error) {
	return scanCustomer(s.pool.QueryRow(ctx,
		"SELECT "+customerColumns+" FROM customers WHERE id = $1", id))
}

// TopUp adds amount to the balance and records it in the ledger, atomically.
func (s *Store) TopUp(ctx context.Context, id, amount int64) (sms.Customer, error) {
	var c sms.Customer
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		c, err = scanCustomer(tx.QueryRow(ctx, `
			UPDATE customers SET balance = balance + $2, updated_at = now()
			WHERE id = $1
			RETURNING `+customerColumns, id, amount))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO balance_transactions (customer_id, kind, amount, balance_after)
			VALUES ($1, 'topup', $2, $3)`, id, amount, c.Balance)
		return err
	})
	return c, err
}

// ListTransactions returns the newest ledger entries of a customer.
func (s *Store) ListTransactions(ctx context.Context, customerID int64, limit int) ([]sms.Transaction, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, kind, amount, balance_after, message_count, created_at
		FROM balance_transactions
		WHERE customer_id = $1
		ORDER BY id DESC
		LIMIT $2`, customerID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (sms.Transaction, error) {
		var t sms.Transaction
		err := row.Scan(&t.ID, &t.Kind, &t.Amount, &t.BalanceAfter, &t.MessageCount, &t.CreatedAt)
		return t, err
	})
}
