CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders (
  order_id      BIGINT PRIMARY KEY,
  user_id       BIGINT NOT NULL,
  order_ts      TIMESTAMP NOT NULL,
  amount        NUMERIC(12,2) NOT NULL,
  currency      TEXT NOT NULL
);

INSERT INTO raw.orders (order_id, user_id, order_ts, amount, currency) VALUES
  (1, 101, NOW() - INTERVAL '3 days',  12.50, 'USD'),
  (2, 101, NOW() - INTERVAL '2 days',  39.90, 'USD'),
  (3, 202, NOW() - INTERVAL '1 days', 199.00, 'EUR'),
  (4, 303, NOW() - INTERVAL '6 hours',  7.10, 'USD')
ON CONFLICT (order_id) DO NOTHING;
