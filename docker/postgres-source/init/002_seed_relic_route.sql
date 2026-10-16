-- Relic Route demo seeds (gothic / archive vintage, Moscow).
-- Runs after 001_init.sql on first Postgres volume init.

INSERT INTO relic.customers (customer_id, email, full_name, city, created_at) VALUES
  (1, 'mara.veil@example.com',   'Mara Veil',     'Moscow', NOW() - INTERVAL '40 days'),
  (2, 'ivan.ash@example.com',    'Ivan Ash',      'Moscow', NOW() - INTERVAL '28 days'),
  (3, 'lena.noir@example.com',   'Lena Noir',     'Saint Petersburg', NOW() - INTERVAL '21 days'),
  (4, 'kir.thorn@example.com',   'Kir Thorn',     'Moscow', NOW() - INTERVAL '14 days'),
  (5, 'sofia.grave@example.com', 'Sofia Grave',   'Kazan',  NOW() - INTERVAL '7 days')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO relic.vans (van_id, name, van_type) VALUES
  (1, 'Night Moth',   'van'),
  (2, 'Bone Spoke',   'cargo_bike'),
  (3, 'Crypt Box',    'popup_box')
ON CONFLICT (van_id) DO NOTHING;

INSERT INTO relic.stop_locations (location_id, name, district, lat, lon) VALUES
  (1, 'Gorky Park Gate',      'Yakimanka',   55.729800, 37.601400),
  (2, 'Patriarch Ponds',      'Presnensky',  55.763900, 37.592200),
  (3, 'Flacon Yard',          'Savyolovsky', 55.805600, 37.584700),
  (4, 'Kitay-Gorod Stairs',   'Tverskoy',    55.755300, 37.629100),
  (5, 'VDNKh Arch',           'Ostankinsky', 55.826000, 37.637700)
ON CONFLICT (location_id) DO NOTHING;

INSERT INTO relic.items (
  item_id, title, category, brand, grade, status, list_price, listed_at, updated_at
) VALUES
  (1,  'Victorian Mourning Brooch',     'jewelry',   'Unknown',      'S', 'sold',     18500.00, NOW() - INTERVAL '20 days', NOW() - INTERVAL '3 days'),
  (2,  'Black Lace Opera Gloves',       'apparel',   'Atelier Noir', 'A', 'sold',      4200.00, NOW() - INTERVAL '18 days', NOW() - INTERVAL '2 days'),
  (3,  '1920s Silver Cigarette Case',   'object',    'WMF',          'A', 'sold',      9600.00, NOW() - INTERVAL '15 days', NOW() - INTERVAL '1 days'),
  (4,  'Batwing Velvet Cape',           'apparel',   'Grave Goods',  'B', 'sold',      7800.00, NOW() - INTERVAL '10 days', NOW() - INTERVAL '3 days'),
  (5,  'Obsidian Rosary',               'jewelry',   'Unknown',      'A', 'listed',    5400.00, NOW() - INTERVAL '9 days',  NOW() - INTERVAL '9 days'),
  (6,  'Porcelain Doll Head (cracked)', 'object',    'Jumeau?',      'C', 'sold',      3100.00, NOW() - INTERVAL '8 days',  NOW() - INTERVAL '1 days'),
  (7,  'Iron Cemetery Gate Key',        'object',    'Unknown',      'B', 'reserved',  2500.00, NOW() - INTERVAL '6 days',  NOW() - INTERVAL '1 days'),
  (8,  'Gothic Revival Mirror',         'furniture', 'Unknown',      'S', 'listed',   42000.00, NOW() - INTERVAL '5 days',  NOW() - INTERVAL '5 days'),
  (9,  'Raven Skull Ring (silver)',     'jewelry',   'Thorn & Co',   'A', 'sourced',   8900.00, NULL,                       NOW() - INTERVAL '2 days'),
  (10, 'Funeral March Sheet Music',     'paper',     'Breitkopf',    'B', 'archived',  1200.00, NOW() - INTERVAL '30 days', NOW() - INTERVAL '12 days')
ON CONFLICT (item_id) DO NOTHING;

-- Van stop days (last few days)
INSERT INTO relic.van_stops (stop_id, van_id, location_id, stop_date, started_at, ended_at) VALUES
  (1, 1, 1, (CURRENT_DATE - 3), (CURRENT_DATE - 3) + TIME '17:00', (CURRENT_DATE - 3) + TIME '22:00'),
  (2, 2, 2, (CURRENT_DATE - 2), (CURRENT_DATE - 2) + TIME '18:00', (CURRENT_DATE - 2) + TIME '21:30'),
  (3, 1, 3, (CURRENT_DATE - 1), (CURRENT_DATE - 1) + TIME '16:30', (CURRENT_DATE - 1) + TIME '22:00'),
  (4, 3, 4,  CURRENT_DATE,       CURRENT_DATE + TIME '17:00',       NULL),
  (5, 1, 5,  CURRENT_DATE,       CURRENT_DATE + TIME '18:00',       NULL)
ON CONFLICT (stop_id) DO NOTHING;

INSERT INTO relic.orders (
  order_id, customer_id, order_ts, channel, status, currency, total_amount, van_id, stop_id
) VALUES
  -- online
  (1, 1, NOW() - INTERVAL '3 days',  'online', 'fulfilled', 'RUB', 18500.00, NULL, NULL),
  (2, 3, NOW() - INTERVAL '2 days',  'online', 'fulfilled', 'RUB',  9600.00, NULL, NULL),
  (3, 5, NOW() - INTERVAL '12 hours','online', 'paid',      'RUB',  4200.00, NULL, NULL),
  -- van channel (tied to stops)
  (4, 2, (CURRENT_DATE - 3) + TIME '19:10', 'van', 'fulfilled', 'RUB', 7800.00, 1, 1),
  (5, 4, (CURRENT_DATE - 1) + TIME '20:05', 'van', 'paid',      'RUB', 3100.00, 1, 3)
ON CONFLICT (order_id) DO NOTHING;

-- One unique item per line (vintage pieces are not double-sold)
INSERT INTO relic.order_items (order_id, item_id, unit_price, qty) VALUES
  (1, 1, 18500.00, 1),
  (2, 3,  9600.00, 1),
  (3, 2,  4200.00, 1),
  (4, 4,  7800.00, 1),
  (5, 6,  3100.00, 1)
ON CONFLICT (order_id, item_id) DO NOTHING;

-- Home delivery for online orders
INSERT INTO relic.deliveries (
  delivery_id, order_id, method, city, address, status, delivery_fee, created_at, shipped_at, delivered_at
) VALUES
  (1, 1, 'courier',      'Moscow',           'Pokrovka 12, apt 4',      'delivered', 450.00, NOW() - INTERVAL '3 days',  NOW() - INTERVAL '2 days',  NOW() - INTERVAL '1 days'),
  (2, 2, 'post',         'Saint Petersburg', 'Liteyny 33, apt 18',      'delivered', 390.00, NOW() - INTERVAL '2 days',  NOW() - INTERVAL '40 hours', NOW() - INTERVAL '6 hours'),
  (3, 3, 'pickup_point', 'Kazan',            'Baumana 51, pickup #12',  'pending',     0.00, NOW() - INTERVAL '12 hours', NULL,                      NULL)
ON CONFLICT (delivery_id) DO NOTHING;

-- Explicit ids above: move identity sequences past them.
SELECT setval(pg_get_serial_sequence('relic.customers', 'customer_id'), (SELECT MAX(customer_id) FROM relic.customers));
SELECT setval(pg_get_serial_sequence('relic.items', 'item_id'), (SELECT MAX(item_id) FROM relic.items));
SELECT setval(pg_get_serial_sequence('relic.van_stops', 'stop_id'), (SELECT MAX(stop_id) FROM relic.van_stops));
SELECT setval(pg_get_serial_sequence('relic.orders', 'order_id'), (SELECT MAX(order_id) FROM relic.orders));
SELECT setval(pg_get_serial_sequence('relic.deliveries', 'delivery_id'), (SELECT MAX(delivery_id) FROM relic.deliveries));
