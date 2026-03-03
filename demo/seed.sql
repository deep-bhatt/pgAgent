-- Seed data: 100k users, 10k products, 200k orders, 500k order_items

-- Users (100k)
INSERT INTO users (username, email, status, last_login)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    CASE WHEN random() < 0.9 THEN 'active' ELSE 'inactive' END,
    NOW() - (random() * INTERVAL '365 days')
FROM generate_series(1, 100000) AS i;

-- Products (10k)
INSERT INTO products (name, category, price, stock)
SELECT
    'Product ' || i,
    (ARRAY['electronics', 'clothing', 'books', 'food', 'toys'])[1 + floor(random() * 5)::int],
    round((random() * 500 + 1)::numeric, 2),
    floor(random() * 1000)::int
FROM generate_series(1, 10000) AS i;

-- Orders (200k)
INSERT INTO orders (user_id, total, status, created_at)
SELECT
    1 + floor(random() * 100000)::int,
    round((random() * 1000 + 10)::numeric, 2),
    (ARRAY['pending', 'processing', 'shipped', 'delivered', 'cancelled'])[1 + floor(random() * 5)::int],
    NOW() - (random() * INTERVAL '180 days')
FROM generate_series(1, 200000) AS i;

-- Order items (500k)
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT
    1 + floor(random() * 200000)::int,
    1 + floor(random() * 10000)::int,
    1 + floor(random() * 5)::int,
    round((random() * 200 + 5)::numeric, 2)
FROM generate_series(1, 500000) AS i;

-- Force dead tuples: update a chunk of rows then don't vacuum
UPDATE users SET last_login = NOW() WHERE id <= 20000;
UPDATE orders SET status = 'processing' WHERE id <= 40000;

-- Analyze tables so stats are available
ANALYZE users;
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;
