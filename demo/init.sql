-- Enable pg_stat_statements
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Schema: users
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'active',
    last_login TIMESTAMP
);

-- Schema: products
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    price NUMERIC(10,2) NOT NULL DEFAULT 0,
    stock INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Schema: orders
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    total NUMERIC(12,2) NOT NULL DEFAULT 0,
    status VARCHAR(30) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    shipped_at TIMESTAMP
);

-- Schema: order_items
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(10,2) NOT NULL DEFAULT 0
);

-- Intentionally UNUSED indexes (for pgAgent to detect)
CREATE INDEX idx_users_created_at ON users(created_at);
CREATE INDEX idx_users_last_login ON users(last_login);
CREATE INDEX idx_products_created_at ON products(created_at);
CREATE INDEX idx_orders_shipped_at ON orders(shipped_at);
CREATE INDEX idx_order_items_quantity ON order_items(quantity);

-- Intentionally MISSING indexes on commonly queried columns:
--   orders.user_id      (no index → seq scans on JOIN/WHERE)
--   orders.status       (no index → seq scans on status filtering)
--   order_items.order_id (no index → seq scans on JOIN)
--   users.email         (no index → seq scans on email lookup)
--   products.category   (no index → seq scans on category filtering)
