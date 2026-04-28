-- ============================================
-- ShopStream v2 - PostgreSQL Schema
-- Database: shopstream
-- ============================================
-- Run this once after creating the database:
--   createdb -U postgres shopstream
--   psql -U postgres -d shopstream -f schema.sql
-- ============================================
-- v2 changes vs v1:
--   - Removed events and crm_contacts tables (deferred to v2.1; see ADR-003).
--   - All comments translated to English.
-- ============================================

-- ============================================
-- 1. USERS
-- ============================================
CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    email       VARCHAR(255) UNIQUE NOT NULL,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    country     VARCHAR(3),
    plan_type   VARCHAR(20) DEFAULT 'freemium',
    created_at  TIMESTAMP DEFAULT NOW(),
    last_login  TIMESTAMP,
    is_active   BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_users_email     ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_country   ON users(country);
CREATE INDEX IF NOT EXISTS idx_users_plan_type ON users(plan_type);

-- ============================================
-- 2. PRODUCTS
-- ============================================
CREATE TABLE IF NOT EXISTS products (
    id              SERIAL PRIMARY KEY,
    merchant_id     INT NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    category        VARCHAR(100),
    price           DECIMAL(10,2) NOT NULL,
    stock_quantity  INT DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_merchant ON products(merchant_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

-- ============================================
-- 3. ORDERS
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    user_id         INT NOT NULL REFERENCES users(id),
    created_at      TIMESTAMP DEFAULT NOW(),
    total_amount    DECIMAL(10,2) NOT NULL,
    status          VARCHAR(20) DEFAULT 'pending',
    country         VARCHAR(3),
    payment_method  VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);

-- ============================================
-- 4. ORDER_ITEMS
-- ============================================
CREATE TABLE IF NOT EXISTS order_items (
    id          SERIAL PRIMARY KEY,
    order_id    INT NOT NULL REFERENCES orders(id),
    product_id  INT NOT NULL REFERENCES products(id),
    quantity    INT NOT NULL,
    unit_price  DECIMAL(10,2) NOT NULL,
    line_total  DECIMAL(10,2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- ============================================
-- Verification
-- ============================================
-- \dt
