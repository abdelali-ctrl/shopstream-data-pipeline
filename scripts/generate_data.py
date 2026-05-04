"""
generate_data.py

Populate the ShopStream PostgreSQL database with synthetic e-commerce data
(users, products, orders, order_items) using Faker.

Idempotency:
    Running this script multiple times appends new rows. To start clean,
    truncate the tables first or drop and recreate the database.

Volume:
    The defaults below produce a small dataset suitable for the demo. Bump
    USERS / PRODUCTS / ORDERS to stress-test the pipeline.

v2 changes vs v1:
    - Removed events and crm_contacts generation (deferred; see ADR-003).
    - English-only comments and log messages.
    - Type hints throughout; deprecated pandas APIs removed.
"""

from __future__ import annotations

import logging
import os
import random
from collections.abc import Iterable
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from faker import Faker
from psycopg2.extras import execute_batch

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("ShopStream.GenerateData")

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "database": os.environ.get("POSTGRES_DB", "shopstream"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

# Volumes
USERS = 500
PRODUCTS = 200
ORDERS = 2_000
MIN_ITEMS_PER_ORDER = 1
MAX_ITEMS_PER_ORDER = 5

COUNTRIES = ["FRA", "DEU", "ESP", "ITA", "GBR", "NLD", "BEL", "PRT", "SWE", "DNK"]
PLAN_TYPES = ["freemium", "premium", "enterprise"]
ORDER_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled", "refunded"]
PAYMENT_METHODS = ["card", "paypal", "bank_transfer", "apple_pay"]
CATEGORIES = ["electronics", "clothing", "books", "home", "beauty", "toys", "sports", "food"]

fake = Faker()
random.seed(42)
Faker.seed(42)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def connect() -> psycopg2.extensions.connection:
    logger.info(
        "Connecting to PostgreSQL at %s:%s/%s",
        DB_CONFIG["host"],
        DB_CONFIG["port"],
        DB_CONFIG["database"],
    )
    return psycopg2.connect(**DB_CONFIG)


def insert_many(conn: psycopg2.extensions.connection, sql: str, rows: Iterable[tuple]) -> None:
    rows = list(rows)
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    logger.info("Inserted %d rows", len(rows))


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def generate_users(conn: psycopg2.extensions.connection) -> list[int]:
    logger.info("Generating %d users", USERS)
    rows = []
    for _ in range(USERS):
        rows.append(
            (
                fake.unique.email(),
                fake.first_name(),
                fake.last_name(),
                random.choice(COUNTRIES),
                random.choices(PLAN_TYPES, weights=[0.7, 0.25, 0.05])[0],
                fake.date_time_between(start_date="-2y", end_date="now"),
                fake.date_time_between(start_date="-30d", end_date="now"),
                random.random() > 0.05,  # 95% active
            )
        )
    sql = """
        INSERT INTO users (email, first_name, last_name, country, plan_type, created_at, last_login, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_many(conn, sql, rows)

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users")
        return [r[0] for r in cur.fetchall()]


def generate_products(conn: psycopg2.extensions.connection) -> list[tuple[int, float]]:
    logger.info("Generating %d products", PRODUCTS)
    rows = []
    for _ in range(PRODUCTS):
        price = round(random.uniform(5, 500), 2)
        rows.append(
            (
                random.randint(1, 50),  # merchant_id
                fake.catch_phrase()[:255],  # name
                fake.text(max_nb_chars=200),  # description
                random.choice(CATEGORIES),  # category
                price,
                random.randint(0, 1000),  # stock_quantity
                fake.date_time_between(start_date="-2y", end_date="-1y"),
                fake.date_time_between(start_date="-1y", end_date="now"),
            )
        )
    sql = """
        INSERT INTO products (merchant_id, name, description, category, price, stock_quantity, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_many(conn, sql, rows)

    with conn.cursor() as cur:
        cur.execute("SELECT id, price FROM products")
        return [(r[0], float(r[1])) for r in cur.fetchall()]


def generate_orders_and_items(
    conn: psycopg2.extensions.connection,
    user_ids: list[int],
    products: list[tuple[int, float]],
) -> None:
    logger.info("Generating %d orders with line items", ORDERS)

    order_rows: list[tuple] = []
    item_rows_by_order: dict[int, list[tuple]] = {}

    # Build orders with their line items first; we need order ids to attach
    # the items, so we insert orders, fetch back the ids, then insert items.
    for i in range(ORDERS):
        items_count = random.randint(MIN_ITEMS_PER_ORDER, MAX_ITEMS_PER_ORDER)
        chosen = random.sample(products, k=items_count)
        items: list[tuple[int, int, float, float]] = []  # product_id, qty, unit_price, line_total
        order_total = 0.0
        for product_id, price in chosen:
            qty = random.randint(1, 5)
            line_total = round(qty * price, 2)
            items.append((product_id, qty, price, line_total))
            order_total += line_total

        order_rows.append(
            (
                random.choice(user_ids),
                fake.date_time_between(start_date="-1y", end_date="now"),
                round(order_total, 2),
                random.choices(
                    ORDER_STATUSES,
                    weights=[0.05, 0.20, 0.20, 0.45, 0.05, 0.05],
                )[0],
                random.choice(COUNTRIES),
                random.choice(PAYMENT_METHODS),
            )
        )
        item_rows_by_order[i] = items  # index keyed for now

    # Insert orders, get back ids in insertion order.
    sql_orders = """
        INSERT INTO orders (user_id, created_at, total_amount, status, country, payment_method)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    with conn.cursor() as cur:
        order_ids: list[int] = []
        for row in order_rows:
            cur.execute(sql_orders, row)
            order_ids.append(cur.fetchone()[0])
        conn.commit()
    logger.info("Inserted %d orders", len(order_ids))

    # Now flatten items and attach the real order_id from the RETURNING.
    item_rows: list[tuple] = []
    for idx, oid in enumerate(order_ids):
        for product_id, qty, unit_price, line_total in item_rows_by_order[idx]:
            item_rows.append((oid, product_id, qty, unit_price, line_total))

    sql_items = """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
        VALUES (%s, %s, %s, %s, %s)
    """
    insert_many(conn, sql_items, item_rows)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    started = datetime.now()
    logger.info("=== ShopStream data generation started ===")

    conn = connect()
    try:
        user_ids = generate_users(conn)
        products = generate_products(conn)
        generate_orders_and_items(conn, user_ids, products)
    finally:
        conn.close()

    elapsed = datetime.now() - started
    logger.info("=== Done in %s ===", elapsed)


if __name__ == "__main__":
    main()
