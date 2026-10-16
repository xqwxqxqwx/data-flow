#!/usr/bin/env python3
"""Relic Route live simulator: keeps the shop running inside Postgres relic.*.

Run after generate_relic_seed.py. Every tick it may:
  - place an online order (items listed -> reserved, order created, delivery pending)
  - take payment (created -> paid, items -> sold) or cancel (items back to listed)
  - move deliveries along (pending -> shipped -> delivered, order -> fulfilled)
  - open / close today's van stops, each van at a new spot every day
  - sell pieces straight from an open van (fulfilled on the spot)
  - source new pieces and put sourced ones on the shelf

Every change is a plain UPDATE/INSERT, so Debezium sees it as CDC events.

Examples:
  docker compose --profile sim up -d relic-simulator
  python scripts/simulate_relic_live.py --pg-host localhost --pg-port 5434 --tick 2

Pieces are reserved with SELECT ... FOR UPDATE SKIP LOCKED, so two shoppers (or the van
and the website) can never sell the same unique piece.
"""

from __future__ import annotations

import argparse
import os
import random
import time
from datetime import datetime

import psycopg2

from generate_relic_seed import BRANDS, CATEGORIES, GRADES, MSK, STREETS


def log(msg: str) -> None:
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def reserve_items(cur, n: int) -> list[tuple[int, float]]:
    cur.execute(
        """
        UPDATE relic.items SET status = 'reserved', updated_at = NOW()
        WHERE item_id IN (
          SELECT item_id FROM relic.items
          WHERE status = 'listed'
          ORDER BY random()
          LIMIT %s
          FOR UPDATE SKIP LOCKED
        )
        RETURNING item_id, list_price
        """,
        (n,),
    )
    return cur.fetchall()


def basket_size(rng: random.Random) -> int:
    return rng.choices([1, 2, 3], weights=[0.75, 0.2, 0.05])[0]


def pick_customer(cur, rng: random.Random) -> tuple[int, str]:
    if rng.random() < 0.1:
        n = rng.randint(1000, 999999)
        city = rng.choice(["Moscow", "Moscow", "Saint Petersburg", "Kazan", "Yekaterinburg"])
        cur.execute(
            """
            INSERT INTO relic.customers (email, full_name, city)
            VALUES (%s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            RETURNING customer_id, city
            """,
            (f"new.shopper{n}@example.com", f"New Shopper {n}", city),
        )
        row = cur.fetchone()
        if row:
            log(f"new customer #{row[0]} from {row[1]}")
            return row
    cur.execute("SELECT customer_id, city FROM relic.customers ORDER BY random() LIMIT 1")
    return cur.fetchone()


def place_online_order(cur, rng: random.Random) -> None:
    items = reserve_items(cur, basket_size(rng))
    if not items:
        return
    customer_id, city = pick_customer(cur, rng)
    total = sum(float(p) for _, p in items)
    cur.execute(
        """
        INSERT INTO relic.orders (customer_id, order_ts, channel, status, total_amount)
        VALUES (%s, NOW(), 'online', 'created', %s)
        RETURNING order_id
        """,
        (customer_id, total),
    )
    order_id = cur.fetchone()[0]
    cur.executemany(
        "INSERT INTO relic.order_items (order_id, item_id, unit_price, qty) VALUES (%s, %s, %s, 1)",
        [(order_id, item_id, price) for item_id, price in items],
    )
    method = rng.choices(["courier", "post", "pickup_point"], weights=[0.45, 0.3, 0.25])[0]
    fee = 0 if total >= 15000 else {"courier": 450, "post": 350, "pickup_point": 0}[method]
    suffix = f"pickup #{rng.randint(1, 40)}" if method == "pickup_point" else f"apt {rng.randint(1, 300)}"
    cur.execute(
        """
        INSERT INTO relic.deliveries (order_id, method, city, address, status, delivery_fee)
        VALUES (%s, %s, %s, %s, 'pending', %s)
        """,
        (order_id, method, city, f"{rng.choice(STREETS)} {rng.randint(1, 120)}, {suffix}", fee),
    )
    log(f"online order #{order_id}: {len(items)} piece(s), {total:.2f} RUB, {method} to {city}")


def settle_payments(cur, rng: random.Random, pay_after: int) -> None:
    cur.execute(
        """
        SELECT order_id FROM relic.orders
        WHERE status = 'created' AND order_ts < NOW() - make_interval(secs => %s)
        FOR UPDATE SKIP LOCKED
        """,
        (pay_after,),
    )
    for (order_id,) in cur.fetchall():
        cancelled = rng.random() < 0.08
        order_status, item_status = ("cancelled", "listed") if cancelled else ("paid", "sold")
        cur.execute(
            "UPDATE relic.orders SET status = %s, updated_at = NOW() WHERE order_id = %s",
            (order_status, order_id),
        )
        cur.execute(
            """
            UPDATE relic.items SET status = %s, updated_at = NOW()
            WHERE item_id IN (SELECT item_id FROM relic.order_items WHERE order_id = %s)
            """,
            (item_status, order_id),
        )
        if cancelled:
            cur.execute(
                "UPDATE relic.deliveries SET status = 'cancelled', updated_at = NOW() WHERE order_id = %s",
                (order_id,),
            )
        log(f"order #{order_id} {order_status}")


def move_deliveries(cur, ship_after: int, deliver_after: int) -> None:
    cur.execute(
        """
        UPDATE relic.deliveries d SET status = 'shipped', shipped_at = NOW(), updated_at = NOW()
        FROM relic.orders o
        WHERE o.order_id = d.order_id AND o.status = 'paid'
          AND d.status = 'pending' AND d.created_at < NOW() - make_interval(secs => %s)
        RETURNING d.order_id
        """,
        (ship_after,),
    )
    for (order_id,) in cur.fetchall():
        log(f"order #{order_id} shipped")
    cur.execute(
        """
        UPDATE relic.deliveries SET status = 'delivered', delivered_at = NOW(), updated_at = NOW()
        WHERE status = 'shipped' AND shipped_at < NOW() - make_interval(secs => %s)
        RETURNING order_id
        """,
        (deliver_after,),
    )
    delivered = [r[0] for r in cur.fetchall()]
    if delivered:
        cur.execute(
            "UPDATE relic.orders SET status = 'fulfilled', updated_at = NOW() WHERE order_id = ANY(%s)",
            (delivered,),
        )
        for order_id in delivered:
            log(f"order #{order_id} delivered")


def msk_today():
    return datetime.now(MSK).date()


def run_vans(cur, rng: random.Random, open_hour: int, close_hour: int) -> None:
    # opening hours and stop_date follow Moscow time; timestamps are UTC (Postgres NOW())
    hour = datetime.now(MSK).hour
    today = msk_today()
    if hour >= close_hour:
        cur.execute(
            """
            UPDATE relic.van_stops SET ended_at = NOW()
            WHERE stop_date = %s AND ended_at IS NULL AND started_at <= NOW()
            RETURNING van_id
            """,
            (today,),
        )
        for (van_id,) in cur.fetchall():
            log(f"van {van_id} closed for the day")
        return
    if hour < open_hour:
        return
    # vans without a stop today pick a spot they did not use yesterday and nobody holds today
    cur.execute(
        """
        SELECT v.van_id FROM relic.vans v
        WHERE NOT EXISTS (
          SELECT 1 FROM relic.van_stops s WHERE s.van_id = v.van_id AND s.stop_date = %s
        )
        """,
        (today,),
    )
    for (van_id,) in cur.fetchall():
        cur.execute(
            """
            SELECT l.location_id, l.name FROM relic.stop_locations l
            WHERE l.location_id NOT IN (
              SELECT location_id FROM relic.van_stops
              WHERE stop_date = %s
                 OR (van_id = %s AND stop_date = %s - 1)
            )
            ORDER BY random() LIMIT 1
            """,
            (today, van_id, today),
        )
        row = cur.fetchone()
        if row is None:
            continue
        cur.execute(
            """
            INSERT INTO relic.van_stops (van_id, location_id, stop_date, started_at)
            VALUES (%s, %s, %s, NOW())
            """,
            (van_id, row[0], today),
        )
        log(f"van {van_id} opened at {row[1]}")


def sell_from_van(cur, rng: random.Random) -> None:
    cur.execute(
        """
        SELECT stop_id, van_id FROM relic.van_stops
        WHERE stop_date = %s AND ended_at IS NULL AND started_at <= NOW()
        ORDER BY random() LIMIT 1
        """,
        (msk_today(),),
    )
    stop = cur.fetchone()
    if stop is None:
        return
    items = reserve_items(cur, basket_size(rng))
    if not items:
        return
    customer_id, _ = pick_customer(cur, rng)
    total = sum(float(p) for _, p in items)
    cur.execute(
        """
        INSERT INTO relic.orders (customer_id, order_ts, channel, status, total_amount, van_id, stop_id)
        VALUES (%s, NOW(), 'van', 'fulfilled', %s, %s, %s)
        RETURNING order_id
        """,
        (customer_id, total, stop[1], stop[0]),
    )
    order_id = cur.fetchone()[0]
    cur.executemany(
        "INSERT INTO relic.order_items (order_id, item_id, unit_price, qty) VALUES (%s, %s, %s, 1)",
        [(order_id, item_id, price) for item_id, price in items],
    )
    cur.execute(
        "UPDATE relic.items SET status = 'sold', updated_at = NOW() WHERE item_id = ANY(%s)",
        ([i for i, _ in items],),
    )
    log(f"van {stop[1]} sold {len(items)} piece(s) for {total:.2f} RUB (order #{order_id})")


def restock(cur, rng: random.Random) -> None:
    if rng.random() < 0.5:
        category = rng.choice(list(CATEGORIES))
        grade = rng.choice(GRADES)
        price = round(rng.uniform(800, 45000) if grade in ("S", "A") else rng.uniform(500, 12000), 2)
        cur.execute(
            """
            INSERT INTO relic.items (title, category, brand, grade, status, list_price)
            VALUES (%s, %s, %s, %s, 'sourced', %s)
            RETURNING item_id
            """,
            (rng.choice(CATEGORIES[category]), category, rng.choice(BRANDS), grade, price),
        )
        item_id = cur.fetchone()[0]
        cur.execute(
            "UPDATE relic.items SET title = title || ' #' || item_id WHERE item_id = %s", (item_id,)
        )
        log(f"sourced new {category} piece #{item_id}")
    else:
        cur.execute(
            """
            UPDATE relic.items SET status = 'listed', listed_at = NOW(), updated_at = NOW()
            WHERE item_id = (
              SELECT item_id FROM relic.items WHERE status = 'sourced'
              ORDER BY item_id LIMIT 1 FOR UPDATE SKIP LOCKED
            )
            RETURNING item_id
            """
        )
        row = cur.fetchone()
        if row:
            log(f"piece #{row[0]} listed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    env = os.environ.get
    parser.add_argument("--pg-host", default=env("SOURCE_PG_HOST", "postgres-source"))
    parser.add_argument("--pg-port", type=int, default=int(env("SOURCE_PG_PORT", "5432")))
    parser.add_argument("--pg-db", default=env("SOURCE_PG_DB", "source"))
    parser.add_argument("--pg-user", default=env("SOURCE_PG_USER", "source"))
    parser.add_argument("--pg-password", default=env("SOURCE_PG_PASSWORD", "source"))
    parser.add_argument("--tick", type=float, default=float(env("SIM_TICK_SEC", "5")), help="seconds between ticks")
    parser.add_argument("--online-rate", type=float, default=float(env("SIM_ONLINE_RATE", "0.2")),
                        help="chance of an online order per tick")
    parser.add_argument("--van-rate", type=float, default=float(env("SIM_VAN_RATE", "0.15")),
                        help="chance of a van sale per tick while a van is open")
    parser.add_argument("--restock-rate", type=float, default=float(env("SIM_RESTOCK_RATE", "0.3")))
    parser.add_argument("--pay-after", type=int, default=int(env("SIM_PAY_AFTER_SEC", "30")))
    parser.add_argument("--ship-after", type=int, default=int(env("SIM_SHIP_AFTER_SEC", "120")))
    parser.add_argument("--deliver-after", type=int, default=int(env("SIM_DELIVER_AFTER_SEC", "300")))
    parser.add_argument("--van-open-hour", type=int, default=int(env("SIM_VAN_OPEN_HOUR", "10")))
    parser.add_argument("--van-close-hour", type=int, default=int(env("SIM_VAN_CLOSE_HOUR", "22")))
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port, dbname=args.pg_db, user=args.pg_user, password=args.pg_password
    )
    log(f"simulator started against {args.pg_host}:{args.pg_port}/{args.pg_db}, tick={args.tick}s")

    steps = [
        (lambda c: settle_payments(c, rng, args.pay_after), 1.0),
        (lambda c: move_deliveries(c, args.ship_after, args.deliver_after), 1.0),
        (lambda c: run_vans(c, rng, args.van_open_hour, args.van_close_hour), 1.0),
        (lambda c: place_online_order(c, rng), args.online_rate),
        (lambda c: sell_from_van(c, rng), args.van_rate),
        (lambda c: restock(c, rng), args.restock_rate),
    ]
    while True:
        for step, rate in steps:
            if rng.random() >= rate:
                continue
            # one transaction per step: a failed step rolls back alone
            try:
                with conn, conn.cursor() as cur:
                    step(cur)
            except psycopg2.Error as e:
                log(f"step failed, rolled back: {e}")
        time.sleep(args.tick)


if __name__ == "__main__":
    main()
