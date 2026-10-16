#!/usr/bin/env python3
"""Generate Relic Route demo data for Postgres (not Docker init).

Init stays tiny (smoke). Use this when you need volume for dbt/Grafana/ML.

Examples:
  python scripts/generate_relic_seed.py --scale mvp --sql-out /tmp/relic_seed.sql
  python scripts/generate_relic_seed.py --scale mvp --apply
  python scripts/generate_relic_seed.py --scale large --apply

Scales:
  mvp   — ~100 orders (1–3 pieces each), ~45 days  (phase 2–4)
  large — ~5k orders, ~120 days (phase 3–4 stress / phase 6 ML prep)
"""

from __future__ import annotations

import argparse
import random
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class Scale:
    name: str
    customers: int
    items: int
    locations: int
    vans: int
    stop_days: int
    orders: int
    seed: int


SCALES = {
    "mvp": Scale("mvp", customers=30, items=250, locations=10, vans=3, stop_days=45, orders=100, seed=42),
    "large": Scale("large", customers=200, items=8000, locations=15, vans=5, stop_days=120, orders=5000, seed=42),
}

FIRST_NAMES = [
    "Mara", "Ivan", "Lena", "Kir", "Sofia", "Daria", "Oleg", "Nina", "Roman", "Vera",
    "Gleb", "Asya", "Timur", "Polina", "Yegor", "Alisa", "Mark", "Inna", "Boris", "Katya",
]
LAST_NAMES = [
    "Veil", "Ash", "Noir", "Thorn", "Grave", "Wolfe", "Cross", "Raven", "Shade", "Voss",
    "Crow", "Bone", "Frost", "Hollow", "Kane", "Drake", "Moss", "Pike", "Stone", "Wraith",
]
CITIES = ["Moscow", "Saint Petersburg", "Kazan", "Nizhny Novgorod", "Yekaterinburg"]
DISTRICTS = [
    ("Yakimanka", 55.7298, 37.6014),
    ("Presnensky", 55.7639, 37.5922),
    ("Savyolovsky", 55.8056, 37.5847),
    ("Tverskoy", 55.7553, 37.6291),
    ("Ostankinsky", 55.8260, 37.6377),
    ("Arbat", 55.7520, 37.5910),
    ("Tagansky", 55.7410, 37.6530),
    ("Khamovniki", 55.7270, 37.5750),
    ("Basmanny", 55.7650, 37.6780),
    ("Danilovsky", 55.7100, 37.6280),
    ("Meshchansky", 55.7750, 37.6285),
    ("Zamoskvorechye", 55.7350, 37.6300),
    ("Dorogomilovo", 55.7450, 37.5350),
    ("Sokolinaya Gora", 55.7700, 37.7300),
    ("Alekseevsky", 55.8100, 37.6500),
]
LOCATION_PREFIX = [
    "Gate", "Yard", "Stairs", "Arch", "Bridge", "Square", "Pier", "Court", "Alley", "Market",
]
VAN_NAMES = ["Night Moth", "Bone Spoke", "Crypt Box", "Ash Wagon", "Velvet Hauler"]
VAN_TYPES = ["van", "cargo_bike", "popup_box", "van", "cargo_bike"]
CATEGORIES = {
    "jewelry": ["Mourning Brooch", "Rosary", "Skull Ring", "Cameo Pendant", "Jet Beads", "Lockets"],
    "apparel": ["Velvet Cape", "Opera Gloves", "Corset", "Widow Veil", "Lace Collar", "Frock Coat"],
    "object": ["Cigarette Case", "Doll Head", "Gate Key", "Snuff Box", "Candle Holder", "Wax Seal"],
    "furniture": ["Revival Mirror", "Side Table", "Prayer Bench", "Display Cabinet"],
    "paper": ["Sheet Music", "Calling Cards", "Diary", "Playbill", "Photo Plate"],
}
BRANDS = ["Unknown", "Atelier Noir", "Grave Goods", "Thorn & Co", "WMF", "Breitkopf", "Jumeau?", "Hauser"]
GRADES = ["S", "A", "A", "B", "B", "B", "C"]
# Business runs on Moscow wall-clock time (no DST since 2014); Postgres stores UTC.
MSK = timezone(timedelta(hours=3))
STREETS = ["Pokrovka", "Arbat", "Liteyny", "Baumana", "Lenina", "Sadovaya", "Myasnitskaya", "Nevsky", "Malysheva"]


def sql_str(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def sql_ts(dt: datetime | None) -> str:
    """Moscow wall-clock datetime -> UTC literal."""
    if dt is None:
        return "NULL"
    utc = dt.replace(tzinfo=MSK).astimezone(timezone.utc)
    return f"TIMESTAMP '{utc.strftime('%Y-%m-%d %H:%M:%S')}'"


def sql_date(d: date) -> str:
    return f"DATE '{d.isoformat()}'"


def sql_num(n: float) -> str:
    return f"{n:.2f}"


def insert_block(table: str, columns: list[str], rows: list[str]) -> list[str]:
    if not rows:
        return []
    return [f"INSERT INTO relic.{table} ({', '.join(columns)}) VALUES", ",\n".join(rows) + ";", ""]


def build_stops(
    rng: random.Random, scale: Scale, today: date
) -> list[tuple[int, int, int, date, datetime, datetime | None]]:
    """Every evening 1–2 vans park somewhere new: never the same spot two days in a row."""
    stops: list[tuple[int, int, int, date, datetime, datetime | None]] = []
    last_location: dict[int, int] = {}
    for day_offset in range(scale.stop_days, -1, -1):
        d = today - timedelta(days=day_offset)
        active_vans = rng.sample(range(1, scale.vans + 1), k=min(scale.vans, rng.randint(1, 2)))
        taken: set[int] = set()
        for van_id in sorted(active_vans):
            options = [
                loc for loc in range(1, scale.locations + 1)
                if loc not in taken and loc != last_location.get(van_id)
            ]
            loc_id = rng.choice(options)
            taken.add(loc_id)
            last_location[van_id] = loc_id
            start = datetime.combine(d, time(rng.choice([16, 17, 18]), rng.choice([0, 15, 30])))
            end = start + timedelta(hours=rng.choice([3, 4, 5])) if d < today else None
            stops.append((len(stops) + 1, van_id, loc_id, d, start, end))
    return stops


def online_order_ts(rng: random.Random, scale: Scale, now: datetime) -> datetime:
    # Most online shopping happens in the evening.
    hour = rng.choices(range(24), weights=[1, 1, 1, 1, 1, 1, 2, 3, 4, 4, 5, 5, 6, 6, 6, 6, 7, 8, 9, 10, 10, 9, 6, 3])[0]
    d = now.date() - timedelta(days=rng.randint(0, scale.stop_days))
    ts = datetime.combine(d, time(hour, rng.randint(0, 59), rng.randint(0, 59)))
    if ts > now:
        ts -= timedelta(days=1)
    return ts


def build_sql(scale: Scale) -> str:
    rng = random.Random(scale.seed)
    now = datetime.now(MSK).replace(tzinfo=None, microsecond=0)
    today = now.date()
    lines: list[str] = [
        "-- Generated by scripts/generate_relic_seed.py",
        f"-- scale={scale.name} orders={scale.orders} items={scale.items} generated_at={now.isoformat()}",
        "BEGIN;",
        "TRUNCATE TABLE",
        "  relic.deliveries,",
        "  relic.order_items,",
        "  relic.orders,",
        "  relic.van_stops,",
        "  relic.items,",
        "  relic.customers,",
        "  relic.stop_locations,",
        "  relic.vans",
        "RESTART IDENTITY CASCADE;",
        "",
    ]

    # vans
    van_rows = [
        f"  ({i}, {sql_str(VAN_NAMES[i - 1])}, {sql_str(VAN_TYPES[i - 1])})"
        for i in range(1, scale.vans + 1)
    ]
    lines += insert_block("vans", ["van_id", "name", "van_type"], van_rows)

    # locations
    loc_rows = []
    for i in range(1, scale.locations + 1):
        district, lat, lon = DISTRICTS[(i - 1) % len(DISTRICTS)]
        jitter_lat = lat + rng.uniform(-0.01, 0.01)
        jitter_lon = lon + rng.uniform(-0.01, 0.01)
        name = f"{district} {LOCATION_PREFIX[(i - 1) % len(LOCATION_PREFIX)]}"
        loc_rows.append(
            f"  ({i}, {sql_str(name)}, {sql_str(district)}, {jitter_lat:.6f}, {jitter_lon:.6f})"
        )
    lines += insert_block("stop_locations", ["location_id", "name", "district", "lat", "lon"], loc_rows)

    # customers — all registered before the order window starts
    customer_city: dict[int, str] = {}
    cust_rows = []
    for i in range(1, scale.customers + 1):
        fn = FIRST_NAMES[(i - 1) % len(FIRST_NAMES)]
        ln = LAST_NAMES[(i * 3) % len(LAST_NAMES)]
        if i > len(FIRST_NAMES):
            ln = f"{ln}{i}"
        email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
        city = CITIES[(i - 1) % len(CITIES)]
        customer_city[i] = city
        created = now - timedelta(days=scale.stop_days + rng.randint(1, 180), hours=rng.randint(0, 23))
        cust_rows.append(
            f"  ({i}, {sql_str(email)}, {sql_str(f'{fn} {ln}')}, {sql_str(city)}, {sql_ts(created)})"
        )
    lines += insert_block("customers", ["customer_id", "email", "full_name", "city", "created_at"], cust_rows)

    # van stops
    stops = build_stops(rng, scale, today)
    stop_rows = [
        f"  ({sid}, {van_id}, {loc_id}, {sql_date(d)}, {sql_ts(start)}, {sql_ts(end)})"
        for sid, van_id, loc_id, d, start, end in stops
    ]
    lines += insert_block(
        "van_stops", ["stop_id", "van_id", "location_id", "stop_date", "started_at", "ended_at"], stop_rows
    )

    # item pool
    items: list[dict] = []
    for i in range(1, scale.items + 1):
        category = rng.choice(list(CATEGORIES.keys()))
        grade = rng.choice(GRADES)
        price = round(rng.uniform(800, 45000) if grade in ("S", "A") else rng.uniform(500, 12000), 2)
        items.append(
            {
                "item_id": i,
                "title": f"{rng.choice(CATEGORIES[category])} #{i}",
                "category": category,
                "brand": rng.choice(BRANDS),
                "grade": grade,
                "list_price": price,
                "status": "listed",
                "listed_at": now - timedelta(days=rng.randint(1, scale.stop_days + 10), hours=rng.randint(0, 23)),
                "updated_at": None,
            }
        )
    available = items[:]
    rng.shuffle(available)

    # orders: decide when and where first, then fill baskets in time order
    open_stops = [s for s in stops if s[4] < now]
    drafts: list[tuple[datetime, str, tuple | None]] = []
    for _ in range(scale.orders):
        if open_stops and rng.random() < 0.35:
            stop = rng.choice(open_stops)
            _, _, _, _, started_at, ended_at = stop
            window_end = min(ended_at or now, now)
            span = (window_end - started_at).total_seconds()
            ts = started_at + timedelta(seconds=rng.uniform(0, span))
            drafts.append((ts.replace(microsecond=0), "van", stop))
        else:
            drafts.append((online_order_ts(rng, scale, now), "online", None))
    drafts.sort(key=lambda d: d[0])

    order_rows: list[str] = []
    oi_rows: list[str] = []
    delivery_rows: list[str] = []
    keep_listed = max(20, scale.items // 10)

    for ts, channel, stop in drafts:
        basket_size = rng.choices([1, 2, 3], weights=[0.75, 0.2, 0.05])[0]
        # only items already on the shelf at order time
        basket = [it for it in available if it["listed_at"] < ts][:basket_size]
        if not basket or len(available) - len(basket) < keep_listed:
            continue
        for it in basket:
            available.remove(it)

        order_id = len(order_rows) + 1
        customer_id = rng.randint(1, scale.customers)
        total = round(sum(it["list_price"] for it in basket), 2)
        age = now - ts

        if channel == "van":
            # paid and handed over on the spot
            status, updated = "fulfilled", ts
            van_sql, stop_sql = str(stop[1]), str(stop[0])
        else:
            van_sql = stop_sql = "NULL"
            method = rng.choices(["courier", "post", "pickup_point"], weights=[0.45, 0.3, 0.25])[0]
            fee = 0.0 if total >= 15000 else {"courier": 450.0, "post": 350.0, "pickup_point": 0.0}[method]
            city = customer_city[customer_id]
            address = (
                f"{rng.choice(STREETS)} {rng.randint(1, 120)}, pickup #{rng.randint(1, 40)}"
                if method == "pickup_point"
                else f"{rng.choice(STREETS)} {rng.randint(1, 120)}, apt {rng.randint(1, 300)}"
            )
            shipped_at = ts + timedelta(hours=rng.uniform(4, 30))
            transit_h = {"courier": (3, 24), "pickup_point": (24, 60), "post": (48, 144)}[method]
            delivered_at = shipped_at + timedelta(hours=rng.uniform(*transit_h))

            if rng.random() < 0.04:
                status, d_status = "cancelled", "cancelled"
                updated = min(ts + timedelta(hours=rng.uniform(0.5, 6)), now)
                shipped_at = delivered_at = None
            elif age < timedelta(minutes=30) and rng.random() < 0.5:
                status, d_status, updated = "created", "pending", ts
                shipped_at = delivered_at = None
            elif delivered_at <= now:
                status, d_status, updated = "fulfilled", "delivered", delivered_at
            elif shipped_at <= now:
                status, d_status, updated = "paid", "shipped", shipped_at
                delivered_at = None
            else:
                status, d_status, updated = "paid", "pending", ts
                shipped_at = delivered_at = None

            delivery_rows.append(
                f"  ({order_id}, {sql_str(method)}, {sql_str(city)}, {sql_str(address)}, {sql_str(d_status)}, "
                f"{sql_num(fee)}, {sql_ts(ts)}, {sql_ts(shipped_at)}, {sql_ts(delivered_at)}, {sql_ts(updated)})"
            )

        for it in basket:
            # cancelled orders put the piece back on the shelf
            it["status"] = {"cancelled": "listed", "created": "reserved"}.get(status, "sold")
            it["updated_at"] = updated if status == "cancelled" else ts
            oi_rows.append(f"  ({order_id}, {it['item_id']}, {sql_num(it['list_price'])}, 1)")

        order_rows.append(
            f"  ({order_id}, {customer_id}, {sql_ts(ts)}, {sql_str(channel)}, {sql_str(status)}, 'RUB', "
            f"{sql_num(total)}, {van_sql}, {stop_sql}, {sql_ts(updated)})"
        )

    # part of the unsold stock is still being prepared or was pulled from sale
    for it in available[: max(3, scale.items // 50)]:
        it["status"], it["listed_at"] = "sourced", None
    for it in available[-max(2, scale.items // 60):]:
        it["status"] = "archived"

    item_rows = []
    for it in items:
        updated = it["updated_at"] or it["listed_at"] or now - timedelta(days=rng.randint(0, 5))
        item_rows.append(
            f"  ({it['item_id']}, {sql_str(it['title'])}, {sql_str(it['category'])}, "
            f"{sql_str(it['brand'])}, {sql_str(it['grade'])}, {sql_str(it['status'])}, "
            f"{sql_num(it['list_price'])}, {sql_ts(it['listed_at'])}, {sql_ts(updated)})"
        )
    lines += insert_block(
        "items",
        ["item_id", "title", "category", "brand", "grade", "status", "list_price", "listed_at", "updated_at"],
        item_rows,
    )
    lines += insert_block(
        "orders",
        ["order_id", "customer_id", "order_ts", "channel", "status", "currency", "total_amount",
         "van_id", "stop_id", "updated_at"],
        order_rows,
    )
    lines += insert_block("order_items", ["order_id", "item_id", "unit_price", "qty"], oi_rows)
    lines += insert_block(
        "deliveries",
        ["order_id", "method", "city", "address", "status", "delivery_fee",
         "created_at", "shipped_at", "delivered_at", "updated_at"],
        delivery_rows,
    )

    # explicit ids were inserted: move identity sequences past them
    for table, col in [("customers", "customer_id"), ("items", "item_id"), ("van_stops", "stop_id"), ("orders", "order_id")]:
        lines.append(
            f"SELECT setval(pg_get_serial_sequence('relic.{table}', '{col}'), "
            f"(SELECT COALESCE(MAX({col}), 0) + 1 FROM relic.{table}), false);"
        )
    lines.append("COMMIT;")
    lines.append("")
    lines.append(
        "SELECT 'customers' AS entity, COUNT(*)::text AS n FROM relic.customers "
        "UNION ALL SELECT 'items', COUNT(*)::text FROM relic.items "
        "UNION ALL SELECT 'van_stops', COUNT(*)::text FROM relic.van_stops "
        "UNION ALL SELECT 'orders', COUNT(*)::text FROM relic.orders "
        "UNION ALL SELECT 'order_items', COUNT(*)::text FROM relic.order_items "
        "UNION ALL SELECT 'deliveries', COUNT(*)::text FROM relic.deliveries "
        "ORDER BY 1;"
    )
    return "\n".join(lines)


def apply_via_docker(sql: str, compose_dir: Path) -> None:
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres-source",
        "psql",
        "-U",
        "source",
        "-d",
        "source",
        "-v",
        "ON_ERROR_STOP=1",
    ]
    proc = subprocess.run(
        cmd,
        input=sql,
        text=True,
        cwd=str(compose_dir),
        capture_output=True,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"psql failed with code {proc.returncode}")
    sys.stdout.write(proc.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scale", choices=sorted(SCALES), default="mvp")
    parser.add_argument("--sql-out", type=Path, help="Write SQL to file instead of stdout")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply SQL into docker compose service postgres-source",
    )
    args = parser.parse_args()

    scale = SCALES[args.scale]
    sql = build_sql(scale)
    repo_root = Path(__file__).resolve().parents[1]

    if args.apply:
        apply_via_docker(sql, repo_root)
        print(f"Applied scale={scale.name} to postgres-source", file=sys.stderr)
        return

    if args.sql_out:
        args.sql_out.write_text(sql, encoding="utf-8")
        print(f"Wrote {args.sql_out}", file=sys.stderr)
        return

    sys.stdout.write(sql)


if __name__ == "__main__":
    main()
