"""Deterministyczny generator wielotabelowego zbioru Mini-E-Commerce dla Lab 7 i Lab 8.

Generuje lekkie pliki CSV/JSON w katalogu ``data/raw``:
- paczkę inicjalną (day1): klientów, produkty oraz zamówienia (z celowymi duplikatami i anomaliami),
- paczkę przyrostową (day2): nowe zamówienia, aktualizacje statusów, nową kolumnę ``promo_code``
  oraz zmiany wymiarów klientów/produktów (pod SCD Type 2 w Lab 8).
"""

from __future__ import annotations

import csv
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path


FIRSTNAMES = [
    "Anna", "Jan", "Katarzyna", "Piotr", "Agnieszka", "Tomasz",
    "Magdalena", "Michał", "Małgorzata", "Marcin", "Karolina", "Paweł",
]
LASTNAMES = [
    "Nowak", "Kowalski", "Wiśniewski", "Wójcik", "Kowalczyk", "Kamiński",
    "Lewandowski", "Zieliński", "Szymański", "Woźniak", "Dąbrowski", "Kozłowski",
]
CITIES = [
    ("Warszawa", "PL"), ("Kraków", "PL"), ("Gdańsk", "PL"),
    ("Wrocław", "PL"), ("Poznań", "PL"), ("Olsztyn", "PL"),
    ("Berlin", "DE"), ("Prague", "CZ"),
]
SEGMENTS = ["STANDARD", "STANDARD", "STANDARD", "PREMIUM", "PREMIUM", "VIP"]
CATEGORIES = {
    "Elektronika": (120.0, 3500.0),
    "Książki": (25.0, 140.0),
    "Dom i Ogród": (40.0, 850.0),
    "Sport": (50.0, 1200.0),
    "Moda": (35.0, 600.0),
    "Gaming": (80.0, 2800.0),
}
PAYMENT_METHODS = ["BLIK", "CARD", "TRANSFER", "COD"]
PROMO_CODES = ["", "", "", "WELCOME10", "SUMMER26", "VIP15"]


def generate_ecommerce_dataset(
    base_dir: str | os.PathLike[str] = "data/raw",
    n_customers: int = 10_000,
    n_products: int = 500,
    n_orders_day1: int = 150_000,
    n_orders_day2_new: int = 15_000,
    n_orders_day2_updates: int = 5_000,
    seed: int = 42,
    force: bool = False,
) -> dict[str, Path]:
    """Tworzy komplet plików wejściowych day1 oraz day2 i zwraca słownik ścieżek."""
    out_dir = Path(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "customers_day1": out_dir / "customers_day1.csv",
        "customers_day2_updates": out_dir / "customers_day2_updates.csv",
        "products_day1": out_dir / "products_day1.json",
        "products_day2_updates": out_dir / "products_day2_updates.json",
        "orders_day1": out_dir / "orders_day1.csv",
        "orders_day2_incremental": out_dir / "orders_day2_incremental.csv",
    }

    if not force and all(p.exists() for p in paths.values()):
        print(f"Dane e-commerce już istnieją w '{out_dir.resolve()}'. Pomijam generowanie.")
        return paths

    rng = random.Random(seed)
    start_date = datetime(2026, 1, 1, 8, 0, 0)

    # 1. Klienci (day1) + celowe braki/duplikaty do czyszczenia w warstwie Silver
    customers_rows = []
    for cid in range(1, n_customers + 1):
        fname = rng.choice(FIRSTNAMES)
        lname = rng.choice(LASTNAMES)
        city, country = rng.choice(CITIES)
        segment = rng.choice(SEGMENTS)
        signup = (start_date - timedelta(days=rng.randint(1, 700))).strftime("%Y-%m-%d")
        email = f"{fname.lower()}.{lname.lower()}.{cid}@example.com"

        # Celowe zabrudzenia (~2% rekordów)
        if cid % 97 == 0:
            email = ""  # brakujący e-mail
        if cid % 113 == 0:
            city = f"  {city.lower()} "  # niespójny format / białe znaki
        customers_rows.append([cid, f"{fname} {lname}", email, city, country, segment, signup])

    # Dodajemy 150 zduplikowanych wierszy klientów (do deduplikacji w Silver)
    for dup_row in rng.sample(customers_rows, k=min(150, len(customers_rows))):
        customers_rows.append(list(dup_row))

    with open(paths["customers_day1"], "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "full_name", "email", "city", "country", "segment", "signup_date"])
        writer.writerows(customers_rows)

    # 2. Aktualizacje klientów (day2 - pod SCD Type 2 w Lab 8)
    upd_customers = []
    for cid in rng.sample(range(1, n_customers + 1), k=min(800, n_customers)):
        city, country = rng.choice(CITIES)
        segment = rng.choice(["PREMIUM", "VIP"])
        upd_customers.append([
            cid,
            f"Klient_{cid}",
            f"updated.{cid}@example.com",
            city,
            country,
            segment,
            "2026-07-01",
            "2026-07-01",
        ])
    # Oraz 200 nowych klientów
    for cid in range(n_customers + 1, n_customers + 201):
        fname = rng.choice(FIRSTNAMES)
        lname = rng.choice(LASTNAMES)
        city, country = rng.choice(CITIES)
        upd_customers.append([
            cid,
            f"{fname} {lname}",
            f"{fname.lower()}.{lname.lower()}.{cid}@example.com",
            city,
            country,
            "STANDARD",
            "2026-07-01",
            "2026-07-01",
        ])

    with open(paths["customers_day2_updates"], "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "full_name", "email", "city", "country", "segment", "signup_date", "effective_date"])
        writer.writerows(upd_customers)

    # 3. Produkty (day1 - JSON Lines)
    product_prices: dict[int, float] = {}
    with open(paths["products_day1"], "w", encoding="utf-8") as f:
        for pid in range(1, n_products + 1):
            category = rng.choice(list(CATEGORIES.keys()))
            low, high = CATEGORIES[category]
            price = round(rng.uniform(low, high), 2)
            product_prices[pid] = price
            rec = {
                "product_id": pid,
                "product_name": f"{category[:4].upper()}-Model-{pid:04d}",
                "category": category,
                "unit_price": price,
                "active": pid % 29 != 0,
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Aktualizacje produktów (day2 - JSON Lines, pod zadanie SCD2 w Lab 8)
    with open(paths["products_day2_updates"], "w", encoding="utf-8") as f:
        for pid in rng.sample(range(1, n_products + 1), k=min(50, n_products)):
            new_cat = rng.choice(list(CATEGORIES.keys()))
            new_price = round(product_prices[pid] * rng.uniform(0.85, 1.25), 2)
            rec = {
                "product_id": pid,
                "product_name": f"{new_cat[:4].upper()}-Model-{pid:04d}",
                "category": new_cat,
                "unit_price": new_price,
                "active": True,
                "effective_date": "2026-07-01",
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # 4. Zamówienia (day1)
    pending_candidates: list[list] = []
    orders_day1_rows: list[list] = []
    for idx in range(1, n_orders_day1 + 1):
        order_id = 100_000 + (idx + 1) // 2
        order_item_id = idx
        cid = rng.randint(1, n_customers)
        pid = rng.randint(1, n_products)
        order_ts = start_date + timedelta(
            days=rng.randint(0, 179),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
        )
        qty = rng.randint(1, 5)
        price = product_prices[pid]
        discount = rng.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15])
        status = rng.choices(
            ["DELIVERED", "SHIPPED", "PENDING", "CANCELLED"],
            weights=[0.70, 0.12, 0.10, 0.08],
            k=1,
        )[0]
        payment = rng.choice(PAYMENT_METHODS)

        # Celowe anomalie jakościowe (~1.5% rekordów) do odfiltrowania w Silver
        if idx % 199 == 0:
            qty = -1
        elif idx % 251 == 0:
            price = -10.0
        elif idx % 311 == 0:
            status = ""

        row = [
            order_item_id,
            order_id,
            cid,
            pid,
            order_ts.strftime("%Y-%m-%d %H:%M:%S"),
            qty,
            price,
            discount,
            status,
            payment,
        ]
        orders_day1_rows.append(row)
        if status in ("PENDING", "SHIPPED") and qty > 0 and price > 0:
            pending_candidates.append(row)

    # Dodajemy 500 duplikatów do day1
    for dup in rng.sample(orders_day1_rows, k=min(500, len(orders_day1_rows))):
        orders_day1_rows.append(list(dup))

    with open(paths["orders_day1"], "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_item_id", "order_id", "customer_id", "product_id",
            "order_ts", "quantity", "unit_price", "discount_pct", "status", "payment_method",
        ])
        writer.writerows(orders_day1_rows)

    # 5. Zamówienia przyrostowe (day2): aktualizacje statusów + nowe zamówienia + nowa kolumna promo_code
    day2_rows: list[list] = []
    for old_row in rng.sample(pending_candidates, k=min(n_orders_day2_updates, len(pending_candidates))):
        updated = list(old_row)
        updated[8] = rng.choices(["DELIVERED", "CANCELLED"], weights=[0.85, 0.15], k=1)[0]
        updated.append("")  # promo_code
        day2_rows.append(updated)

    day2_start = datetime(2026, 7, 1, 0, 0, 0)
    for offset in range(1, n_orders_day2_new + 1):
        order_item_id = n_orders_day1 + offset
        order_id = 500_000 + (offset + 1) // 2
        cid = rng.randint(1, n_customers + 200)
        pid = rng.randint(1, n_products)
        order_ts = day2_start + timedelta(
            days=rng.randint(0, 14),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
        )
        qty = rng.randint(1, 4)
        price = product_prices[pid]
        discount = rng.choice([0.0, 0.0, 0.10, 0.15])
        status = rng.choices(["DELIVERED", "SHIPPED", "PENDING"], weights=[0.6, 0.25, 0.15], k=1)[0]
        payment = rng.choice(PAYMENT_METHODS)
        promo = rng.choice(PROMO_CODES)
        day2_rows.append([
            order_item_id, order_id, cid, pid,
            order_ts.strftime("%Y-%m-%d %H:%M:%S"),
            qty, price, discount, status, payment, promo,
        ])

    with open(paths["orders_day2_incremental"], "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_item_id", "order_id", "customer_id", "product_id",
            "order_ts", "quantity", "unit_price", "discount_pct", "status",
            "payment_method", "promo_code",
        ])
        writer.writerows(day2_rows)

    print(f"Wygenerowano zbiór Mini-E-Commerce w '{out_dir.resolve()}':")
    for name, path in paths.items():
        size_mb = path.stat().st_size / (1024 ** 2)
        print(f"  - {name}: {path.name} ({size_mb:.2f} MB)")

    return paths


if __name__ == "__main__":
    generate_ecommerce_dataset()
