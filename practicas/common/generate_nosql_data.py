# Databricks notebook source
"""Fuentes JSON reproducibles para el laboratorio de modelado documental.

Usa solamente la biblioteca estándar. Cada bloque conserva las mismas métricas,
pero tiene claves propias; test = 1 bloque y small = 100 bloques.
"""

import copy
import json


NOSQL_BLOCKS = {"test": 1, "small": 100}

# Precios históricos del pedido, en USD ficticios. El catálogo actual tiene
# otro precio para P001: permite discutir snapshots y referencias.
_PRODUCTS = [
    ("P001", "Notebook", "tecnologia", 1000),
    ("P002", "Auriculares", "tecnologia", 100),
    ("P003", "Mate", "hogar", 20),
    ("P004", "Termo", "hogar", 50),
    ("P005", "Libro", "libros", 30),
    ("P006", "Lampara", "hogar", 80),
]

# Cliente, estado, puntaje heurístico y líneas (producto, cantidad).
_ORDERS = [
    (1, "paid", 0.10, [(1, 1), (2, 2)]),
    (1, "paid", 0.20, [(3, 2), (4, 1)]),
    (4, "paid", 0.15, [(5, 3)]),
    (1, "cancelled", 0.30, [(2, 1), (6, 1)]),
    (3, "paid", 0.95, [(1, 2)]),
    (2, "pending", 0.10, [(3, 1), (5, 1)]),
    (1, "paid", 0.85, [(4, 2), (6, 1)]),
    (3, "paid", 0.40, [(2, 3)]),
    (1, "paid", 0.92, [(1, 1), (5, 1)]),
    (2, "paid", 0.50, [(3, 5)]),
    (1, "pending", 0.20, []),
    (3, "paid", 0.88, [(6, 2), (2, 1)]),
]


def build_nosql_sources(scale="test"):
    """Devuelve líneas JSON de pedidos, clientes y productos.

    Por bloque: 12 pedidos aceptables + JSON roto + null JSON + array raíz
    + pedido con total N/A. El pedido 11 tiene items vacío; el 12 usa un
    total numérico representado como texto. Los cupones de 9..12 son,
    respectivamente, ausente, null explícito, texto vacío y texto con valor.
    """
    if scale not in NOSQL_BLOCKS:
        raise ValueError("Usá test o small para el laboratorio NoSQL")

    def encode(value):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)

    sources = {"orders": [], "customers": [], "products": []}
    for product_id, name, category, price in _PRODUCTS:
        sources["products"].append(encode({
            "product_id": product_id, "name": name, "category": category,
            "current_price": price + (100 if product_id == "P001" else 0),
            "currency": "USD",
        }))

    for block in range(NOSQL_BLOCKS[scale]):
        customers = {}
        for local_id, country in enumerate(["AR", "UY", "BR", "UY"], 1):
            customer = {
                "customer_id": f"C{block * 4 + local_id:04d}",
                "name": f"Cliente ficticio {block * 4 + local_id}",
                "country": country,
                "email": f"cliente{block * 4 + local_id}@example.test",
            }
            customers[local_id] = customer
            sources["customers"].append(encode(customer))

        first_order = None
        for local_id, (customer_id, status, risk_score, lines) in enumerate(_ORDERS, 1):
            customer = customers[customer_id]
            items = []
            for product_index, quantity in lines:
                product_id, name, category, price = _PRODUCTS[product_index - 1]
                items.append({
                    "product_id": product_id, "name": name, "category": category,
                    "quantity": quantity, "unit_price": price,
                })
            total = sum(item["quantity"] * item["unit_price"] for item in items)
            order = {
                "order_id": f"O{block * 12 + local_id:06d}",
                "schema_version": 1 if local_id <= 8 else 2,
                "created_at": f"2026-09-{1 + (local_id - 1) % 4:02d}T12:00:00Z",
                "status": status,
                "customer_id": customer["customer_id"],
                "customer_snapshot": {"name": customer["name"], "country": customer["country"]},
                "shipping": {"country": customer["country"], "city": "Ciudad ficticia"},
                "items": items,
                "total": f"{total:.2f}" if local_id == 12 else total,
                "currency": "USD",
                "payment": {"method": "card" if local_id % 2 else "wallet"},
                "risk": {"score": risk_score},
            }
            if local_id >= 9:
                order["customer_snapshot"]["loyalty_level"] = "gold"
                order["device"] = {"id": f"device_{block}_shared", "platform": "web"}
            if local_id == 10:
                order["coupon"] = None
            elif local_id == 11:
                order["coupon"] = ""
            elif local_id == 12:
                order["coupon"] = "BIENVENIDA"
            sources["orders"].append(encode(order))
            if local_id == 1:
                first_order = order

        bad_order = copy.deepcopy(first_order)
        bad_order["order_id"] = f"BAD_TOTAL_{block:03d}"
        bad_order["total"] = "N/A"
        sources["orders"].extend([
            '{"order_id": "ROTO",',
            "null",
            encode(["esto", "no", "es", "un pedido"]),
            encode(bad_order),
        ])
    return sources
