"""Checks locales del material; los notebooks verifican Spark en Databricks."""

import ast
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import unittest

from practicas.common.generate_nosql_data import build_nosql_sources


LAB = Path(__file__).resolve().parents[1]


def read_orders(lines):
    """Oráculo local para los cuatro casos de rechazo del dataset didáctico."""
    accepted, reasons = [], Counter()
    for line in lines:
        try:
            order = json.loads(line)
        except json.JSONDecodeError:
            reasons["JSON_INVALIDO"] += 1
            continue
        if order is None:
            reasons["JSON_NULO"] += 1
        elif not isinstance(order, dict):
            reasons["DOCUMENTO_NO_OBJETO"] += 1
        else:
            try:
                Decimal(str(order["total"]))
            except InvalidOperation:
                reasons["TOTAL_NO_NUMERICO"] += 1
                continue
            accepted.append(order)
    return accepted, reasons


class SourcesTests(unittest.TestCase):
    def test_reproducible_and_supported_scales(self):
        for scale in ["test", "small"]:
            with self.subTest(scale=scale):
                self.assertEqual(build_nosql_sources(scale), build_nosql_sources(scale))
        with self.assertRaises(ValueError):
            build_nosql_sources("demo")

    def test_ingestion_references_versions_and_line_grain(self):
        for scale, factor in [("test", 1), ("small", 100)]:
            with self.subTest(scale=scale):
                sources = build_nosql_sources(scale)
                orders, reasons = read_orders(sources["orders"])
                customers = [json.loads(line) for line in sources["customers"]]
                products = [json.loads(line) for line in sources["products"]]
                self.assertEqual(len(sources["orders"]), 16 * factor)
                self.assertEqual(len(orders), 12 * factor)
                self.assertEqual(reasons, Counter({name: factor for name in [
                    "JSON_INVALIDO", "JSON_NULO", "DOCUMENTO_NO_OBJETO", "TOTAL_NO_NUMERICO"]}))
                self.assertEqual(Counter(o["schema_version"] for o in orders), {1: 8 * factor, 2: 4 * factor})
                self.assertEqual(len({o["order_id"] for o in orders}), len(orders))
                customer_ids = {c["customer_id"] for c in customers}
                product_ids = {p["product_id"] for p in products}
                self.assertEqual(len(customer_ids), 4 * factor)
                self.assertEqual(len(product_ids), 6)
                self.assertTrue(all(o["customer_id"] in customer_ids for o in orders))
                self.assertTrue(all(i["product_id"] in product_ids for o in orders for i in o["items"]))
                lines = [(o["order_id"], pos) for o in orders for pos, _ in enumerate(o["items"])]
                self.assertEqual(len(lines), 18 * factor)
                self.assertEqual(len(set(lines)), len(lines))
                for order in orders:
                    self.assertEqual(order["currency"], "USD")
                    self.assertEqual(Decimal(str(order["total"])), sum(
                        (Decimal(str(i["unit_price"])) * i["quantity"] for i in order["items"]), Decimal(0)))

    def test_paid_sales_and_alerts_against_business_expectations(self):
        for scale, factor in [("test", 1), ("small", 100)]:
            with self.subTest(scale=scale):
                orders, _ = read_orders(build_nosql_sources(scale)["orders"])
                paid = [o for o in orders if o["status"] == "paid"]
                self.assertEqual(len(paid), 9 * factor)
                self.assertEqual(sum(len(o["items"]) for o in paid), 14 * factor)
                sales = defaultdict(lambda: {"units": 0, "amount": Decimal(0), "orders": set()})
                for order in paid:
                    for item in order["items"]:
                        category = sales[item["category"]]
                        category["units"] += item["quantity"]
                        category["amount"] += Decimal(str(item["unit_price"])) * item["quantity"]
                        category["orders"].add(order["order_id"])
                expected = {
                    "tecnologia": (10 * factor, Decimal(4600) * factor, 5 * factor),
                    "hogar": (13 * factor, Decimal(530) * factor, 4 * factor),
                    "libros": (4 * factor, Decimal(120) * factor, 2 * factor),
                }
                self.assertEqual({k: (v["units"], v["amount"], len(v["orders"])) for k, v in sales.items()}, expected)
                self.assertEqual(sum(v["amount"] for v in sales.values()), Decimal(5250) * factor)
                alert_ids = {o["order_id"] for o in paid
                             if Decimal(str(o["total"])) >= 1000 and o["risk"]["score"] >= 0.8}
                self.assertEqual(alert_ids, {f"O{block * 12 + index:06d}"
                                            for block in range(factor) for index in [5, 9]})

    def test_null_empty_array_type_change_and_historical_price(self):
        sources = build_nosql_sources()
        orders, _ = read_orders(sources["orders"])
        by_id = {o["order_id"]: o for o in orders}
        self.assertNotIn("coupon", by_id["O000009"])
        self.assertIsNone(by_id["O000010"]["coupon"])
        self.assertEqual(by_id["O000011"]["coupon"], "")
        self.assertEqual(by_id["O000012"]["coupon"], "BIENVENIDA")
        self.assertEqual(by_id["O000011"]["items"], [])
        self.assertEqual(by_id["O000011"]["status"], "pending")
        self.assertEqual(by_id["O000012"]["total"], "260.00")
        products = {p["product_id"]: p for p in map(json.loads, sources["products"])}
        self.assertEqual(by_id["O000001"]["items"][0]["unit_price"], 1000)
        self.assertEqual(products["P001"]["current_price"], 1100)


class NotebookTests(unittest.TestCase):
    def test_python_cells_and_run_dependencies(self):
        notebooks = sorted(LAB.rglob("*.ipynb"))
        self.assertEqual(len(notebooks), 5)
        for path in notebooks:
            notebook = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(notebook["nbformat"], 4)
            for index, cell in enumerate(notebook["cells"], 1):
                if cell["cell_type"] != "code":
                    continue
                source = "".join(cell["source"])
                with self.subTest(notebook=path.name, cell=index):
                    self.assertEqual(cell["outputs"], [])
                    self.assertIsNone(cell["execution_count"])
                    if source.startswith("%run "):
                        relative = source.removeprefix("%run ").strip()
                        self.assertTrue((path.parent / (relative + ".py")).resolve().is_file())
                    elif source.startswith("%sql\n"):
                        self.assertTrue(source.removeprefix("%sql\n").strip())
                    else:
                        ast.parse(source, filename=f"{path.name}:cell-{index}")

    def test_teacher_new_document_matches_evolution_contract(self):
        notebook = json.loads((LAB / "docente/04_solucion.ipynb").read_text(encoding="utf-8"))
        source = next("".join(cell["source"]) for cell in notebook["cells"]
                      if "".join(cell["source"]).startswith("new_order ="))
        new_order = ast.literal_eval(ast.parse(source).body[0].value)
        orders, _ = read_orders(build_nosql_sources()["orders"])
        self.assertNotIn(new_order["order_id"], {o["order_id"] for o in orders})
        self.assertEqual(new_order["schema_version"], 3)
        self.assertEqual(new_order["customer_id"], "C0001")
        self.assertEqual(new_order["total"], 60)
        self.assertEqual(new_order["status"], "paid")
        self.assertIsInstance(new_order["gift"]["message"], str)
        self.assertIs(new_order["gift"]["wrapped"], True)
        self.assertEqual(new_order["items"][0]["quantity"] * new_order["items"][0]["unit_price"], 60)
        self.assertTrue(all(key in new_order for key in
                            ["created_at", "payment", "risk", "shipping", "customer_snapshot"]))


if __name__ == "__main__":
    unittest.main()
