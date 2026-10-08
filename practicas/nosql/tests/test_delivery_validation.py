"""Casos negativos para contratos de entrega, independientes de VARIANT/Spark."""

import ast
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace
import unittest


LAB = Path(__file__).resolve().parents[1]


def notebook_function(path, name):
    notebook = json.loads(path.read_text(encoding="utf-8"))
    for cell in notebook["cells"]:
        source = "".join(cell["source"])
        if cell["cell_type"] != "code" or source.startswith("%"):
            continue
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
    raise AssertionError(f"Falta {name} en {path}")


def correct_order():
    new = SimpleNamespace(
        version=3, gift_type="OBJECT<message: STRING, wrapped: BOOLEAN>",
        total=Decimal("60.00"), status="paid", currency="USD", customer_id="C0001",
        items_type="ARRAY<OBJECT<product_id: STRING>>", risk_score=0.05,
        gift_message_type="STRING", gift_message="Entrega del laboratorio",
        gift_wrapped_type="BOOLEAN", gift_wrapped=True,
        created_at=datetime(2026, 9, 5, 12),
        snapshot_name_type="STRING", snapshot_name="Cliente ficticio 1",
        snapshot_country_type="STRING", snapshot_country="AR",
        shipping_country_type="STRING", shipping_country="AR",
        shipping_city_type="STRING", shipping_city="Ciudad ficticia",
        payment_method_type="STRING", payment_method="card")
    lines = [SimpleNamespace(product_id="P005", category="libros", quantity=2, price=Decimal("30.00"))]
    return new, lines


class DeliveryValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        namespace = {}
        for name in ["validate_delivery_schema", "validate_evolved_order"]:
            node = notebook_function(LAB / "03_desafio.ipynb", name)
            exec(compile(ast.Module(body=[node], type_ignores=[]), "student-guard", "exec"), namespace)
        cls.validate_schema = staticmethod(namespace["validate_delivery_schema"])
        cls.validate_order = staticmethod(namespace["validate_evolved_order"])

    def test_view_contract_rejects_convertible_wrong_types(self):
        expected = {"category": "string", "units": "bigint", "amount": "decimal(18,2)"}
        self.validate_schema(SimpleNamespace(dtypes=list(expected.items())), expected, "ventas")
        for column in ["units", "amount"]:
            with self.subTest(column=column):
                wrong = {**expected, column: "double"}
                with self.assertRaises(AssertionError):
                    self.validate_schema(SimpleNamespace(dtypes=list(wrong.items())), expected, "ventas")
        for wrong in [{**expected, "orders": "bigint"}, {"category": "string", "units": "bigint"}]:
            with self.assertRaises(AssertionError):
                self.validate_schema(SimpleNamespace(dtypes=list(wrong.items())), expected, "ventas")

    def test_evolution_rejects_missing_fields_text_boolean_or_wrong_category(self):
        self.validate_order(*correct_order())
        for field, value in [("gift_wrapped_type", "STRING"), ("gift_message", ""),
                             ("created_at", None), ("snapshot_name", None),
                             ("shipping_city", ""), ("payment_method", None)]:
            with self.subTest(field=field):
                new, lines = correct_order()
                setattr(new, field, value)
                with self.assertRaises(AssertionError):
                    self.validate_order(new, lines)
        new, lines = correct_order()
        lines[0].category = "hogar"
        with self.assertRaises(AssertionError):
            self.validate_order(new, lines)
        with self.assertRaises(AssertionError):
            self.validate_order(None, lines)

    def test_student_and_teacher_use_identical_guards(self):
        for name in ["validate_delivery_schema", "validate_evolved_order"]:
            student = notebook_function(LAB / "03_desafio.ipynb", name)
            teacher = notebook_function(LAB / "docente/04_solucion.ipynb", name)
            self.assertEqual(ast.dump(student), ast.dump(teacher))


if __name__ == "__main__":
    unittest.main()
