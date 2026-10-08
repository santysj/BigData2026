"""Regresiones del control de entrega; no simulan el motor de streaming."""

import ast
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from practicas.common.streaming_expectations import expected_gold_rows


LAB = Path(__file__).resolve().parents[1]
TYPES = {
    "window_start": "timestamp", "window_end": "timestamp",
    "payment_channel": "string", "event_count": "bigint",
    "total_amount": "decimal(18,2)", "fraud_count": "bigint",
}


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


class Reference:
    def __init__(self, rows, types=None):
        self.rows = rows
        self.dtypes = list((types or TYPES).items())

    def collect(self):
        return self.rows


def correct_reference(factor):
    # Oráculo del histórico batch: incluye late_bad de 999, sin fraude.
    values = [(0, "card", 4, "1419.00", 1), (0, "wallet", 1, "50.00", 0),
              (5, "card", 1, "300.00", 1), (5, "wallet", 1, "80.00", 0),
              (25, "card", 2, "275.00", 0), (25, "wallet", 1, "150.00", 0)]
    rows = []
    for minute, channel, count, amount, fraud in values:
        start = datetime(2026, 3, 12, 12, minute)
        rows.append(SimpleNamespace(
            window_start=start, window_end=start + timedelta(minutes=5),
            payment_channel=channel, event_count=count * factor,
            total_amount=Decimal(amount) * factor, fraud_count=fraud * factor))
    return Reference(rows)


class DeliveryValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        node = notebook_function(LAB / "07_desafio.ipynb", "validate_batch_reference")
        namespace = {"expected_gold_rows": expected_gold_rows}
        exec(compile(ast.Module(body=[node], type_ignores=[]), "student-guard", "exec"), namespace)
        cls.validate = staticmethod(namespace["validate_batch_reference"])

    def test_reference_rejects_wrong_window_or_fraud_with_correct_totals(self):
        for factor in [1, 20, 100]:
            self.validate(correct_reference(factor), factor)
        for change in ["window_end", "fraud_count", "date", "duplicate"]:
            with self.subTest(change=change):
                reference = correct_reference(1)
                if change == "window_end":
                    reference.rows[0].window_end += timedelta(minutes=5)
                elif change == "fraud_count":
                    reference.rows[0].fraud_count += 1
                elif change == "date":
                    reference.rows[0].window_start += timedelta(days=1)
                    reference.rows[0].window_end += timedelta(days=1)
                else:
                    reference.rows.append(deepcopy(reference.rows[0]))
                with self.assertRaises(AssertionError):
                    self.validate(reference, 1)

    def test_reference_requires_declared_types_and_columns(self):
        for column, wrong_type in [("total_amount", "double"), ("event_count", "double"),
                                   ("window_start", "timestamp_ntz")]:
            with self.subTest(column=column):
                types = {**TYPES, column: wrong_type}
                with self.assertRaises(AssertionError):
                    self.validate(Reference(correct_reference(1).rows, types), 1)
        types = {name: dtype for name, dtype in TYPES.items() if name != "fraud_count"}
        with self.assertRaises(AssertionError):
            self.validate(Reference(correct_reference(1).rows, types), 1)

    def test_student_and_teacher_use_identical_guard(self):
        student = notebook_function(LAB / "07_desafio.ipynb", "validate_batch_reference")
        teacher = notebook_function(LAB / "docente/08_solucion_desafio.ipynb", "validate_batch_reference")
        self.assertEqual(ast.dump(student), ast.dump(teacher))


if __name__ == "__main__":
    unittest.main()
