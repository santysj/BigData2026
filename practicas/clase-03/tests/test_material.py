"""Tests sin Spark; el modelo local es un oráculo del fixture, no del runtime."""

import ast
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from practicas.common.config import CourseConfig
from practicas.common.generate_stream_data import (
    STREAM_BATCHES, STREAM_REPETITIONS, build_stream_rows, normalize_stream_batch,
    publish_stream_batch, stream_input_directory, stream_rows_to_jsonl,
)
from practicas.common.streaming_expectations import expected_gold_rows, expected_stream_counts
from practicas.common.streaming_support import stream_checkpoint


LAB = Path(__file__).resolve().parents[1]


def model_stages(config):
    """Modelo de llegadas: watermark previo, dedup de identidad y cierre de ventana.

    Trata cada archivo como una llegada. No predice cuántos microbatches usa
    Spark ni sus batchIds/contadores internos de estado.
    """
    cumulative = {key: 0 for key in ["bronze", "classified", "valid", "quarantine", "unique"]}
    max_seen = None
    identities = set()
    accepted = []
    for batch_id in STREAM_BATCHES:
        rows = build_stream_rows(config, batch_id)
        watermark_before = max_seen - timedelta(minutes=10) if max_seen else None
        valid = []
        for row in rows:
            try:
                amount = Decimal(row["amount"])
            except InvalidOperation:
                cumulative["quarantine"] += 1
                continue
            if int(row["customer_id"]) >= config.rows["customers"]:
                cumulative["quarantine"] += 1
                continue
            valid.append((row, amount, datetime.fromisoformat(row["event_ts"].replace("Z", "+00:00"))))
        cumulative["bronze"] += len(rows)
        cumulative["classified"] += len(rows)
        cumulative["valid"] += len(valid)
        for row, amount, timestamp in valid:
            if watermark_before and timestamp <= watermark_before:
                continue
            if row["event_id"] in identities:
                continue
            identities.add(row["event_id"])
            accepted.append((row, amount, timestamp))
            cumulative["unique"] += 1
        max_seen = max([time for _, _, time in valid] + ([max_seen] if max_seen else []))
        watermark_after = max_seen - timedelta(minutes=10)
        groups = defaultdict(lambda: [0, Decimal(0), 0])
        for row, amount, time in accepted:
            if row["event_type"] != "purchase":
                continue
            start = time.replace(minute=(time.minute // 5) * 5, second=0, microsecond=0)
            if start + timedelta(minutes=5) < watermark_after:
                key = (start.strftime("%Y-%m-%d %H:%M:%S"), row["payment_channel"])
                groups[key][0] += 1
                groups[key][1] += amount
                groups[key][2] += int(row["is_fraud"])
        gold = {key: tuple(value) for key, value in groups.items()}
        yield batch_id, dict(cumulative, gold=len(gold)), gold, list(accepted)


class FixtureTests(unittest.TestCase):
    def test_scales_references_and_serialization(self):
        for scale, factor in STREAM_REPETITIONS.items():
            with self.subTest(scale=scale):
                config = CourseConfig("main", "test_stream", scale)
                lengths = []
                for batch in STREAM_BATCHES:
                    rows = build_stream_rows(config, batch)
                    self.assertEqual(rows, build_stream_rows(config, batch))
                    self.assertEqual(rows, [json.loads(line) for line in stream_rows_to_jsonl(rows).splitlines()])
                    lengths.append(len(rows))
                    for row in rows:
                        self.assertEqual(row["source_batch_id"], batch)
                        self.assertLess(int(row["product_id"]), config.rows["products"])
                        if not row["event_id"].startswith("bad_customer"):
                            self.assertLess(int(row["customer_id"]), config.rows["customers"])
                self.assertEqual(lengths, [3 * factor, 6 * factor, 2 * factor, 2 * factor, factor])

    def test_duplicate_identity_and_late_event_times(self):
        config = CourseConfig("main", "test_stream", "test")
        first = {r["event_id"]: r for r in build_stream_rows(config, "stream_001")}
        second = {r["event_id"]: r for r in build_stream_rows(config, "stream_002")}
        keys = set(first["e002_000"]) - {"source_batch_id"}
        self.assertEqual({k: first["e002_000"][k] for k in keys}, {k: second["e002_000"][k] for k in keys})
        self.assertEqual(second["late_ok_000"]["event_ts"], "2026-03-12T12:03:00Z")
        fourth = {r["event_id"]: r for r in build_stream_rows(config, "stream_004")}
        self.assertEqual(fourth["late_bad_000"]["amount"], "999")
        self.assertEqual(fourth["late_bad_000"]["event_ts"], "2026-03-12T12:02:00Z")

    def test_stage_outputs_against_independent_temporal_model(self):
        for scale, factor in STREAM_REPETITIONS.items():
            config = CourseConfig("main", "test_stream", scale)
            for batch, counts, gold, accepted in model_stages(config):
                with self.subTest(scale=scale, batch=batch):
                    self.assertEqual(counts, expected_stream_counts(batch, factor))
                    self.assertEqual(gold, expected_gold_rows(batch, factor))
                    ids = {r["event_id"] for r, _, _ in accepted}
                    self.assertNotIn("late_bad_000", ids)
                    if batch >= "stream_002":
                        self.assertIn("late_ok_000", ids)

    def test_final_batch_stream_difference_and_control(self):
        for scale, factor in STREAM_REPETITIONS.items():
            config = CourseConfig("main", "test_stream", scale)
            all_rows = [r for batch in STREAM_BATCHES for r in build_stream_rows(config, batch)]
            by_id = {}
            invalid = Counter()
            for row in all_rows:
                try:
                    amount = Decimal(row["amount"])
                except InvalidOperation:
                    invalid["amount"] += 1
                    continue
                if int(row["customer_id"]) >= config.rows["customers"]:
                    invalid["customer"] += 1
                    continue
                if row["event_type"] == "purchase":
                    by_id.setdefault(row["event_id"], amount)
            self.assertEqual(invalid, {"amount": factor, "customer": factor})
            self.assertEqual(len(by_id), 10 * factor)
            self.assertEqual(sum(by_id.values()), Decimal(2274) * factor)
            final_gold = expected_gold_rows("stream_005", factor)
            self.assertEqual(sum(r[0] for r in final_gold.values()), 9 * factor)
            self.assertEqual(sum(r[1] for r in final_gold.values()), Decimal(1275) * factor)
            control = build_stream_rows(config, "stream_005")
            self.assertTrue(all(r["event_type"] == "control" and r["amount"] == "0" for r in control))

    def test_parameters_and_distinct_checkpoint_paths(self):
        for invalid in ["batch_002", "stream_006", "stream_001/../../", ""]:
            with self.assertRaises(ValueError):
                normalize_stream_batch(invalid)
        config = CourseConfig("main", "test_stream", "test")
        checkpoints = {stream_checkpoint(config, stage) for stage in ["bronze", "quality", "dedup", "gold"]}
        self.assertEqual(len(checkpoints), 4)
        self.assertTrue(all(path.startswith(config.volume_path + "/streaming_lab/checkpoints/") for path in checkpoints))
        with self.assertRaises(ValueError):
            stream_checkpoint(config, "../bronze")


class FakeFile:
    def __init__(self, name):
        self.name = name

    def isDir(self):
        return False


class FakeFS:
    def __init__(self):
        self.files = {}

    def ls(self, directory):
        return [FakeFile(path.rsplit("/", 1)[1]) for path in self.files]

    def put(self, path, content, overwrite):
        if path in self.files and not overwrite:
            raise FileExistsError(path)
        self.files[path] = content


class FakeAudit:
    def __init__(self, stages):
        self.stages = stages

    def where(self, expression):
        stage = expression.split("'")[1]
        return SimpleNamespace(count=lambda: int(stage in self.stages))


class ProducerTests(unittest.TestCase):
    def test_order_no_overwrite_and_prior_validation(self):
        config = CourseConfig("main", "test_stream", "test")
        fs = FakeFS()
        dbutils = SimpleNamespace(fs=fs)
        completed = set()
        spark = SimpleNamespace(table=lambda name: FakeAudit(completed))
        with self.assertRaises(ValueError):
            publish_stream_batch(dbutils, spark, config, "stream_003")
        result = publish_stream_batch(dbutils, spark, config, "stream_001")
        self.assertEqual(result["physical_rows"], 3)
        self.assertEqual(len(fs.files), 1)
        before = dict(fs.files)
        with self.assertRaises(FileExistsError):
            publish_stream_batch(dbutils, spark, config, "stream_001")
        with self.assertRaises(ValueError):
            publish_stream_batch(dbutils, spark, config, "stream_002")
        self.assertEqual(fs.files, before)
        completed.add("stream_001")
        publish_stream_batch(dbutils, spark, config, "stream_002")
        self.assertEqual(len(fs.files), 2)

    def test_unexpected_source_files_stop_publication(self):
        config = CourseConfig("main", "test_stream", "test")
        fs = FakeFS()
        fs.files[stream_input_directory(config) + "/otro.jsonl"] = "{}"
        with self.assertRaises(ValueError):
            publish_stream_batch(SimpleNamespace(fs=fs), SimpleNamespace(), config, "stream_001")
        self.assertEqual(len(fs.files), 1)


class MaterialTests(unittest.TestCase):
    def test_notebook_python_and_run_dependencies(self):
        notebooks = list(LAB.rglob("*.ipynb"))
        self.assertEqual(len(notebooks), 10)
        for path in notebooks:
            nb = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(nb["nbformat"], 4)
            self.assertEqual(len({c["id"] for c in nb["cells"]}), len(nb["cells"]))
            for index, cell in enumerate(nb["cells"], 1):
                if cell["cell_type"] != "code":
                    continue
                source = "".join(cell["source"])
                with self.subTest(notebook=path.name, cell=index):
                    self.assertEqual(cell["outputs"], [])
                    if source.startswith("%run "):
                        self.assertTrue((path.parent / (source.removeprefix("%run ").strip() + ".py")).resolve().is_file())
                    elif source.startswith("%sql\n"):
                        self.assertTrue(source.removeprefix("%sql\n").strip())
                    else:
                        ast.parse(source, filename=f"{path.name}:cell-{index}")
        for name in ["generate_stream_data.py", "streaming_support.py", "streaming_rules.py", "streaming_expectations.py"]:
            ast.parse((LAB.parent / "common" / name).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
