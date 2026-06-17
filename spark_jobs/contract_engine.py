"""
Contract Engine — Apicurio Registry Native Implementation.

Fetches data contracts (schema + quality rules) from Apicurio Registry v3
at job startup via a single REST call per topic. Rules are compiled into
native Spark Column expressions and executed on distributed workers.

No local YAML files. No Python UDFs. No per-record HTTP calls.
"""

import os
import re
import logging
import requests

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import IntegerType, LongType, BooleanType, TimestampType

logger = logging.getLogger(__name__)

APICURIO_URL = os.environ.get("APICURIO_URL", "http://apicurio:8080")

# Iceberg SQL type mapping (used by provision_tables.py)
ICEBERG_TYPE_MAP = {
    "string":    "STRING",
    "int":       "INT",
    "integer":   "INT",
    "long":      "BIGINT",
    "float":     "FLOAT",
    "double":    "DOUBLE",
    "boolean":   "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "bytes":     "BINARY",
}

# Spark StructType mapping (used for JSON deserialization)
SPARK_TYPE_MAP = {
    "string":    StringType(),
    "int":       IntegerType(),
    "integer":   IntegerType(),
    "long":      LongType(),
    "float":     DoubleType(),
    "double":    DoubleType(),
    "boolean":   BooleanType(),
    "timestamp": TimestampType(),
}


class ContractEngine:
    """
    Loads a data contract from Apicurio Registry and compiles its quality
    rules into native Spark expressions for distributed validation.

    Contract structure fetched from Apicurio:
      - schema fields      → used for StructType + Iceberg DDL
      - quality rules      → compiled to F.when() Column expressions
      - metadata           → iceberg_table, quarantine_table, partitioning, format
    """

    def __init__(self, group_id: str, artifact_id: str, contract: dict):
        self._group_id    = group_id
        self._artifact_id = artifact_id
        self._contract    = contract

        # Parse fields: list of {"name": ..., "type": ..., "nullable": ...}
        self._fields = {
            f["name"]: f
            for f in contract.get("schema", {}).get("fields", [])
        }
        # Quality rules: list of {"name": ..., "expr": ..., "kind": ..., "disabled": ...}
        self._rules = [
            r for r in contract.get("quality", {}).get("rules", [])
            if not r.get("disabled", False)
        ]

        logger.info(
            "ContractEngine: %s/%s v%s loaded — %d fields, %d rules",
            group_id, artifact_id, self.version, len(self._fields), len(self._rules)
        )

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def for_topic(cls, topic: str) -> "ContractEngine":
        parts = topic.split(".")
        if len(parts) < 3:
            raise ValueError(f"Topic '{topic}' must follow <layer>.<group>.<artifact> convention")

        group_id = parts[1]
        artifact_id = parts[2]
        base = f"{APICURIO_URL}/apis/registry/v3"

        # Fetch the actual contract content (not metadata)
        resp = requests.get(
            f"{base}/groups/{group_id}/artifacts/{artifact_id}/versions/branch=latest/content",
            headers={"Accept": "application/json"},
            timeout=10,
        )

        if resp.status_code == 404:
            raise FileNotFoundError(
                f"No contract found in Apicurio for group='{group_id}', artifact='{artifact_id}'."
            )

        resp.raise_for_status()
        contract = resp.json()

        logger.info("Loaded contract from Apicurio: %s/%s", group_id, artifact_id)
        return cls(group_id, artifact_id, contract)
    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return self._contract.get("info", {}).get("title", self._artifact_id)

    @property
    def version(self) -> str:
        return self._contract.get("info", {}).get("version", "1.0.0")

    @property
    def format(self) -> str:
        return self._contract.get("format", "avro").lower()

    @property
    def kafka_topic(self) -> str:
        return self._contract.get("kafkaTopic", "")

    @property
    def iceberg_table(self) -> str:
        return self._contract.get("storage", {}).get("icebergTable", "")

    @property
    def quarantine_table(self) -> str:
        return self._contract.get("storage", {}).get("quarantineTable", "")

    @property
    def checkpoint_path(self) -> str:
        if cp := self._contract.get("storage", {}).get("checkpointPath"):
            return cp
        safe = self.kafka_topic.replace(".", "_")
        return f"s3a://warehouse/checkpoints/{safe}"

    @property
    def partitioning(self) -> list:
        return self._contract.get("storage", {}).get("partitioning", [])

    # ── Core Validation ───────────────────────────────────────────────────────

    def build_native_validation_df(self, df, target_schema: StructType = None):
        """
        Deserialize (if needed) and validate records using native Spark expressions.

        Avro mode:
          - df must already contain 'parsed_payload' struct (from from_avro() upstream)
          - df must already contain 'raw_payload' string column

        JSON mode:
          - target_schema is required (built from contract fields)
          - parsing via from_json() is handled here
        """
        if self.format == "json":
            if target_schema is None:
                raise ValueError(
                    "target_schema is required for JSON format. "
                    "Use get_spark_schema_from_contract(engine)."
                )
            df = df.withColumn("raw_payload", F.col("value").cast("string"))
            df = df.withColumn(
                "parsed_payload", F.from_json(F.col("raw_payload"), target_schema)
            )

        validation_exprs = []

        for rule in self._rules:
            expr_str  = rule.get("expr", "").strip()
            rule_name = rule.get("name", "unnamed_rule")
            if not expr_str:
                continue
            spark_expr = self._compile_rule(expr_str, rule_name)
            if spark_expr is not None:
                validation_exprs.append(spark_expr)

        if validation_exprs:
            error_array = F.array_remove(F.array(*validation_exprs), None)
        else:
            error_array = F.array().cast("array<string>")

        return (
            df
            .withColumn("validation_errors", error_array)
            .withColumn("is_valid", F.size(F.col("validation_errors")) == 0)
        )

    def _compile_rule(self, expr: str, rule_name: str):
        """
        Compile a single quality rule expression into a Spark F.when() Column.

        Supported CEL-style patterns:
          field in ['A','B']          → allowed_values
          field > N / field < N         → range check
          field >= N && field <= N      → range check
          has(field)                    → not null / not empty
          field != ''                   → not empty
          field.matches('pattern')      → regex
          field == null                 → null check
        """
        col = lambda f: F.col(f"parsed_payload.{f}")

        # ── allowed_values:  severity in ['INFO','WARNING']
        m = re.match(r"^(\w+)\s+in\s+\[(.+)\]$", expr.strip())
        if m:
            field = m.group(1)
            vals  = [v.strip().strip("\'\"") for v in m.group(2).split(",")]
            return F.when(
                ~col(field).cast("string").isin(vals),
                f"ALLOWED_VALUES_VIOLATION [{rule_name}]: '{field}' not in {vals}"
            ).otherwise(None)

        # ── range:  amount > 0
        m = re.match(r"^(\w+)\s*([><=!]+)\s*([\d.\-]+)$", expr.strip())
        if m:
            field, op, val = m.group(1), m.group(2), float(m.group(3))
            spark_cond = {
                ">":  col(field) <= val,
                ">=": col(field) < val,
                "<":  col(field) >= val,
                "<=": col(field) > val,
                "!=": col(field) == val,
            }.get(op)
            if spark_cond is not None:
                return F.when(
                    spark_cond,
                    f"RANGE_VIOLATION [{rule_name}]: '{field}' failed '{expr}'"
                ).otherwise(None)

        # ── range band:  amount >= 0 && amount <= 5000
        m = re.match(
            r"^(\w+)\s*>=\s*([\d.\-]+)\s*&&\s*\1\s*<=\s*([\d.\-]+)$", expr.strip()
        )
        if m:
            field, lo, hi = m.group(1), float(m.group(2)), float(m.group(3))
            return F.when(
                (col(field) < lo) | (col(field) > hi),
                f"RANGE_VIOLATION [{rule_name}]: '{field}' out of [{lo}, {hi}]"
            ).otherwise(None)

        # ── not_empty / not_null:  has(field)  or  field != ''
        m = re.match(r"^has\((\w+)\)$", expr.strip())
        if m:
            field = m.group(1)
            return F.when(
                col(field).isNull() | (F.trim(col(field).cast("string")) == ""),
                f"NOT_EMPTY_VIOLATION [{rule_name}]: '{field}' is null or empty"
            ).otherwise(None)

        m = re.match(r"^(\w+)\s*!=\s*['\"][\'\"]$", expr.strip())
        if m:
            field = m.group(1)
            return F.when(
                F.trim(col(field).cast("string")) == "",
                f"NOT_EMPTY_VIOLATION [{rule_name}]: '{field}' is empty"
            ).otherwise(None)

        # ── regex:  field.matches('pattern')
        m = re.match(r"^(\w+)\.matches\(['\"](.+)['\"]\)$", expr.strip())
        if m:
            field, pattern = m.group(1), m.group(2)
            return F.when(
                ~col(field).cast("string").rlike(pattern),
                f"REGEX_VIOLATION [{rule_name}]: '{field}' does not match pattern"
            ).otherwise(None)

        # ── not_future (custom extension):  event_time < now()
        if "now()" in expr and "<" in expr:
            m = re.match(r"^(\w+)\s*<\s*now\(\)$", expr.strip())
            if m:
                field = m.group(1)
                return F.when(
                    F.col(f"parsed_payload.{field}").cast("timestamp")
                    > F.current_timestamp(),
                    f"NOT_FUTURE_VIOLATION [{rule_name}]: '{field}' is in the future"
                ).otherwise(None)

        logger.warning("Rule '%s' expression not compiled: '%s'", rule_name, expr)
        return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def get_spark_schema(self) -> StructType:
        """Build a Spark StructType from contract field definitions (JSON mode)."""
        fields = []
        for name, fdef in self._fields.items():
            spark_type = SPARK_TYPE_MAP.get(fdef.get("type", "string").lower(), StringType())
            fields.append(StructField(name, spark_type, fdef.get("nullable", True)))
        return StructType(fields)

    def summary(self) -> dict:
        return {
            "contract":         self.name,
            "version":          self.version,
            "topic":            self.kafka_topic,
            "format":           self.format,
            "fields":           len(self._fields),
            "rules":            len(self._rules),
            "iceberg_table":    self.iceberg_table,
            "quarantine_table": self.quarantine_table,
            "checkpoint":       self.checkpoint_path,
        }
