"""
Generic Data Contract Engine (Optimized for Native Spark Execution).
"""

import os
import glob
import yaml
import logging
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

class ContractEngine:

    def __init__(self, contract_path: str):
        with open(contract_path, "r") as f:
            raw = yaml.safe_load(f)
        self._c      = raw["contract"]
        self._fields = {fld["name"]: fld for fld in self._c.get("fields", [])}
        logger.info("ContractEngine: %s v%s loaded (%d fields)",
                    self.name, self.version, len(self._fields))

    @classmethod
    def for_topic(cls, topic: str, contracts_dir: str) -> "ContractEngine":
        folder_hint = topic.replace("-", "_")
        pattern     = os.path.join(contracts_dir, "**", "*.yaml")
        candidates  = []

        for path in glob.glob(pattern, recursive=True):
            try:
                with open(path) as f:
                    data = yaml.safe_load(f)
                c = data.get("contract", {})
                if (c.get("kafka_topic") == topic
                        and c.get("status", "active") == "active"):
                    candidates.append((c.get("version", 0), path))
            except Exception:
                continue

        if not candidates:
            raise FileNotFoundError(
                f"No active contract found for topic '{topic}' under {contracts_dir}"
            )

        candidates.sort(key=lambda x: x[0], reverse=True)
        return cls(candidates[0][1])

    @property
    def name(self):               return self._c["name"]
    @property
    def version(self):            return self._c.get("version", 0)
    @property
    def iceberg_table(self):      return self._c["iceberg_table"]
    @property
    def quarantine_table(self):   return self._c["quarantine_table"]
    @property
    def kafka_topic(self):        return self._c["kafka_topic"]
    @property
    def format(self):             return self._c.get("format", "json").lower()
    @property
    def checkpoint_path(self):
        return self._c.get("checkpoint_path", f"s3a://warehouse/checkpoints/{self.name}")
    @property
    def partitioning(self):
        return self._c.get("partitioning", [])
    def build_native_validation_df(self, df):
        """
        Compile YAML contract rules into native Spark expressions.
        No Python row-by-row processing.
        """

        validation_exprs = []

        for field_name, field_def in self._fields.items():

            nullable = field_def.get("nullable", True)
            col_ref = F.col(f"parsed_payload.{field_name}")

            # ------------------------------------------------------------------
            # Required field validation
            # ------------------------------------------------------------------

            if not nullable:
                validation_exprs.append(
                    F.when(
                        col_ref.isNull(),
                        f"NULL_VIOLATION: '{field_name}' is required but missing"
                    ).otherwise(None)
                )

            # ------------------------------------------------------------------
            # Contract checks
            # ------------------------------------------------------------------

            for check in field_def.get("checks", []):

                ctype = check.get("type")

                # --------------------------------------------------------------
                # NOT EMPTY
                # --------------------------------------------------------------

                if ctype == "not_empty":

                    validation_exprs.append(
                        F.when(
                            F.trim(col_ref.cast("string")) == "",
                            f"NOT_EMPTY_VIOLATION: '{field_name}' is empty"
                        ).otherwise(None)
                    )

                # --------------------------------------------------------------
                # RANGE
                # --------------------------------------------------------------

                elif ctype == "range":

                    lo = float(check["min"])
                    hi = float(check["max"])

                    validation_exprs.append(
                        F.when(
                            (col_ref < lo) | (col_ref > hi),
                            f"RANGE_VIOLATION: '{field_name}' out of [{lo}, {hi}]"
                        ).otherwise(None)
                    )

                # --------------------------------------------------------------
                # RANGE BY FIELD
                # --------------------------------------------------------------

                elif ctype == "range_by_field":

                    reference_field = check["field"]

                    for reference_value, bounds in check["ranges"].items():

                        lo, hi = bounds

                        validation_exprs.append(
                            F.when(
                                (
                                    F.col(
                                        f"parsed_payload.{reference_field}"
                                    ) == reference_value
                                )
                                &
                                (
                                    (col_ref < lo)
                                    |
                                    (col_ref > hi)
                                ),
                                f"RANGE_BY_FIELD_VIOLATION: '{field_name}' invalid for {reference_value}"
                            ).otherwise(None)
                        )

                # --------------------------------------------------------------
                # ALLOWED VALUES
                # --------------------------------------------------------------

                elif ctype == "allowed_values":

                    vals = [str(v) for v in check.get("values", [])]

                    validation_exprs.append(
                        F.when(
                            ~col_ref.cast("string").isin(vals),
                            f"ALLOWED_VALUES_VIOLATION: '{field_name}' invalid"
                        ).otherwise(None)
                    )

                # --------------------------------------------------------------
                # REGEX
                # --------------------------------------------------------------

                elif ctype == "regex":

                    validation_exprs.append(
                        F.when(
                            ~col_ref.cast("string").rlike(check["pattern"]),
                            f"REGEX_VIOLATION: '{field_name}' bad format"
                        ).otherwise(None)
                    )

                # --------------------------------------------------------------
                # NOT FUTURE
                # --------------------------------------------------------------

                elif ctype == "not_future":

                    tolerance_seconds = int(
                        check.get("tolerance_seconds", 0)
                    )

                    validation_exprs.append(
                        F.when(
                            col_ref.cast("timestamp")
                            >
                            (
                                F.current_timestamp()
                                +
                                F.expr(
                                    f"INTERVAL {tolerance_seconds} SECONDS"
                                )
                            ),
                            f"NOT_FUTURE_VIOLATION: '{field_name}' is in the future"
                        ).otherwise(None)
                    )

        # ----------------------------------------------------------------------
        # Final validation array
        # ----------------------------------------------------------------------

        if validation_exprs:

            error_array = F.array_remove(
                F.array(*validation_exprs),
                None
            )

        else:

            error_array = F.array().cast("array<string>")

        return (
            df
            .withColumn("validation_errors", error_array)
            .withColumn(
                "is_valid",
                F.size(F.col("validation_errors")) == 0
            )
        )

    def summary(self) -> dict:
        return {
            "contract":         self.name,
            "version":          self.version,
            "topic":            self.kafka_topic,
            "format":           self.format,
            "fields":           len(self._fields),
            "iceberg_table":    self.iceberg_table,
            "quarantine_table": self.quarantine_table,
            "checkpoint":       self.checkpoint_path,
        }