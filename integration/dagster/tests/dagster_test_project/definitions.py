# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Dagster test project for OpenLineage integration testing.

Assets / Jobs:
  orders_pipeline       — happy path: raw_orders → clean_orders → order_stats
  partial_pipeline      — partial: only raw_orders + clean_orders
  failing_pipeline      — failure: raw_orders succeeds, then clean_orders_fail fails
  op_pipeline           — op-based (non-asset) job: extract_op → transform_op → load_op

All jobs use the openlineage_sensor for event capture.
"""

import os
import random

from dagster import (
    AssetExecutionContext,
    Definitions,
    In,
    MaterializeResult,
    MetadataValue,
    Nothing,
    OpExecutionContext,
    Out,
    TableColumn,
    TableSchema,
    asset,
    define_asset_job,
    graph,
    job,
    op,
)

from openlineage.dagster import openlineage_sensor


# ── Happy-path assets ──────────────────────────────────────────────────────────


@asset(
    description="Raw orders loaded from the source system.",
    metadata={"dagster/uri": MetadataValue.url("file:///data/raw_orders.parquet")},
)
def raw_orders(context: AssetExecutionContext) -> MaterializeResult:
    row_count = random.randint(100, 500)  # noqa: S311
    context.log.info("Loaded %d raw orders", row_count)
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(row_count),
            "dagster/column_schema": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn("order_id", type="int", description="Unique order identifier"),
                        TableColumn("customer_id", type="int", description="Reference to customer"),
                        TableColumn("amount", type="float", description="Order amount in USD"),
                        TableColumn("status", type="string", description="Order status"),
                        TableColumn("created_at", type="timestamp", description="Order creation time"),
                    ]
                )
            ),
        }
    )


@asset(description="Cleaned orders with invalid records filtered out.", deps=[raw_orders])
def clean_orders(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Cleaning orders...")
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(random.randint(80, 400)),  # noqa: S311
            "dagster/column_schema": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn("order_id", type="int", description="Unique order identifier"),
                        TableColumn("customer_id", type="int", description="Reference to customer"),
                        TableColumn("amount", type="float", description="Cleaned order amount"),
                        TableColumn("status", type="string", description="Validated status"),
                    ]
                )
            ),
        }
    )


@asset(description="Aggregated order statistics per customer.", deps=[clean_orders])
def order_stats(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Computing order stats...")
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(random.randint(10, 50)),  # noqa: S311
            "dagster/column_schema": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn("customer_id", type="int", description="Customer identifier"),
                        TableColumn("total_orders", type="int", description="Number of orders"),
                        TableColumn("total_amount", type="float", description="Total spend"),
                    ]
                )
            ),
        }
    )


# ── Failing asset ──────────────────────────────────────────────────────────────

@asset(description="Deliberately fails — used to test FAIL event emission.", deps=[raw_orders])
def clean_orders_fail(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("About to fail intentionally...")
    if os.environ.get("DAGSTER_TEST_FORCE_FAIL", "1") == "1":
        raise ValueError("Intentional failure for OpenLineage FAIL event test")
    return MaterializeResult()


# ── Jobs ───────────────────────────────────────────────────────────────────────

orders_pipeline = define_asset_job(
    name="orders_pipeline",
    selection=[raw_orders, clean_orders, order_stats],
    description="End-to-end orders processing pipeline.",
)

partial_pipeline = define_asset_job(
    name="partial_pipeline",
    selection=[raw_orders, clean_orders],
    description="Partial pipeline: only raw_orders and clean_orders.",
)

failing_pipeline = define_asset_job(
    name="failing_pipeline",
    selection=[raw_orders, clean_orders_fail],
    description="Pipeline that intentionally fails at clean_orders_fail.",
)


# ── Op-based job ───────────────────────────────────────────────────────────────

@op(out=Out(int))
def extract_op(context: OpExecutionContext) -> int:
    count = random.randint(100, 500)  # noqa: S311
    context.log.info("Extracted %d records", count)
    return count


@op(ins={"record_count": In(int)}, out=Out(int))
def transform_op(context: OpExecutionContext, record_count: int) -> int:
    filtered = int(record_count * 0.9)
    context.log.info("Transformed: %d → %d", record_count, filtered)
    return filtered


@op(ins={"record_count": In(int)}, out=Out(Nothing))
def load_op(context: OpExecutionContext, record_count: int) -> None:
    context.log.info("Loaded %d records to destination", record_count)


@job(description="Op-based pipeline (no assets) for testing non-asset lineage.")
def op_pipeline():
    load_op(transform_op(extract_op()))


# ── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=[raw_orders, clean_orders, order_stats, clean_orders_fail],
    jobs=[orders_pipeline, partial_pipeline, failing_pipeline, op_pipeline],
    sensors=openlineage_sensor(),
)
