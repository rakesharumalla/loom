"""Run Databricks SQL (UC DDL/grants, Delta reads) through the Statement Execution API."""
from __future__ import annotations

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def warehouse_id(w: WorkspaceClient) -> str:
    whs = list(w.warehouses.list())
    if not whs:
        raise RuntimeError("no SQL warehouse available")
    serverless = [x for x in whs if x.enable_serverless_compute]
    return (serverless or whs)[0].id


def run_sql(w: WorkspaceClient, statement: str, wh: str | None = None) -> list[list]:
    wh = wh or warehouse_id(w)
    r = w.statement_execution.execute_statement(warehouse_id=wh, statement=statement, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{statement[:80]}... -> {r.status.state}: {r.status.error.message if r.status.error else ''}")
    return (r.result.data_array or []) if r.result else []
