"""Migration and runtime checks for exact tenant ownership on the shared ledger."""

from shepherd_contracts.errors import ConfigurationError
from shepherd_workflow.ports import owned_tables
from shepherd_workflow.sql.schema import TENANT_COLLATION, tenant_id_column_type
from sqlalchemy import LargeBinary, MetaData, Table, cast, inspect, select
from sqlalchemy.engine import Connection

from shepherd_workflow_service.models.tenancy import validate_tenant_id

__all__ = ["check_tenant_collations", "migrate_tenant_ownership", "tenant_id_column_type"]

_STAGE_KEYS = ("stage_runs", "admission_estimates", "admission_cost_policy")


def _ownership_tables(connection: Connection, prefix: str) -> list[str]:
    optional = (
        "execution_bindings", "outbox_delivery", "admission_policies", "admission_tenants",
        "admission_backlog", "admission_ready", "admission_attempts", "admission_estimates",
    )
    present = set(inspect(connection).get_table_names())
    return sorted(owned_tables(prefix) | {prefix + name for name in optional} & present)


def check_tenant_collations(connection: Connection, prefix: str) -> None:
    if connection.dialect.name != "mysql":
        return
    inspector = inspect(connection)
    for name, fields in _exact_columns(connection, prefix).items():
        columns = {column["name"]: column["type"] for column in inspector.get_columns(name)}
        for field in fields:
            if getattr(columns.get(field), "collation", None) != TENANT_COLLATION:
                raise ConfigurationError(
                    "Run the tenant ownership migration before using this ledger"
                )


def _exact_columns(connection: Connection, prefix: str) -> dict[str, dict[str, int]]:
    columns = {name: {"org_id": 64} for name in _ownership_tables(connection, prefix)}
    present = set(inspect(connection).get_table_names())
    for suffix in _STAGE_KEYS:
        if prefix + suffix in present:
            columns.setdefault(prefix + suffix, {})["stage_key"] = 128
    return columns


def migrate_tenant_ownership(connection: Connection, prefix: str) -> None:
    """Audit all owners and links before changing MySQL columns. Run with writers stopped."""
    metadata = MetaData()
    tables = {
        name.removeprefix(prefix): Table(name, metadata, autoload_with=connection)
        for name in _ownership_tables(connection, prefix)
    }
    for table in tables.values():
        for raw in connection.scalars(select(cast(table.c.org_id, LargeBinary)).distinct()):
            try:
                validate_tenant_id(raw.decode("ascii"))
            except (AttributeError, TypeError, ValueError) as exc:
                raise ConfigurationError(
                    f"Noncanonical tenant ownership in {table.name}; migration stopped"
                ) from exc

    for child_name, parent_name, key in (
        ("stage_runs", "jobs", "job_id"),
        ("execution_bindings", "jobs", "job_id"),
        ("attempts", "stage_runs", "stage_run_id"),
        ("events", "stage_runs", "stage_run_id"),
    ):
        if child_name not in tables:
            continue
        child, parent = tables[child_name], tables[parent_name]
        join = child.outerjoin(parent, cast(child.c[key], LargeBinary) == cast(
            parent.c[key], LargeBinary
        ))
        inconsistent = connection.execute(select(child.c.org_id).select_from(join).where(
            parent.c[key].is_(None)
            | (cast(child.c.org_id, LargeBinary) != cast(parent.c.org_id, LargeBinary))
        ).limit(1)).first()
        if inconsistent is not None:
            raise ConfigurationError(f"Invalid ownership link in {child.name}; migration stopped")

    if connection.dialect.name == "mysql":
        quote = connection.dialect.identifier_preparer.quote
        inspector = inspect(connection)
        for name, fields in _exact_columns(connection, prefix).items():
            types = {column["name"]: column["type"] for column in inspector.get_columns(name)}
            for field, length in fields.items():
                if types[field].collation != TENANT_COLLATION:
                    connection.exec_driver_sql(
                        f"ALTER TABLE {quote(name)} MODIFY COLUMN {quote(field)} VARCHAR({length})"
                        f" CHARACTER SET utf8mb4 COLLATE {TENANT_COLLATION} NOT NULL"
                    )
    check_tenant_collations(connection, prefix)
