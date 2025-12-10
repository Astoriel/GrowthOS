"""Schema validators for canonical marketing tables."""

from __future__ import annotations

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.domain.contracts import CONTRACT_SPECS, TableContract
from growth_os.domain.models import ValidationIssue, ValidationResult
from growth_os.ingestion.catalog import discover_tables
from growth_os.ingestion.freshness import compute_freshness


def validate_contract(
    connector: GrowthConnector,
    contract: TableContract,
    present_columns: set[str],
) -> list[ValidationIssue]:
    """Validate a single table against its contract.

    Checks required columns (with alias fallback) and returns any issues found.
    """
    issues: list[ValidationIssue] = []

    for required in contract.required_columns:
        if required in present_columns:
            continue
        aliases = contract.aliases.get(required, ())
        if any(alias in present_columns for alias in aliases):
            issues.append(
                ValidationIssue(
                    table_name=contract.name,
                    severity="warning",
                    message=f"Missing canonical column `{required}` but alias detected.",
                )
            )
        else:
            issues.append(
                ValidationIssue(
                    table_name=contract.name,
                    severity="error",
                    message=f"Missing required column `{required}`.",
                )
            )

    return issues


def validate_all_contracts(
    connector: GrowthConnector,
    contract_specs: dict | None = None,
) -> ValidationResult:
    """Validate all canonical marketing tables against their contracts.

    Uses CONTRACT_SPECS by default.
    """
    specs = contract_specs if contract_specs is not None else CONTRACT_SPECS
    result = ValidationResult()
    tables = {table.name: table for table in discover_tables(connector)}

    for contract_name, contract in specs.items():
        table = tables.get(contract_name)
        if table is None:
            result.issues.append(
                ValidationIssue(
                    table_name=contract_name,
                    severity="error",
                    message="Missing canonical table.",
                )
            )
            continue

        present_columns = {col.name for col in table.columns}
        result.issues.extend(validate_contract(connector, contract, present_columns))

    result.freshness = compute_freshness(connector)
    return result
