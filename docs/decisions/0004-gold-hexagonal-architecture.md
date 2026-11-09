# ADR-004: Hexagonal Architecture and Independent Assets for Gold Layer

## Status
Accepted

## Date
2026-11-08

## Context
Prior to this decision, the Gold analytical layer was packaged as a monolithic "black box" Dagster asset (`gold_v2_active_release`), driven by procedural scripts. This created significant operational hurdles:
- Inability to inspect, monitor, or retry individual fact calculations in Dagster.
- A failure in one metric halted all other fact calculations.
- Tight coupling between DuckDB SQL generation, schema definitions, and Iceberg storage adapters.

## Decision
Adopt Hexagonal Architecture (Ports and Adapters) for `src/procurement/gold/`:
1. **Domain (`gold.domain`)**:
   - Pure models (`Table`, `Column`, `DIMENSIONS`, `FACTS`, `FOREIGN_KEYS`, `RunPlan`, `SilverReadyInput`), grains, policies, and schemas without database/network I/O.
   - Graph & Clustering domain algorithms (`domain/clustering/`: `union_find.py`, `planning.py`, `uf_graph.py`, `relationship_index.py`, `full_uf_planner.py`).
2. **Ports (`gold.ports`)**:
   - Abstract Protocols for inbound (`process_gold.py`, `query_gold.py`) and outbound (`gold_storage.py`, `silver_reader.py`, `state_store.py`).
3. **Application (`gold.application`)**:
   - Service orchestration (`processor.py`, `gold_service.py`, `warehouse.py`, `reconciliation.py`, `runtime.py`, `state.py`, `aggregation.py`, `calculation.py`).
   - Dedicated analytical compute engine (`application/compute/`) with DuckDB transformation SQL templates.
4. **Adapters (`gold.adapters`)**:
   - Driven adapters for Apache Iceberg Lakehouse (`adapters/driven/iceberg/`) and Silver lakehouse reader (`adapters/driven/silver/`).
   - Driving adapter (`adapters/driving/cli.py`) consolidating all CLI commands (`schema`, `version`, `plan`, `resume`, `promote`, etc.).
5. **Legacy Elimination**:
   - Completely purged the temporary `src/procurement/gold/v2/` directory and consolidated all code and SQL into their respective hexagonal layers.
   - Renamed `tests/gold_v2/` to `tests/gold/`.
6. **Independent Dagster Assets**:
   - Decompose the Gold layer into 9 distinct dimension assets (`gold_dimensions`) and 7 distinct daily fact assets (`gold_facts`).

## Consequences
- **Observability**: Each fact table has its own execution logs, run times, row count metadata, and automated asset checks.
- **Testability**: Domain schemas and transformation logic are independently testable in memory without running an Iceberg cluster.
- **Maintainability**: Clean architectural boundaries prevent accidental leakage of infrastructure dependencies into business rules.
