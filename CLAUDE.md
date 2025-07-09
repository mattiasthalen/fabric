# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Environment Setup

This project uses Python 3.11+ with dependencies managed via requirements.txt. To set up the development environment:

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (required for Fabric connections)
source set_env_vars.sh
```

## Common Development Commands

### Running DLT Pipelines
```bash
# Run a specific DLT pipeline (e.g., northwind)
python dlt/northwind/northwind.py
```

### SQLMesh Operations
```bash
# Plan and run SQLMesh transformations
sqlmesh plan

# Run specific environment
sqlmesh plan dev

# Format SQL code
sqlmesh format

# Run tests
sqlmesh test
```

### Working with Notebooks
Execute the Jupyter notebooks in the `fabric/` directory:
- `Retrieve Codebase.Notebook/` - Codebase analysis
- `SQLMesh Runner.Notebook/` - SQLMesh execution
- `dlt Runner.Notebook/` - DLT pipeline execution

## Architecture Overview

This is a three-layer data warehouse built on Microsoft Fabric using the Hook modeling methodology:

### Layer 1: DAS (Data Analytical Storage) - Bronze/Raw
- **Purpose**: Raw data ingestion from source systems
- **Location**: `sqlmesh/models/das/`
- **Key Components**:
  - `raw_blueprint.py` - Dynamic raw model generation from DLT schemas
  - `scd_blueprint.sql` - Slowly Changing Dimensions (Type 2) implementation
- **Features**: Hash-based deduplication, timestamp conversion, anti-join patterns

### Layer 2: DAB (Data Analytical Business) - Silver/Business
- **Purpose**: Business-oriented data modeling using Hook methodology
- **Location**: `sqlmesh/models/dab/hook/`
- **Configuration Files**:
  - `concepts.yaml` - Core business concepts (customer, product, order, etc.)
  - `keysets.yaml` - Source system to concept mappings
  - `frames.yaml` - Table to business concept mappings with hooks
- **Key Component**: `hook_blueprint.py` - Generates frame models from configuration

### Layer 3: DAR (Data Analytical Requirements) - Gold/Consumption
- **Purpose**: Unified consumption layer using USS (Universal Semantic Schema)
- **Location**: `sqlmesh/models/dar/uss/`
- **Key Components**:
  - `bridge_blueprint.py` - Creates relationships between frames
  - `bridge__as_of.py` - Unified view of all bridge tables
  - `_bridge__as_is.sql` - Current records only view

## Hook Modeling Methodology

### Core Concepts
- **Hooks**: Standardized relationship identifiers (e.g., `_hook__category__id`)
- **Primary Hooks**: Main business key for each frame
- **Composite Hooks**: Multi-field business keys
- **PIT Hooks**: Point-in-time relationships for temporal queries
- **Keyset Prefixes**: Namespace relationships by source system

### Working with Hook Configuration

When adding new source systems or tables:

1. **Define Concepts** (`concepts.yaml`):
   ```yaml
   - name: new_concept
     type: core
   ```

2. **Define Keysets** (`keysets.yaml`):
   ```yaml
   - name: source_system.concept.qualifier
     concept: concept_name
     qualifier: id
     source_system: source_system_name
   ```

3. **Define Frames** (`frames.yaml`):
   ```yaml
   - name: source_system__table_name
     hooks:
     - name: _hook__concept__qualifier
       primary: true
       concept: concept_name
       qualifier: qualifier
       keyset: source_system.concept.qualifier
       expression: source_column_name
   ```

## Technical Implementation Details

### SQLMesh Configuration
- **Project**: adventure-works
- **Default Gateway**: fabric (Microsoft Fabric)
- **State Connection**: SQL Database for SQLMesh state management
- **Default Dialect**: duckdb with case-sensitive normalization
- **Environment**: Dynamic based on git branch (`dev__{branch}`)

### Data Flow Pattern
1. **Landing Zone** → **DAS Raw** (deduplication, typing)
2. **DAS Raw** → **DAS SCD** (slowly changing dimensions)
3. **DAS SCD** → **DAB Hook Frames** (business concept mapping)
4. **DAB Hook Frames** → **DAR USS Bridge** (relationship resolution)
5. **DAR USS Bridge** → **DAR USS Views** (consumption layer)

### Key Patterns
- **Hash-based Change Detection**: SHA256 hashes for deduplication
- **Temporal Modeling**: Validity periods with `_record__valid_from/to`
- **Relationship Resolution**: Hook-based joins with temporal awareness
- **Schema Evolution**: Dynamic column management across versions

## Important Development Notes

- All models use incremental processing patterns
- Temporal relationships are handled through PIT (Point-in-Time) hooks
- The `STAR__LIST` macro is used for dynamic column management
- Environment variables are required for Fabric authentication
- Git branch determines the SQLMesh environment name

## File Structure
```
├── dlt/                    # DLT pipelines for data ingestion
├── fabric/                 # Fabric notebooks and warehouse projects
├── sqlmesh/               # SQLMesh project
│   ├── models/das/        # Raw data models
│   ├── models/dab/hook/   # Business models with Hook methodology
│   ├── models/dar/uss/    # Consumption models with USS
│   ├── macros/            # Custom SQLMesh macros
│   └── config.py          # SQLMesh configuration
├── pyproject.toml         # Project dependencies
└── requirements.txt       # Compiled dependencies
```