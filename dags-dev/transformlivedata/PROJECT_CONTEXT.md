# transformlivedata: Project Context & Architecture Principles

**Last Updated:** March 4, 2026  
**Status:** Architecture Locked  
**Reference:** Keep this file open when planning or updating the migration plan

---

## Core Rule: Phase Implementation Order

The implementation MUST follow this strict order:

```
0. CREATE v7.py SKELETON (Phase 1)
   ↓
1. RAW VALIDATION (Phases 2-3)
   ↓
2. TRANSFORMED VALIDATION + DATA DOCS (Phases 4-7)
   ↓
3. LINEAGE (Phases 8-9)
   ↓
4. FINAL INTEGRATION (Phase N/A - Deliverables 1-3 complete)
```

**Do NOT deviate from this order.** Start by copying v6 → v7, then incrementally add and integrate each validated component while building v7.

---

## Architecture: 3-File, 3-Class, 3-Layer Approach

### Configuration Files (JSON-Driven)

| File | Purpose | Validation Approach | Used By |
|------|---------|-------------------|---------|
| **raw_expectations.json** | Raw API response schema | JSON Schema (jsonschema library) | RawDataExpectations |
| **expectations.json** | Transformed DataFrame rules | Great Expectations (GX) | TransformedDataExpectations |
| **lineage.json** | Column metadata + sources | Custom JSON format | LineageReportGenerator |

### Classes (Single Responsibility)

| Class | Responsibility | Phase | Dependencies |
|-------|-----------------|-------|--------------|
| **RawDataExpectations** | Validate raw API dicts against JSON Schema | 2 | raw_expectations.json + jsonschema |
| **TransformedDataExpectations** | Validate transformed DataFrame via GX | 3 | expectations.json + GX (independent) |
| **LineageReportGenerator** | Generate HTML lineage reports | 6 | lineage.json (ZERO dependency on validation classes) |

### Validation Layers (14 Total Rules)

```
RAW LAYER (Pre-transformation)
├─ 3 jsonschema validations from raw_expectations.json
│  ├─ raw_positions_structure: {'metadata', 'payload', 'payload.l'} keys
│  ├─ raw_field_types: extracted_at, payload.hr, vehicle fields
│  └─ raw_vehicle_fields: vehicles have {p, c, py, px, ta}
│
TRANSFORMED LAYER (Post-transformation)
├─ 11 GX expectations from expectations.json
│  ├─ Column existence (1)
│  ├─ Column types (5)
│  ├─ Nullability (1)
│  ├─ Ranges (6)
│  └─ Coordinate bounds (2)
│
LINEAGE LAYER (Metadata only)
└─ No validation, only documentation
   └─ 20 columns with input sources + transformations
```

---

## Key Architectural Principles (LOCKED)

1. **RAW FIRST:** Always implement RawDataExpectations before TransformedDataExpectations
2. **JSON-DRIVEN:** All validation rules live in config files, not hardcoded
3. **jsonschema for raw:** Use `jsonschema` library (not Pydantic, not hardcoding)
4. **GX for transformed:** Use GX checkpoint for DataFrame validation
5. **Complete separation:** Lineage is 100% independent, zero coupling to validation classes
6. **No orchestration:** Do NOT create a wrapper class combining validation + lineage
7. **No Composition pattern:** TransformedDataExpectations and RawDataExpectations are independent classes that will be instantiated via the transform script.
8. **Configuration first:** Before coding any class method, ensure the JSON file is complete
9. **NEW FILES ONLY:** Each new implementation class goes in its own dedicated file. Do NOT add implementations to existing files.
10. **SINGLE requirements.txt:** All dependencies go in `/dags-dev/requirements.txt` (NOT in subfolders like transformlivedata/). This mirrors Airflow's architecture where subfolders contain source code only. When adding a dependency: (a) add to `/dags-dev/requirements.txt`, (b) run `pip install -r requirements.txt` immediately to ensure it's installed.

DELIVERABLE 3: LINEAGE ← Integrate into v7
├─ Phase 7: Create LineageReportGenerator class (quality/lineage_report.py)
└─ Phase 8: Integrate LineageReportGenerator into v7.py + test

FINAL INTEGRATION & DOCUMENTATION
├─ Phase 9: Full Integration Testing (v7.py complete)
└─ Phase 10: Documentation
```

---

## Project Structure & Dependencies (Airflow Mirroring)

### Single requirements.txt Architecture

The pipeline structure mirrors **Airflow's pattern:**
- **Subfolders** (like `transformlivedata/`, `refinedfinishedtrips/`, etc.) contain **source code only**
- These folders are imported as Python packages into DAG scripts
- **All dependencies** go in a **single requirements.txt** at `/dags-dev/requirements.txt`
- Subfolders like `transformlivedata/` should **NOT have their own requirements.txt**
- This ensures a unified dependency environment for Airflow deployment

### Correct Dependency Workflow

❌ **WRONG:** Add dependencies to `transformlivedata/requirements.txt` or other subfolder requirements.txt  
✅ **CORRECT:** Add dependencies to `/dags-dev/requirements.txt` (project root)

When adding a new dependency:
1. Edit `/dags-dev/requirements.txt` (add line: `package==version`)
2. Run `pip install -r requirements.txt` immediately to activate
3. Document addition in migration plan completion notes

**Example:** Phase 2 required `jsonschema==4.20.0`:
- ✅ Added to `/dags-dev/requirements.txt`
- ✅ Ran `pip install jsonschema==4.20.0` to ensure installation
- ✅ Updated migration plan and PROJECT_CONTEXT.md

---

## Implementation Phases (Sequential, Not Parallel)

**v7.py Skeleton First, Then Incremental Integration**

```
PHASE 1: v7.py SKELETON
└─ Phase 1: Copy transformlivedata-v6.py → transformlivedata-v7.py (no changes yet)

DELIVERABLE 1: RAW VALIDATION ← Integrate into v7
├─ Phase 2: Create RawDataExpectations class (quality/raw_data_expectations.py)
└─ Phase 3: Integrate RawDataExpectations into v7.py + test

DELIVERABLE 2: TRANSFORMED VALIDATION + GX DATA DOCS ← Integrate into v7
├─ Phase 4: Set Up GX Infrastructure (BEFORE creating class)
├─ Phase 5: Create TransformedDataExpectations class (quality/transformed_data_expectations.py)
├─ Phase 6: Integrate TransformedDataExpectations into v7.py + test
└─ Phase 7: Create GXDataDocsGenerator class (quality/gx_data_docs.py) + integrate (docs triggered on every run)

DELIVERABLE 3: LINEAGE ← Integrate into v7
├─ Phase 8: Create LineageReportGenerator class (quality/lineage_report.py)
└─ Phase 9: Integrate LineageReportGenerator into v7.py + test
```

---

## Implementation Pattern: Copy & Integrate

1. **Phase 1:** Copy v6 → v7 (exact copy)
2. **Phase 2-3:** Create RawDataExpectations → Integrate into v7
3. **Phase 4-6:** Set Up GX → Create TransformedDataExpectations → Integrate into v7
4. **Phase 7-8:** Create LineageReportGenerator → Integrate into v7
5. **Phase 9-10:** Full integration testing + documentation

**Key principle:** Minimal changes to v7. Only add imports and validation calls at 3 integration points. Keep transformation logic unchanged.

## Phase Details

See [MIGRATION_PLAN_GE_JSON_DRIVEN.md](MIGRATION_PLAN_GE_JSON_DRIVEN.md) for detailed implementation steps for each phase.

---

## Class Structure (Reference)

| Class | Purpose | Config File | Phase |
|-------|---------|-------------|-------|
| **RawDataExpectations** | Validates raw API dicts (jsonschema-driven) | raw_expectations.json | 2 |
| **TransformedDataExpectations** | Validates transformed DataFrames (GX-driven) | expectations.json | 5 |
| **LineageReportGenerator** | Generates HTML lineage reports | lineage.json | 7 |

See [MIGRATION_PLAN_GE_JSON_DRIVEN.md](MIGRATION_PLAN_GE_JSON_DRIVEN.md) for class method signatures and implementations.

---

## Critical Decisions (Do Not Change)

- ❌ Do NOT use Pydantic models for raw validation (use jsonschema)
- ❌ Do NOT hardcode validation rules (extract to JSON files)
- ❌ Do NOT create a combined ValidatorOrchestrator class
- ❌ Do NOT implement lineage before expectations
- ❌ Do NOT import LineageReportGenerator in validation classes
- ❌ Do NOT compose RawDataExpectations inside TransformedDataExpectations
- ✅ DO use jsonschema library for raw_expectations.json
- ✅ DO use GX Checkpoint for expectations.json
- ✅ DO keep lineage completely independent
- ✅ DO implement phases in strict order
- ✅ DO instantiate both validation classes separately in transform script

---

## File Locations

```
transformlivedata/
├── config/
│   ├── raw_expectations.json       # JSON Schema (3 rules)
│   ├── expectations.json           # GX ExpectationSuite (11 rules)
│   └── lineage.json                # Column metadata (20 columns)
├── quality/
│   ├── ge_expectations.py          # RawDataExpectations + TransformedDataExpectations
│   ├── lineage_report.py           # LineageReportGenerator
│   └── __init__.py
├── transformlivedata-v7.py         # ⭐ IMPLEMENTATION (creates v7 with full validation integration)
├── PROJECT_CONTEXT.md              # This file (reference always)
├── MIGRATION_PLAN_GE_JSON_DRIVEN.md # Implementation plan (read for details)
└── README.md                       # Usage documentation (update in Phase 8)
```

---

## Implementation Output

**Final Deliverable:** `transformlivedata-v7.py`

This file will integrate all validation classes with minimal changes:
- Import 3 validator/lineage classes
- Create instances at startup
- Call validators at 3 integration points (raw input, transformed output, pipeline end)
- Keep transformation logic unchanged from v6

See [MIGRATION_PLAN_GE_JSON_DRIVEN.md](MIGRATION_PLAN_GE_JSON_DRIVEN.md) for integration code examples.

---

## How to Use This File

When planning or updating:

1. **Before modifying the migration plan:** Read this file to understand the locking principles
2. **When starting a new phase:** Check the "Implementation Phases" section
3. **When designing classes:** Refer to "Class Signatures"
4. **When unsure about architecture:** Check "Key Architectural Principles"
5. **When uncertain about dependencies:** Check "Critical Decisions"

**Rule:** If anything in your plan contradicts this file, this file wins. Update the migration plan, not this file.

---

**Status:** LOCKED - Do not change without explicit agreement to change the entire architecture.
