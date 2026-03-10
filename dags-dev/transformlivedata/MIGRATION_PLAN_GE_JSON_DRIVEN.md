# Migration Plan: JSON-Driven Validation (2-File Architecture)

⚠️ **READ FIRST:** [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) — Architecture principles and phase ordering locked

**Date Created:** March 4, 2026  
**Last Updated:** March 4, 2026 at 14:45  
**Status:** Ready for Implementation  
**Scope:** transformlivedata pipeline, local dev environment only

---

## Architecture Decision: 2-File Approach (Separated Concerns)

### Previous Attempt
Combined validation + lineage into one file, which mixed standard GX ExpectationSuite format with custom sections.

**Why this failed:** GX expects standard ExpectationSuite JSON format. Mixing in custom `lineage` sections is non-standard and confusing.

### Solution: 2 Separate Files (Each with Clear Purpose)

| File | Purpose | Owner | Used by | Format |
|------|---------|-------|---------|--------|
| `expectations.json` | GX validation rules (standard format) | Data Engineer | GX Checkpoint | GX ExpectationSuite |
| `lineage.json` | Column lineage metadata | Data Engineer | LineageReportGenerator | Custom JSON |

**Key insight:** Clean separation of concerns. Each file is independent and serves one purpose. No mixing of standards.

---

## Configuration Files

### 1. `raw_expectations.json` (JSON Schema) ✅ CREATED
**What it is:** JSON schema for raw API response validation  
**Where it lives:** [transformlivedata/config/raw_expectations.json](config/raw_expectations.json)  
**Who edits it:** Data Engineer  
**Format:** JSON Schema (standard JSON Schema format)

**Key features:**
- Validates raw API response structure (metadata, payload, payload.l)
- Validates field types (extracted_at: str, payload.hr: int|array|string, etc.)
- Validates vehicle records have required fields (p, c, py, px, ta)
- Used by `RawDataExpectations` class via jsonschema validation

---

### 2. `expectations.json` (Pure GX Format, Transformed Only) ✅ CREATED
**What it is:** Great Expectations ExpectationSuite in standard JSON format (transformed data only)  
**Where it lives:** [transformlivedata/config/expectations.json](config/expectations.json)  
**Who edits it:** Data Engineer  
**Format:** GX ExpectationSuite (standard)

**Key features:**
- 11 GX native expectations for post-transformation validation
- Column existence and types
- Not-null constraints, value ranges, coordinate bounds
- Used by `TransformedDataExpectations` class via GX Checkpoint

**Total validation mapping (14 total):**
- **Raw Layer (3 jsonschema validations - pre-transformation):** via [raw_expectations.json](config/raw_expectations.json)
- **Transformed Layer (11 GX validations - post-transformation):** via [expectations.json](config/expectations.json)

---

### 3. `lineage.json` (Column Lineage Only) ✅ CREATED
**What it is:** Column-level lineage metadata for HTML report generation  
**Where it lives:** [transformlivedata/config/lineage.json](config/lineage.json)  
**Who edits it:** Data Engineer  
**Format:** Custom JSON with job metadata and 20 columns

---

## Phase Structure: 9 Total Phases (3 Sequential Deliverables)

```
DELIVERABLE 1: RAW VALIDATION (Phases 1-3) ✅ COMPLETE
├─ Phase 1: v7.py skeleton
├─ Phase 2: RawDataExpectations class
└─ Phase 3: Integrate raw validation + test

DELIVERABLE 2: TRANSFORMED VALIDATION + DATA DOCS (Phases 4-7)
├─ Phase 4: GX Infrastructure setup
├─ Phase 5: TransformedDataExpectations class
├─ Phase 6: Integrate GX validation + test
└─ Phase 7: GX Data Docs generation (triggered on every run)

DELIVERABLE 3: LINEAGE (Phases 8-9)
├─ Phase 8: Create LineageReportGenerator class
└─ Phase 9: Integrate LineageReportGenerator + test
```

---

## Implementation: 9 Phases (v7 Skeleton First, Then Incremental Integration)

### ⚠️ DEPENDENCY MANAGEMENT RULE

**All dependencies go in `/dags-dev/requirements.txt` (root level).**

When a phase requires new dependencies:
1. Add to `/dags-dev/requirements.txt` (NOT to subfolder requirements.txt)
2. Run `pip install -r requirements.txt` immediately to activate
3. Document the addition in that phase's completion notes

**Examples from this migration:**
- **Phase 2 (RawDataExpectations)** required `jsonschema==4.20.0`
  - ✅ Added to `/dags-dev/requirements.txt`
  - ✅ Installed: `pip install jsonschema==4.20.0`
  - ✅ Verified in test environment

**Why:** Mirrors Airflow architecture where `transformlivedata/` contains source code only. Single requirements.txt at project root ensures unified dependencies on Airflow deployment.

---

### PHASE 1: v7.py SKELETON

#### Phase 1: Copy transformlivedata-v6.py → transformlivedata-v7.py (0.5 day)
**Task:**
```bash
cp transformlivedata-v6.py transformlivedata-v7.py
```

**Status:**
- v7.py is now exact copy of v6.py
- No code changes yet
- Ready for incremental integration of components

**Deliverable:** v7.py skeleton ✅

---

### DELIVERABLE 1: RAW VALIDATION (Integrate into v7)

#### Phase 2: Create & Implement RawDataExpectations (1 day)
**Configuration:** [raw_expectations.json](config/raw_expectations.json) ✅ (already created)

**Tasks:**
1. Create NEW file `transformlivedata/quality/raw_data_expectations.py`
2. Implement RawDataExpectations class (jsonschema-driven)
3. Load raw_expectations.json in `__init__`
4. Implement `validate(data: Dict) → Tuple[bool, List[str]]` method

```python
class RawDataExpectations:
    """Pre-transformation validation on raw API response (jsonschema-driven)"""
    
    def __init__(self, config_file: str) -> None:
        """Load raw_expectations.json JSON schema"""
        with open(config_file) as f:
            self.schema = json.load(f)
    
    def validate(self, data: Dict) -> Tuple[bool, List[str]]:
        """Validate raw API response dict against JSON schema
        Returns: (is_valid, error_messages)
        """
        try:
            jsonschema.validate(instance=data, schema=self.schema)
            return True, []
        except jsonschema.ValidationError as e:
            return False, [str(e)]
```

**Deliverable:** RawDataExpectations class ready for integration

---

#### Phase 3: Integrate RawDataExpectations into v7.py + Test (1 day)
**Tasks:**
1. In v7.py: `from quality.raw_data_expectations import RawDataExpectations`
2. Instantiate validator: `raw_validator = RawDataExpectations(config_file="config/raw_expectations.json")`
3. Find raw input point in v7 (where raw_positions_dict enters pipeline)
4. Add ONE validation call:
   ```python
   raw_valid, raw_errors = raw_validator.validate(raw_positions_dict)
   if not raw_valid:
       raise ValueError(f"Raw validation failed: {raw_errors}")
   ```
5. Run unit tests:
   - Load RawDataExpectations in test
   - Validate sample raw data
   - Verify 3 schema validations work
6. Run v7 with real data and confirm raw validation executes

**Key Point:** Keep transformation logic completely unchanged. Only add validation at input.

**Deliverable:** v7.py with raw validation integrated and tested ✅

---

### DELIVERABLE 2: TRANSFORMED VALIDATION (Integrate into v7)

#### Phase 4: Set Up GX Infrastructure (1 day)
**⚠️ IMPORTANT: Do this BEFORE creating TransformedDataExpectations (Phase 5)**

**Tasks:**
1. Initialize GX context
2. Create GX DataSource from pandas DataFrames
3. Set up GX checkpoint that loads expectations.json

```python
# Preparation code (Phase 4)
from great_expectations.core.batch import RuntimeBatchRequest
import great_expectations as gx

ge_context = gx.get_context()
datasource = ge_context.get_datasource("pandas_datasource")
checkpoint_config = ge_context.load_expectation_suite("expectations.json")
```

**Deliverable:** GX configured and ready for TransformedDataExpectations creation

---

#### Phase 5: Create & Implement TransformedDataExpectations (1 day)
**Configuration:** [expectations.json](config/expectations.json) ✅ (already created)

**Tasks:**
1. Create NEW file `transformlivedata/quality/transformed_data_expectations.py`
2. Initialize GX context + checkpoint in `__init__` (using Phase 4 setup)
3. Load expectations.json
4. Implement `validate_output_dataframe(output_df) → Dict` method

```python
class TransformedDataExpectations:
    """Post-transformation validation via GX checkpoint (independent)"""
    
    def __init__(self, expectations_config: str) -> None:
        """Initialize with expectations.json and GX context
        GX infrastructure must be set up in Phase 4 before this class exists
        """
        # Load expectations.json and initialize GX checkpoint
        pass
    
    def validate_output_dataframe(self, output_df) -> Dict:
        """Validate transformed DataFrame via GX checkpoint (11 expectations)"""
        # Run GX checkpoint
        # Return checkpoint results with validation status
        pass
```

**Deliverable:** TransformedDataExpectations class ready for integration

---

#### Phase 7: Create & Implement GX Data Docs Generation (1 day)
**⚠️ NEW PHASE: Data docs triggered on every v7.py run**

**Tasks:**
1. Create NEW file `transformlivedata/quality/gx_data_docs.py`
2. Implement GXDataDocsGenerator class
3. Initialize GX context with data_docs_config
4. Implement `generate_docs(checkpoint_result) → str` method
   - Returns path to generated docs directory
5. Store docs in: `./transformlivedata/gx_data_docs/`

```python
class GXDataDocsGenerator:
    """Generate GX data docs from checkpoint validation results"""
    
    def __init__(self, context_root_dir: str = "./transformlivedata/gx") -> None:
        """Initialize GX context and data docs config"""
        pass
    
    def generate_docs(self, checkpoint_result) -> str:
        """Generate HTML data docs from checkpoint results
        Returns: Path to generated docs directory
        """
        # Build data docs from checkpoint validation results
        # Store in gx_data_docs/ directory
        # Return docs path for logging
        pass
```

**Deliverable:** GXDataDocsGenerator class ready for integration

---

#### Phase 6: Integrate TransformedDataExpectations & GX Data Docs into v7.py + Test (1 day)
**Tasks:**
1. In v7.py: `from quality.transformed_data_expectations import TransformedDataExpectations`
2. In v7.py: `from quality.gx_data_docs import GXDataDocsGenerator`
3. Instantiate both:
   ```python
   transformed_validator = TransformedDataExpectations("./transformlivedata/config/expectations.json")
   docs_generator = GXDataDocsGenerator("./transformlivedata/gx")
   ```
4. Find transformed output point in v7 (after transform_positions())
5. Add TWO validation calls:
   ```python
   # Validate transformed output
   gx_result = transformed_validator.validate_output_dataframe(positions_table)
   if not gx_result['success']:
       raise ValueError(f"Transformed data validation failed")
   
   # Generate data docs from validation results
   docs_path = docs_generator.generate_docs(gx_result)
   logger.info(f"GX data docs generated at {docs_path}")
   ```
6. Run unit tests on both classes
7. Run v7 with real data and verify:
   - Transformed validation executes
   - Data docs are generated in `transformlivedata/gx_data_docs/`
   - Docs contain expected vs actual results

**Key Point:** Data docs are regenerated on every successful validation run.

**Deliverable:** v7.py with GX validation + data docs integrated and tested ✅

---

### DELIVERABLE 3: LINEAGE (Integrate into v7)

#### Phase 8: Create & Implement LineageReportGenerator (1 day)
**Configuration:** [lineage.json](config/lineage.json) ✅ (already created)

**Tasks:**
1. Create `transformlivedata/quality/lineage_report.py`
2. Implement LineageReportGenerator class
3. Load lineage.json in `__init__`
4. Implement `generate_html(output_file) → None` method

```python
class LineageReportGenerator:
    """Generate HTML lineage report from lineage.json (ZERO coupling to validation)."""
    
    def __init__(self, config_file: str):
        """Load lineage.json"""
        with open(config_file) as f:
            self.config = json.load(f)
    
    def generate_html(self, output_file: str = "lineage_report.html"):
        """Generate interactive HTML lineage diagram
        Shows: Job name/owner, 20 columns with sources & transformations
        """
        html = self._build_html()
        with open(output_file, "w") as f:
            f.write(html)
    
    def _build_html(self) -> str:
        """Build HTML with lineage tables and SVG diagram"""
        pass
```

**Deliverable:** LineageReportGenerator class ready for integration

---

#### Phase 9: Integrate LineageReportGenerator into v7.py + Test (1 day)
**Tasks:**
1. In v7.py: `from quality.lineage_report import LineageReportGenerator`
2. Instantiate generator: `lineage_gen = LineageReportGenerator(config_file="config/lineage.json")`
3. Find pipeline end point in v7
4. Add ONE lineage call:
   ```python
   lineage_gen.generate_html(output_file="lineage_report.html")
   ```
5. Run unit tests:
   - Load LineageReportGenerator in test
   - Generate HTML report
   - Verify all 20 columns in report
   - Verify HTML/SVG structure valid
6. Run v7 with real data and confirm lineage_report.html is generated correctly

**Key Point:** Lineage has ZERO coupling to validation classes. It's independent.

**Deliverable:** v7.py with raw validation + transformed validation + lineage integrated and tested ✅

---

## FINAL SUMMARY

**v7.py Final Integration (All 9 Phases Complete):**

v7.py will integrate:
- ✅ Phase 1-3: Raw validation (RawDataExpectations)
- ✅ Phase 4-7: Transformed validation + GX data docs (TransformedDataExpectations + GXDataDocsGenerator)
- ✅ Phase 8-9: Lineage reporting (LineageReportGenerator)

**Result:** Complete validation pipeline with automatic data docs generation triggered on every run
**End-to-End Validation:**
1. Run v7.py with real data
2. Verify complete pipeline:
   - Raw validation (Phase 3) ✅
   - Transform (unchanged logic)
   - Transformed validation (Phase 6) ✅
   - Lineage report generation (Phase 8) ✅
3. Verify all 14 validations execute (3 raw + 11 GX)
4. Verify lineage_report.html generated with 20 columns
5. v7.py is production-ready

**Deliverable:** v7.py fully integrated and validated ✅

---

#### Phase 10: Documentation (0.5 day)
**Tasks:**
1. Update [README.md](README.md) with v7 usage patterns
2. Document the incremental integration approach as reference for future migrations
3. Show the 3 integration points in v7:
   - Raw validation input point
   - Transformed validation output point
   - Lineage generation end point

**Deliverable:** Documentation complete ✅

---

## File Structure After Migration

```
transformlivedata/
├── config/
│   ├── raw_expectations.json                [NEW - JSON Schema for raw validation, 3 rules]
│   ├── expectations.json                    [NEW - GX ExpectationSuite for transformed validation, 11 rules]
│   └── lineage.json                         [NEW - column lineage metadata, 20 columns]
├── quality/
│   ├── raw_data_expectations.py             [NEW - RawDataExpectations class only]
│   ├── transformed_data_expectations.py     [NEW - TransformedDataExpectations class only]
│   ├── lineage_report.py                    [NEW - LineageReportGenerator class only]
│   └── __init__.py                          [NEW - empty init file]
├── transformlivedata-v7.py                  [NEW - copy of v6.py with 3 validation integrations]
├── test_raw_expectations.py                 [NEW - unit + integration tests for raw validation]
└── [other existing files unchanged]
```

**Key Principle:** Each class in its own file. No mixing implementations in ge_expectations.py. Each validator/lineage module is 100% independent and callable separately.

---

## Total Effort Estimate

| Phase | Days | Status | Notes |
|-------|------|--------|-------|
| **PHASE 1: v7 SKELETON** | | | |
| 1. Copy v6 → v7 | 0.5 | TODO | `cp transformlivedata-v6.py transformlivedata-v7.py` |
| **DELIVERABLE 1: RAW VALIDATION** | | | |
| 2. Create RawDataExpectations class | 1 | TODO | jsonschema-driven, ready for integration |
| 3. Integrate into v7 + test | 1 | TODO | Import, add raw validation call, test in v7 |
| **DELIVERABLE 2: TRANSFORMED VALIDATION** | | | |
| 4. Set Up GX Infrastructure | 1 | TODO | Initialize context, datasource, checkpoint (BEFORE class) |
| 5. Create TransformedDataExpectations class | 1 | TODO | GX-driven, ready for integration |
| 6. Integrate into v7 + test | 1 | TODO | Import, add transformed validation call, test in v7 |
| **DELIVERABLE 3: LINEAGE** | | | |
| 7. Create LineageReportGenerator class | 1 | TODO | HTML report generation, ready for integration |
| 8. Integrate into v7 + test | 1 | TODO | Import, add lineage generation call, test in v7 |
| **FINAL INTEGRATION** | | | |
| 9. Full Integration Tests (v7 complete) | 1 | TODO | End-to-end: raw → transform → GX → lineage |
| 10. Documentation | 0.5 | TODO | Update README with v7 patterns |
| **TOTAL** | **10 days** | | **v7 skeleton first, then incremental integration** |

---

## Success Criteria

✅ `raw_expectations.json` is JSON schema format (3 raw validations, standard JSON Schema)  
✅ `expectations.json` is pure Great Expectations ExpectationSuite format (11 transformed expectations)  
✅ `lineage.json` contains job metadata and 20 columns with input sources + transformations  
✅ `RawDataExpectations` is jsonschema-driven (loads raw_expectations.json schema), validates raw dicts via JSON schema  
✅ `TransformedDataExpectations` is GX-driven, completely independent from RawDataExpectations and lineage  
✅ `LineageReportGenerator` handles ONLY lineage, completely independent from expectations  
✅ No orchestration abstraction that combines both — all three classes called independently  
✅ All 14 validation rules execute: 3 raw jsonschema + 11 GX transformed expectations  
✅ All validation config in JSON files (no hardcoded methods except jsonschema/GX library usage)  
✅ GX checkpoint loads expectations.json and validates DataFrame  
✅ RawDataExpectations loads raw_expectations.json and validates API response dict  
✅ LineageReportGenerator produces interactive HTML report from lineage.json  
✅ HTML report shows all 20 columns with input sources, transformations, and dependency diagram  
✅ No external dependencies (no Marquez, no OpenLineage)  
✅ All expectations tests passing (Phases 1-4)  
✅ All lineage tests passing (Phases 5-7)  
✅ Clear documentation for separate expectations and lineage workflows  

---

## Design Principles

1. **Complete separation:** RawDataExpectations, TransformedDataExpectations, and LineageReportGenerator are independent with clear responsibilities
2. **Expectations first:** Prioritize data validation (Phases 1-4) before lineage (Phases 5-7)
3. **GX standard format:** expectations.json stays pure ExpectationSuite (no custom extensions)
4. **Lineage as secondary:** Lineage is metadata-only, not tied to validation workflow
5. **Configuration-driven:** All business logic in JSON files, orchestration in separate Python classes
6. **Local dev focused:** No Docker, no Marquez, no OpenLineage API
7. **Maintainable:** Edit JSON or class-specific methods without affecting the other module

---

**Ready to begin Phase 1: Create expectations.json**

