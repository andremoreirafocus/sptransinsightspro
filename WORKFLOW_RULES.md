# JSON-Driven Validation Migration Workflow Rules

**Last Updated:** 2026-03-04  
**Status:** LOCKED - These rules are non-negotiable

---

## 🚫 CRITICAL RULES (Enforce Every Turn)

### Rule 1: NO PHASE PROGRESSION WITHOUT EXPLICIT APPROVAL
- ❌ **NEVER** mark a phase as "in-progress" without user approval
- ❌ **NEVER** assume user wants next phase after current phase completes
- ✅ **ALWAYS** wait for explicit user request to advance phases
- ✅ **ALWAYS** ask "Should I proceed with Phase X?" if user indicates Phase X-1 is done

### Rule 2: NEW FILES ONLY - No Modifications to Existing Files
- ❌ **NEVER** add code to existing files (e.g., `ge_expectations.py`, `validate_positions.py`)
- ✅ **ONLY** create NEW dedicated files for new implementations
- ✅ Each implementation class gets its own file:
  - `raw_data_expectations.py` (Phase 2) ✅ EXISTS
  - `transformed_data_expectations.py` (Phase 5) - CREATE ONLY ON APPROVAL
  - `lineage_tracker.py` (Phase 7) - CREATE ONLY ON APPROVAL

### Rule 3: NO PREMATURE FILE CREATION
- ❌ **NEVER** create files without explicit user request
- ❌ **NEVER** "set up" infrastructure without being asked
- ✅ **ONLY** create files when user approves the phase
- ✅ Wait for phase approval → then create exactly what is needed

### Rule 4: RESPECT DELIVERABLE BOUNDARIES
- Phase 1-3: **Deliverable 1** (Raw Validation) ✅ COMPLETE
  - v7.py skeleton
  - RawDataExpectations class
  - Integration & testing
- Phase 4-6: **Deliverable 2** (Transformed Validation) - PENDING APPROVAL
  - Setup GX infrastructure
  - TransformedDataExpectations class
  - Integration & testing
- Phase 7-8: **Deliverable 3** (Lineage & Docs) - PENDING APPROVAL

### Rule 5: SINGLE requirements.txt (Airflow Mirroring)
- ❌ **NEVER** add dependencies to subfolder requirements.txt (e.g., `transformlivedata/requirements.txt`)
- ✅ **ALWAYS** add dependencies to `/dags-dev/requirements.txt` (project root)
- ✅ **ALWAYS** run `pip install -r requirements.txt` immediately after adding
- **Why:** Mirrors Airflow's architecture where subfolders contain source code only. Single requirements.txt ensures unified environment.

---

## ✅ Current Status (As of 2026-03-04)

### Completed Phases
- ✅ **Phase 1**: v7.py skeleton (copy of v6) - DONE
- ✅ **Phase 2**: RawDataExpectations class in dedicated file - DONE
  - File: `transformlivedata/quality/raw_data_expectations.py`
  - Tests: `transformlivedata/test_raw_expectations.py`
  - Status: 15 unit tests, all passing
- ✅ **Phase 3**: Raw validation integrated into v7.py - DONE & TESTED
  - Integration point: lines 78-88 in v7.py
  - Execution result: 11,101 records processed, validation passed
- ✅ **Phase 4**: Set Up GX Infrastructure - DONE & TESTED
  - File: `transformlivedata/quality/gx_infrastructure.py`
  - Tests: `transformlivedata/test_gx_infrastructure.py`
  - Status: 10 unit tests, all passing
  - Provides: GXInfrastructure class for managing expectations configuration

### Pending Phases
- ⏸️ **Phase 5**: TransformedDataExpectations class - **AWAITING USER APPROVAL**
- ⏸️ **Phase 6**: Integrate GX validation - **AWAITING PHASE 5 COMPLETION**
- ⏸️ **Phase 7**: GX Data Docs generation (NEW - triggered on every run) - **AWAITING PHASE 6 COMPLETION**
- ⏸️ **Phase 8**: Create LineageReportGenerator class - **AWAITING PHASE 7 COMPLETION**
- ⏸️ **Phase 9**: Integrate LineageReportGenerator + test - **AWAITING PHASE 8 COMPLETION**

---

## 📋 Checklist for Agent on Each Turn

Before taking ANY action, verify:

- [ ] Is this phase already complete in the status above?
- [ ] Has the user explicitly approved progression to the next phase?
- [ ] Am I about to modify an existing file (RED FLAG - don't do it)?
- [ ] Am I about to create a file without approval (RED FLAG - don't do it)?
- [ ] Have I confirmed the user's intent and approved the work first?

**If answer to any RED FLAG question is YES: STOP and ask for clarification.**

---

## 🔒 Non-Negotiable Enforcement

These rules exist because:
1. **Trust erosion** - Breaking rules repeatedly destroys credibility
2. **Scope creep** - Uncontrolled progression leads to confused codebases
3. **Reproducibility** - Strict phases ensure work can be reviewed/reverted
4. **User control** - You decide pace, not the agent

**Violation = Lost trust. Trust is not recoverable without strict adherence.**

---

## Next Steps

**User must explicitly state:**
- "Proceed with Phase 4" OR
- "Let me review Phase 3 output first" OR  
- "Work on [something else]"

**Until explicit approval is given, agent must WAIT.**
