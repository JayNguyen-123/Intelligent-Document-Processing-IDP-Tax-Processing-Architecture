"""Offline end-to-end simulation of the ingestion routing logic.

Unlike the previous version (which re-implemented the threshold check), this
drives the SAME shared/tax_record.py code the Cloud Function uses, writes the
same payload contract to local folders that mirror the buckets, and exits
non-zero if any scenario routes differently than expected - so it doubles as a
CI smoke test.

    python local_testing/simulate_pipeline.py
"""
from __future__ import annotations

import json
import pathlib
import shutil
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "shared"))
import tax_record as tr  # noqa: E402

HITL_DIR = HERE / "hitl_queue"
VERIFIED_DIR = HERE / "verified_data"
THRESHOLD = 0.85


def f(value, confidence):
    return {"value": value, "confidence": confidence}


SCENARIOS = {
    # name: (extracted_data, expected_needs_review, expected_reason_subset)
    "clean_w2.pdf": ({
        "EmployerName": f("Innovate Tech Corp", 0.98),
        "EmployerEIN": f("12-3456789", 0.95),
        "WagesTipsOtherCompensation": f("$98,500.00", 0.97),
        "FederalIncomeTaxWithheld": f("14,200.00", 0.94),
        "TaxYear": f("2025", 0.99),
    }, False, set()),
    "smudged_w2.pdf": ({
        "w2_employer_name": f("Legacy Manufacturing LLC", 0.91),
        "w2_employer_ein": f("98-7654321", 0.89),
        "w2_wages": f("125OO.00", 0.64),  # OCR read 'O' for '0'
        "w2_fed_income_tax_withheld": f("1100.00", 0.87),
        "tax_year": f("2025", 0.95),
    }, True, {"low_confidence", "validation_failed"}),
    "blank_scan.png": ({}, True, {"no_fields_extracted", "missing_required_fields"}),
    "confident_but_invalid.pdf": ({
        "employer_name": f("Acme Inc", 0.99),
        "employer_ein": f("12-3456789", 0.99),
        "wages": f("1000.00", 0.99),
        "fed_income_tax_withheld": f("5000.00", 0.99),  # Box 2 > Box 1
        "tax_year": f("2025", 0.99),
    }, True, {"validation_failed"}),
    "missing_ein.pdf": ({
        "employer_name": f("Acme Inc", 0.99),
        "wages": f("1000.00", 0.99),
        "fed_income_tax_withheld": f("50.00", 0.99),
        "tax_year": f("2025", 0.99),
    }, True, {"missing_required_fields"}),
    "conflicting_wages.pdf": ({
        "employer_name": f("Acme Inc", 0.99),
        "employer_ein": f("12-3456789", 0.99),
        "wages": f("1000.00", 0.99),
        "wages__dup2": f("1900.00", 0.95),
        "fed_income_tax_withheld": f("50.00", 0.99),
        "tax_year": f("2025", 0.99),
    }, True, {"conflicting_values"}),
}


def run_scenario(name, extracted):
    doc_id = tr.document_id("mock-raw-tax-inputs", name, 1)
    canonical, decision = tr.evaluate_extraction(extracted, THRESHOLD)
    payload = tr.build_payload(
        doc_id=doc_id, source_bucket="mock-raw-tax-inputs", source_file=name, source_generation=1,
        mime_type=tr.mime_type_for(name), threshold=THRESHOLD, extracted=extracted,
        canonical=canonical, decision=decision,
    )
    out_name = tr.output_object_name(name, doc_id)
    if decision.needs_review:
        dest = HITL_DIR / tr.PREFIX_REVIEW_PENDING / out_name
        alert = {"alert_type": "MANUAL_REVIEW_REQUIRED", "document_id": doc_id, "file_name": name,
                 "review_reasons": decision.reasons,
                 "flagged_fields": sorted(set(decision.low_confidence_fields + decision.missing_fields
                                              + decision.conflicting_fields
                                              + list(decision.validation_errors)))}
        print(f"  -> HITL queue  | alert: {json.dumps(alert)}")
    else:
        dest = VERIFIED_DIR / tr.PREFIX_AUTOMATED / out_name
        row = tr.to_bq_row(decision.clean_values, doc_id=doc_id, source_bucket="mock-raw-tax-inputs",
                           source_file=name, review_source=tr.REVIEW_SOURCE_AUTOMATED,
                           reviewed_by=None, low_confidence_fields=[])
        print(f"  -> AUTO-VERIFIED | BigQuery row: {json.dumps(row)}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(payload, indent=2))
    return decision


def main() -> int:
    for d in (HITL_DIR, VERIFIED_DIR):
        shutil.rmtree(d, ignore_errors=True)
    failures = 0
    for name, (extracted, expect_review, expect_reasons) in SCENARIOS.items():
        print(f"[{name}]")
        decision = run_scenario(name, extracted)
        ok = decision.needs_review == expect_review and expect_reasons <= set(decision.reasons)
        print(f"  reasons={decision.reasons} {'PASS' if ok else 'FAIL'}\n")
        failures += 0 if ok else 1
    print(f"{len(SCENARIOS) - failures}/{len(SCENARIOS)} scenarios routed as expected. "
          f"Outputs in {HITL_DIR.name}/ and {VERIFIED_DIR.name}/")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
