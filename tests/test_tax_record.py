import pathlib
import re
import sys
import unittest
from decimal import Decimal

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))
import tax_record as tr  # noqa: E402

GOOD = {
    "EmployerName": {"value": "Acme Inc", "confidence": 0.99},
    "EmployerEIN": {"value": "123456789", "confidence": 0.99},
    "WagesTipsOtherCompensation": {"value": "$50,000.00", "confidence": 0.99},
    "FederalIncomeTaxWithheld": {"value": "5,000", "confidence": 0.99},
    "TaxYear": {"value": "2025", "confidence": 0.99},
}


class NormalizeKeyTest(unittest.TestCase):
    def test_variants(self):
        self.assertEqual(tr.normalize_key("EmployerEIN"), "employer_ein")
        self.assertEqual(tr.normalize_key("Employer EIN:"), "employer_ein")
        self.assertEqual(tr.normalize_key("WagesTipsOtherCompensation"), "wages_tips_other_compensation")
        self.assertEqual(tr.normalize_key("1 Wages, tips, other compensation"), "1_wages_tips_other_compensation")
        self.assertEqual(tr.normalize_key(""), "")

    def test_alias_override(self):
        aliases = tr.load_field_aliases('{"wages": ["MyBox1"]}')
        self.assertIn("my_box1", aliases["wages"])
        with self.assertRaises(ValueError):
            tr.load_field_aliases('{"bogus": ["x"]}')


class ParserTest(unittest.TestCase):
    def test_money(self):
        self.assertEqual(tr.parse_money("$98,500.5"), Decimal("98500.50"))
        self.assertEqual(tr.parse_money("12 345"), Decimal("12345.00"))
        for bad in ["125OO.00", "-5", "", None, "1.234", "abc", "9999999999"]:
            with self.assertRaises(tr.ValidationError, msg=bad):
                tr.parse_money(bad)

    def test_ein(self):
        self.assertEqual(tr.parse_ein("12 3456789"), "12-3456789")
        with self.assertRaises(tr.ValidationError):
            tr.parse_ein("12-345678")

    def test_tax_year(self):
        self.assertEqual(tr.parse_tax_year("2025"), 2025)
        for bad in ["1999", "3025", "25", "20x5"]:
            with self.assertRaises(tr.ValidationError):
                tr.parse_tax_year(bad)

    def test_cross_field(self):
        _, errors = tr.validate_record({"employer_name": "A", "employer_ein": "123456789", "wages": "10",
                                        "fed_income_tax_withheld": "11", "tax_year": "2025"})
        self.assertIn("_record", errors)

    def test_mask_pii(self):
        self.assertEqual(tr.mask_pii("SSN 123-45-6789"), "SSN ***-**-6789")


class RoutingTest(unittest.TestCase):
    def test_clean_document_auto_verifies(self):
        canonical, d = tr.evaluate_extraction(GOOD, 0.85)
        self.assertFalse(d.needs_review, d.reasons)
        self.assertEqual(d.clean_values["wages"], Decimal("50000.00"))
        self.assertEqual(canonical["employer_ein"]["source_key"], "EmployerEIN")

    def test_empty_extraction_goes_to_review(self):
        # Regression: the original code auto-verified documents with zero fields.
        _, d = tr.evaluate_extraction({}, 0.85)
        self.assertTrue(d.needs_review)
        self.assertIn("no_fields_extracted", d.reasons)

    def test_low_confidence(self):
        data = dict(GOOD, TaxYear={"value": "2025", "confidence": 0.5})
        _, d = tr.evaluate_extraction(data, 0.85)
        self.assertEqual(d.low_confidence_fields, ["tax_year"])

    def test_noise_fields_do_not_gate(self):
        data = dict(GOOD, VoidCheckbox={"value": "", "confidence": 0.1})
        _, d = tr.evaluate_extraction(data, 0.85)
        self.assertFalse(d.needs_review)

    def test_conflicting_duplicates(self):
        data = dict(GOOD, **{"WagesTipsOtherCompensation__dup2": {"value": "1.00", "confidence": 0.9}})
        _, d = tr.evaluate_extraction(data, 0.85)
        self.assertIn("wages", d.conflicting_fields)
        self.assertTrue(d.needs_review)

    def test_threshold_bounds(self):
        with self.assertRaises(ValueError):
            tr.evaluate_extraction(GOOD, 0)


class ContractTest(unittest.TestCase):
    def test_document_id_is_deterministic_and_generation_sensitive(self):
        a = tr.document_id("b", "x.pdf", 1)
        self.assertEqual(a, tr.document_id("b", "x.pdf", 1))
        self.assertNotEqual(a, tr.document_id("b", "x.pdf", 2))

    def test_output_name_no_collision_between_extensions(self):
        a = tr.output_object_name("in/w2.pdf", tr.document_id("b", "in/w2.pdf", 1))
        b = tr.output_object_name("in/w2.png", tr.document_id("b", "in/w2.png", 1))
        self.assertNotEqual(a, b)
        self.assertNotIn("/", a)

    def test_mime(self):
        self.assertEqual(tr.mime_type_for("a.PDF"), "application/pdf")
        self.assertEqual(tr.mime_type_for("a.tiff"), "image/tiff")
        self.assertIsNone(tr.mime_type_for("a.docx"))

    def test_bq_row_types(self):
        _, d = tr.evaluate_extraction(GOOD, 0.85)
        row = tr.to_bq_row(d.clean_values, doc_id="id", source_bucket="b", source_file="f",
                           review_source=tr.REVIEW_SOURCE_AUTOMATED, reviewed_by=None, low_confidence_fields=[])
        self.assertEqual(row["wages"], "50000.00")  # exact NUMERIC as string, not float
        self.assertEqual(row["tax_year"], 2025)
        self.assertRegex(row["processed_at"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00$")
        with self.assertRaises(ValueError):
            tr.to_bq_row({}, doc_id="i", source_bucket="b", source_file="f", review_source="X",
                         reviewed_by=None, low_confidence_fields=[])

    def test_ddl_matches_contract(self):
        sql = (ROOT / "warehouse" / "looker_schema.sql").read_text()
        body = re.search(r"CREATE TABLE IF NOT EXISTS[^(]*\((.*?)\)\s*PARTITION", sql, re.S).group(1)
        cols = tuple(re.findall(r"^\s*(\w+)\s+(?:STRING|NUMERIC|INT64|TIMESTAMP|ARRAY)", body, re.M))
        self.assertEqual(cols, tr.BQ_COLUMNS)


if __name__ == "__main__":
    unittest.main()
