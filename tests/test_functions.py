"""Tests for the ingestion and alert Cloud Functions using in-memory SDK fakes."""
import base64
import importlib
import json
import os
import pathlib
import sys
import unittest
from types import SimpleNamespace as NS

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "shared"))
import _stubs  # noqa: E402

_stubs.install()
os.environ.update({
    "GCP_PROJECT_ID": "proj", "DOCAI_PROCESSOR_ID": "proc",
    "HITL_BUCKET_NAME": "hitl", "VERIFIED_BUCKET_NAME": "verified",
})


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ingestion = _load(ROOT / "cloud_functions" / "ingestion" / "main.py", "ingestion_main")
alerts = _load(ROOT / "cloud_functions" / "alerts" / "main.py", "alerts_main")


def entity(type_, text, conf, props=()):
    return NS(type_=type_, mention_text=text, confidence=conf, properties=list(props), normalized_value=None)


def doc_with(entities):
    return NS(text="", entities=entities, pages=[])


GOOD_ENTITIES = [
    entity("EmployerName", "Acme Inc", 0.99), entity("EmployerEIN", "12-3456789", 0.99),
    entity("WagesTipsOtherCompensation", "50,000.00", 0.99),
    entity("FederalIncomeTaxWithheld", "5,000.00", 0.99), entity("TaxYear", "2025", 0.99),
]


class IngestionTest(unittest.TestCase):
    def setUp(self):
        self.gcs = _stubs.FakeStorageClient()
        self.bq = _stubs.FakeBigQuery()
        self.pub = _stubs.FakePublisher()
        self.gcs.bucket("raw").blob("in/w2.pdf").upload_from_string(b"%PDF-1.7 fake")
        ingestion._clients.clear()
        ingestion._clients.update(gcs=self.gcs, bq=self.bq, pub=self.pub)

    def event(self, name="in/w2.pdf", size=13):
        return _stubs.CloudEvent({"bucket": "raw", "name": name, "generation": "1", "size": str(size),
                                  "contentType": "application/pdf"})

    def keys(self, bucket):
        return sorted(n for (b, n) in self.gcs.store if b == bucket)

    def test_clean_document_goes_to_bigquery_and_verified(self):
        ingestion._clients["docai"] = _stubs.FakeDocAI(doc_with(GOOD_ENTITIES))
        ingestion.process_tax_upload(self.event())
        self.assertEqual(len(self.bq.rows), 1)
        self.assertEqual(self.bq.rows[0]["wages"], "50000.00")
        self.assertEqual(len(self.keys("verified")), 1)
        self.assertEqual(self.pub.messages, [])

    def test_duplicate_event_is_idempotent(self):
        ingestion._clients["docai"] = _stubs.FakeDocAI(doc_with(GOOD_ENTITIES))
        ingestion.process_tax_upload(self.event())
        ingestion.process_tax_upload(self.event())
        self.assertEqual(len(self.bq.rows), 1)

    def test_empty_extraction_routes_to_hitl_with_alert(self):
        ingestion._clients["docai"] = _stubs.FakeDocAI(doc_with([]))
        ingestion.process_tax_upload(self.event())
        self.assertEqual(self.bq.rows, [])
        hitl = self.keys("hitl")
        self.assertEqual(len(hitl), 1)
        self.assertTrue(hitl[0].startswith("review_pending/"))
        msg = json.loads(self.pub.messages[0][1])
        self.assertIn("no_fields_extracted", msg["review_reasons"])

    def test_permanent_docai_error_routes_to_hitl(self):
        ingestion._clients["docai"] = _stubs.FakeDocAI(error=_stubs.EXC["InvalidArgument"]("too many pages"))
        ingestion.process_tax_upload(self.event())
        payload = json.loads(self.gcs.store[("hitl", self.keys("hitl")[0])][0])
        self.assertIn("processing_error", payload["metadata"]["review_reasons"])

    def test_transient_docai_error_is_raised_for_retry(self):
        ingestion._clients["docai"] = _stubs.FakeDocAI(error=_stubs.EXC["ServiceUnavailable"]("503"))
        with self.assertRaises(_stubs.GoogleAPICallError):
            ingestion.process_tax_upload(self.event())
        self.assertEqual(self.keys("hitl"), [])

    def test_publish_failure_raises_and_retry_resends(self):
        ingestion._clients["docai"] = _stubs.FakeDocAI(doc_with([]))
        self.pub.fail = True
        with self.assertRaises(RuntimeError):
            ingestion.process_tax_upload(self.event())
        self.pub.fail = False
        ingestion.process_tax_upload(self.event())  # retry: queue item exists -> re-alert
        self.assertEqual(len(self.pub.messages), 2)
        self.assertEqual(len(self.keys("hitl")), 1)

    def test_unsupported_and_folder_objects(self):
        ingestion._clients["docai"] = _stubs.FakeDocAI(error=AssertionError("must not be called"))
        ingestion.process_tax_upload(self.event(name="in/"))
        self.assertEqual(self.keys("hitl"), [])
        ev = _stubs.CloudEvent({"bucket": "raw", "name": "in/notes.docx", "generation": "1", "size": "5",
                                "contentType": "application/msword"})
        ingestion.process_tax_upload(ev)
        self.assertEqual(len(self.keys("hitl")), 1)

    def test_bigquery_failure_raises_before_marking_done(self):
        ingestion._clients["docai"] = _stubs.FakeDocAI(doc_with(GOOD_ENTITIES))
        self.bq.errors = [{"index": 0, "errors": ["bad"]}]
        with self.assertRaises(RuntimeError):
            ingestion.process_tax_upload(self.event())
        self.assertEqual(self.keys("verified"), [])  # will be retried

    def test_form_parser_fallback_joins_segments_and_keeps_duplicates(self):
        text = "Wages: 1,000.00 Wages: 2,000.00"
        seg = lambda s, e: NS(start_index=s, end_index=e)  # noqa: E731
        anchor = lambda *segs: NS(text_segments=list(segs))  # noqa: E731
        ff = lambda ls, le, vs, ve: NS(field_name=NS(text_anchor=anchor(seg(ls, le)), confidence=0.9),  # noqa: E731
                                       field_value=NS(text_anchor=anchor(seg(vs, ve)), confidence=0.95))
        doc = NS(text=text, entities=[], pages=[NS(form_fields=[ff(0, 6, 7, 15), ff(16, 22, 23, 31)])])
        fields = ingestion.extract_fields(doc)
        self.assertEqual(fields["wages"], {"value": "1,000.00", "confidence": 0.9})
        self.assertIn("wages__dup2", fields)


class AlertsTest(unittest.TestCase):
    def msg(self, alert):
        return _stubs.CloudEvent({"message": {"data": base64.b64encode(json.dumps(alert).encode()).decode()}})

    def test_escaping(self):
        alert = {"alert_type": "MANUAL_REVIEW_REQUIRED", "file_name": "<script>x</script>.pdf",
                 "flagged_fields": ["a<b"], "review_reasons": ["low_confidence"], "document_id": "d"}
        self.assertNotIn("<script>", alerts.build_email_html(alert))
        self.assertNotIn("<script>", json.dumps(alerts.build_slack_payload(alert)))

    def test_malformed_message_is_acked(self):
        alerts.handle_hitl_alert(_stubs.CloudEvent({"message": {"data": "bm90IGpzb24="}}))

    def test_all_channels_failing_raises_for_retry(self):
        alerts.SLACK_WEBHOOK_URL = "https://hooks.example/x"
        alerts.send_slack_notification = lambda a: (_ for _ in ()).throw(alerts.DeliveryError("down"))
        try:
            with self.assertRaises(alerts.DeliveryError):
                alerts.handle_hitl_alert(self.msg({"alert_type": "MANUAL_REVIEW_REQUIRED", "file_name": "f"}))
        finally:
            alerts.SLACK_WEBHOOK_URL = ""


if __name__ == "__main__":
    unittest.main()
