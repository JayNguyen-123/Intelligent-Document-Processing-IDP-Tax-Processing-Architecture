"""Minimal in-memory stand-ins for the GCP / SendGrid SDKs so the Cloud Function
modules can be imported and exercised in unit tests without network or SDKs.
If the real SDKs are installed, they are still shadowed - tests stay hermetic."""
from __future__ import annotations

import sys
import types
from types import SimpleNamespace


def _module(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules[name] = mod
    return mod


# --- google.api_core.exceptions ------------------------------------------------
class GoogleAPICallError(Exception):
    @property
    def message(self):
        return str(self)


def _exc(name):
    return type(name, (GoogleAPICallError,), {})


EXC = {n: _exc(n) for n in ["InvalidArgument", "FailedPrecondition", "NotFound", "PermissionDenied",
                             "PreconditionFailed", "ServiceUnavailable"]}


# --- Fake GCS ------------------------------------------------------------------
class FakeBlob:
    def __init__(self, store, bucket, name):
        self.store, self.bucket_name, self.name = store, bucket, name

    @property
    def generation(self):
        return self.store.get((self.bucket_name, self.name), (None, None))[1]

    def exists(self):
        return (self.bucket_name, self.name) in self.store

    def download_as_bytes(self, if_generation_match=None):
        if (self.bucket_name, self.name) not in self.store:
            raise EXC["NotFound"](self.name)
        data, gen = self.store[(self.bucket_name, self.name)]
        if if_generation_match is not None and str(if_generation_match) != str(gen):
            raise EXC["PreconditionFailed"](self.name)
        return data

    def upload_from_string(self, data, content_type=None, if_generation_match=None):
        current = self.store.get((self.bucket_name, self.name))
        if if_generation_match == 0 and current is not None:
            raise EXC["PreconditionFailed"](self.name)
        gen = (current[1] + 1) if current else 1
        self.store[(self.bucket_name, self.name)] = (data.encode() if isinstance(data, str) else data, gen)


class FakeBucket:
    def __init__(self, store, name):
        self.store, self.name = store, name

    def blob(self, name):
        return FakeBlob(self.store, self.name, name)


class FakeStorageClient:
    def __init__(self, *a, **k):
        self.store = {}

    def bucket(self, name):
        return FakeBucket(self.store, name)


class FakeBigQuery:
    def __init__(self, *a, **k):
        self.rows, self.errors = [], []

    def insert_rows_json(self, table, rows, row_ids=None):
        self.rows.extend(rows)
        return self.errors


class FakeFuture:
    def __init__(self, fail=False):
        self.fail = fail

    def result(self, timeout=None):
        if self.fail:
            raise RuntimeError("publish failed")
        return "msg-1"


class FakePublisher:
    def __init__(self, *a, **k):
        self.messages, self.fail = [], False

    def topic_path(self, project, topic):
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic, data, **attrs):
        self.messages.append((topic, data, attrs))
        return FakeFuture(self.fail)


class FakeDocAI:
    def __init__(self, document=None, error=None):
        self.document, self.error = document, error

    def processor_path(self, *parts):
        return "/".join(parts)

    processor_version_path = processor_path

    def process_document(self, request=None, timeout=None):
        if self.error:
            raise self.error
        return SimpleNamespace(document=self.document)


def install() -> None:
    _module("google.api_core")
    _module("google.api_core.exceptions", GoogleAPICallError=GoogleAPICallError, **EXC)
    _module("google.api_core.client_options", ClientOptions=lambda **k: k)
    cloud = _module("google.cloud")
    docai = _module("google.cloud.documentai_v1",
                    DocumentProcessorServiceClient=FakeDocAI,
                    ProcessRequest=lambda **k: k, RawDocument=lambda **k: k)
    storage = _module("google.cloud.storage", Client=FakeStorageClient)
    bigquery = _module("google.cloud.bigquery", Client=FakeBigQuery)
    pubsub = _module("google.cloud.pubsub_v1", PublisherClient=FakePublisher)
    cloud.documentai_v1, cloud.storage, cloud.bigquery, cloud.pubsub_v1 = docai, storage, bigquery, pubsub
    import google
    google.api_core = sys.modules["google.api_core"]
    google.cloud = cloud
    sys.modules["google.api_core"].exceptions = sys.modules["google.api_core.exceptions"]
    sys.modules["google.api_core"].client_options = sys.modules["google.api_core.client_options"]

    def cloud_event(fn):
        return fn
    _module("functions_framework", cloud_event=cloud_event)

    class Mail:
        def __init__(self, **k):
            self.kwargs = k
    _module("sendgrid", SendGridAPIClient=lambda key: SimpleNamespace(send=lambda m: SimpleNamespace(status_code=202)))
    _module("sendgrid.helpers")
    _module("sendgrid.helpers.mail", Mail=Mail)


class CloudEvent(dict):
    """dict for ['id'] access plus a .data attribute, like cloudevents.http.CloudEvent."""
    def __init__(self, data, event_id="evt-1"):
        super().__init__(id=event_id)
        self.data = data
