from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

PACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACK_ROOT))

from lib import rabbitmq_client


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fake_pika_module():
    module = ModuleType("pika")

    class PlainCredentials:
        def __init__(self, username, password):
            self.username = username
            self.password = password

    class ExternalCredentials:
        pass

    class SSLOptions:
        def __init__(self, context, host):
            self.context = context
            self.host = host

    class ConnectionParameters:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    module.PlainCredentials = PlainCredentials
    module.credentials = SimpleNamespace(ExternalCredentials=ExternalCredentials)
    module.SSLOptions = SSLOptions
    module.ConnectionParameters = ConnectionParameters
    return module


class FakeChannel:
    def __init__(self, events=None):
        self.events = events if events is not None else []

    def basic_ack(self, **kwargs):
        self.events.append(("ack", kwargs))

    def basic_nack(self, **kwargs):
        self.events.append(("nack", kwargs))


def delivery(tag=1, redelivered=False, exchange="events", routing_key="test"):
    return SimpleNamespace(
        delivery_tag=tag,
        redelivered=redelivered,
        exchange=exchange,
        routing_key=routing_key,
        consumer_tag="consumer-1",
    )


class PackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sensor = load_module("rabbitmq_sensor_test", PACK_ROOT / "sensors" / "rabbitmq_message.py")

    def test_pack_and_resource_contracts(self):
        pack = (PACK_ROOT / "pack.yaml").read_text(encoding="utf-8")
        self.assertIn('source_version: "1.1.1"', pack)
        self.assertIn('source_revision: "e79bcedf5cef611ff6088b79d9e5926a288fa951"', pack)
        self.assertIn('license: "Apache-2.0"', pack)
        action_paths = list((PACK_ROOT / "actions").glob("*.yaml"))
        self.assertEqual(
            {path.stem for path in action_paths},
            {"publish_message", "list_exchanges", "list_queues", "list_bindings"},
        )
        for path in action_paths:
            action = path.read_text(encoding="utf-8")
            self.assertIn(f"ref: rabbitmq.{path.stem}", action)
            self.assertIn("runner_type: python", action)
            self.assertIn("entry_point: rabbitmq_action.py", action)
            self.assertIn("parameter_delivery: stdin", action)
            self.assertIn("parameter_format: json", action)
            self.assertIn("output_format: json", action)
            self.assertIn("default_execution_permission_set_refs: [standard]", action)
            self.assertIn("operation: {type: string, required: true}", action)
            self.assertIn("result: {type: object, required: true}", action)
        trigger = (PACK_ROOT / "triggers" / "message.yaml").read_text(encoding="utf-8")
        sensor = (PACK_ROOT / "sensors" / "rabbitmq_message.yaml").read_text(encoding="utf-8")
        self.assertIn("trigger_types: [rabbitmq.message]", sensor)
        self.assertIn('pattern: "^/run/secrets/.+"', trigger)
        self.assertIn("body_base64:", trigger)
        self.assertNotIn("password", "".join(path.read_text(encoding="utf-8") for path in action_paths))

    def test_license_and_notice_match_verified_source(self):
        license_text = (PACK_ROOT / "LICENSE").read_text(encoding="utf-8")
        notice = (PACK_ROOT / "NOTICE").read_text(encoding="utf-8")
        self.assertIn("Apache License", license_text)
        self.assertIn("Version 2.0, January 2004", license_text)
        self.assertIn("e79bcedf5cef611ff6088b79d9e5926a288fa951", notice)

    def test_key_lookup_requests_decryption(self):
        calls = {}
        get_key = ModuleType("attune.api_client.api.secrets.get_key")
        get_key.sync_detailed = lambda ref, *, client, decrypt: calls.update(ref=ref, client=client, decrypt=decrypt) or SimpleNamespace(
            status_code=200,
            parsed=SimpleNamespace(data=SimpleNamespace(value={"amqp": {"host": "broker"}})),
        )
        secrets = ModuleType("attune.api_client.api.secrets")
        secrets.get_key = get_key
        attune = ModuleType("attune")
        attune.context = SimpleNamespace(client="execution-client")
        modules = {
            "attune": attune,
            "attune.api_client": ModuleType("attune.api_client"),
            "attune.api_client.api": ModuleType("attune.api_client.api"),
            "attune.api_client.api.secrets": secrets,
        }
        with patch.dict(sys.modules, modules):
            rabbitmq_client.fetch_key("rabbitmq.credentials")
        self.assertEqual(calls, {"ref": "rabbitmq.credentials", "client": "execution-client", "decrypt": True})

    def test_amqp_parameters_are_bounded_and_support_oauth_and_external(self):
        pika = fake_pika_module()
        base = {
            "host": "broker.example",
            "vhost": "/production",
            "tls": True,
            "heartbeat_seconds": 45,
            "socket_timeout_seconds": 9,
            "stack_timeout_seconds": 12,
            "blocked_timeout_seconds": 20,
        }
        with patch.dict(sys.modules, {"pika": pika}), patch.object(rabbitmq_client, "_ssl_context", return_value="context"):
            oauth = rabbitmq_client.connection_parameters({**base, "auth_method": "oauth2", "token": "synthetic-token"})
            external = rabbitmq_client.connection_parameters(
                {**base, "auth_method": "external", "client_cert_file": "/cert", "client_key_file": "/key"}
            )
        self.assertEqual(oauth.kwargs["credentials"].username, "")
        self.assertEqual(oauth.kwargs["credentials"].password, "synthetic-token")
        self.assertEqual(oauth.kwargs["socket_timeout"], 9.0)
        self.assertEqual(oauth.kwargs["stack_timeout"], 12.0)
        self.assertEqual(oauth.kwargs["ssl_options"].host, "broker.example")
        self.assertIsInstance(external.kwargs["credentials"], pika.credentials.ExternalCredentials)
        with patch.dict(sys.modules, {"pika": pika}), self.assertRaisesRegex(
            rabbitmq_client.RabbitMQPackError, "socket_timeout_seconds"
        ):
            rabbitmq_client.connection_parameters({**base, "auth_method": "oauth2", "token": "x", "socket_timeout_seconds": 0})

    def test_management_paginates_and_percent_encodes_path_segments(self):
        requests = ModuleType("requests")
        calls = []
        responses = [
            SimpleNamespace(status_code=200, json=lambda: {"items": [{"name": "one"}], "page_count": 2}),
            SimpleNamespace(status_code=200, json=lambda: {"items": [{"name": "two"}], "page_count": 2}),
        ]

        def get(url, **kwargs):
            calls.append((url, kwargs))
            return responses.pop(0)

        requests.get = get
        credentials = {
            "management": {
                "url": "https://broker.example/rabbitmq",
                "auth_method": "basic",
                "username": "monitor",
                "password": "synthetic-password",
                "timeout_seconds": 8,
            }
        }
        with patch.dict(sys.modules, {"requests": requests}), patch.object(rabbitmq_client, "fetch_key", return_value=credentials):
            result = rabbitmq_client.list_queues({"vhost": "/team/a", "page_size": 1, "max_pages": 5})
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["pages"], 2)
        self.assertFalse(result["truncated"])
        self.assertIn("/api/queues/%2Fteam%2Fa", calls[0][0])
        self.assertEqual(calls[1][1]["params"]["page"], 2)
        self.assertEqual(calls[0][1]["params"]["disable_stats"], "false")
        self.assertEqual(calls[0][1]["timeout"], (8.0, 8.0))
        self.assertFalse(calls[0][1]["allow_redirects"])

    def test_binding_resource_names_are_encoded(self):
        requests = ModuleType("requests")
        calls = []
        requests.get = lambda url, **kwargs: calls.append((url, kwargs)) or SimpleNamespace(status_code=200, json=list)
        credentials = {
            "management": {
                "url": "https://broker.example",
                "auth_method": "oauth2",
                "token": "synthetic-token",
            }
        }
        with patch.dict(sys.modules, {"requests": requests}), patch.object(rabbitmq_client, "fetch_key", return_value=credentials):
            rabbitmq_client.list_bindings({"vhost": "/", "source_exchange": "source/a", "destination_queue": "queue b"})
        self.assertIn("/api/bindings/%2F/e/source%2Fa/q/queue%20b", calls[0][0])
        self.assertEqual(calls[0][1]["headers"]["Authorization"], "Bearer synthetic-token")

    def test_management_errors_do_not_expose_response_or_credentials(self):
        requests = ModuleType("requests")
        requests.get = lambda *args, **kwargs: SimpleNamespace(status_code=401, text="synthetic-secret-body")
        credentials = {
            "management": {
                "url": "https://secret-host.invalid",
                "username": "synthetic-user",
                "password": "synthetic-password",
            }
        }
        with patch.dict(sys.modules, {"requests": requests}), self.assertRaises(rabbitmq_client.RabbitMQPackError) as raised:
            rabbitmq_client.management_request(credentials, ["queues", "/"])
        message = str(raised.exception)
        self.assertIn("status 401", message)
        self.assertNotIn("synthetic-secret-body", message)
        self.assertNotIn("synthetic-password", message)
        self.assertNotIn("secret-host", message)

    def test_publish_uses_confirm_mandatory_and_persistent_properties(self):
        pika = fake_pika_module()
        calls = []

        class BasicProperties:
            def __init__(self, **kwargs):
                self.values = kwargs

        class Channel:
            def confirm_delivery(self):
                calls.append("confirm")

            def basic_publish(self, **kwargs):
                calls.append(("publish", kwargs))
                return True

        class Connection:
            is_open = True

            def __init__(self, parameters):
                self.parameters = parameters

            def channel(self):
                return Channel()

            def close(self):
                self.is_open = False
                calls.append("close")

        pika.BasicProperties = BasicProperties
        pika.BlockingConnection = Connection
        credentials = {"amqp": {"host": "broker", "tls": False, "auth_method": "plain", "username": "user", "password": "secret"}}
        params = {"message": "hello", "exchange": "events", "routing_key": "key", "message_id": "id-1"}
        with patch.dict(sys.modules, {"pika": pika}), patch.object(rabbitmq_client, "fetch_key", return_value=credentials):
            result = rabbitmq_client.publish_message(params)
        publish = calls[1][1]
        self.assertEqual(calls[0], "confirm")
        self.assertEqual(publish["body"], b"hello")
        self.assertTrue(publish["mandatory"])
        self.assertEqual(publish["properties"].values["delivery_mode"], 2)
        self.assertEqual(publish["properties"].values["message_id"], "id-1")
        self.assertEqual(calls[-1], "close")
        self.assertEqual(result, {"confirmed": True, "persistent": True, "mandatory": True})

    def test_delivery_acks_only_after_successful_emit(self):
        events = []
        channel = FakeChannel(events)

        def emit(payload):
            events.append(("emit", payload))
            return 42

        handler = self.sensor.DeliveryHandler("queue", {}, emit, lambda seconds: None)
        properties = SimpleNamespace(headers={}, message_id="id-1")
        handler.handle(channel, delivery(), properties, b"hello")
        self.assertEqual([item[0] for item in events], ["emit", "ack"])
        payload = events[0][1]
        self.assertEqual(payload["body_text"], "hello")
        self.assertEqual(payload["body_base64"], "aGVsbG8=")
        self.assertEqual(events[1][1], {"delivery_tag": 1, "multiple": False})

    def test_delivery_requeues_then_terminally_nacks(self):
        events = []
        delays = []
        payloads = []

        def emit(payload):
            payloads.append(payload)
            raise RuntimeError("synthetic emission failure")

        handler = self.sensor.DeliveryHandler(
            "queue",
            {"max_retries": 1, "retry_delay_seconds": 0.25},
            emit,
            delays.append,
        )
        channel = FakeChannel(events)
        properties = SimpleNamespace(headers={}, message_id="retry-id")
        handler.handle(channel, delivery(tag=1), properties, b"body")
        handler.handle(channel, delivery(tag=2, redelivered=True), properties, b"body")
        self.assertEqual(payloads[0]["retry_count"], 0)
        self.assertEqual(payloads[1]["retry_count"], 1)
        self.assertEqual(delays, [0.25])
        self.assertTrue(events[0][1]["requeue"])
        self.assertFalse(events[1][1]["requeue"])
        self.assertTrue(all(not item[1]["multiple"] for item in events))

    def test_broker_delivery_count_can_force_terminal_nack(self):
        channel = FakeChannel()
        properties = SimpleNamespace(headers={"x-delivery-count": 4}, message_id="id-1")
        handler = self.sensor.DeliveryHandler("queue", {"max_retries": 3}, lambda payload: None, lambda seconds: None)
        handler.handle(channel, delivery(redelivered=True), properties, b"body")
        self.assertEqual(channel.events, [("nack", {"delivery_tag": 1, "multiple": False, "requeue": False})])

    def test_delivery_without_safe_identity_requeues_instead_of_conflating_bodies(self):
        channel = FakeChannel()
        properties = SimpleNamespace(headers={}, message_id=None)
        handler = self.sensor.DeliveryHandler("queue", {"max_retries": 0, "retry_delay_seconds": 0}, lambda payload: None, lambda seconds: None)
        handler.handle(channel, delivery(tag=1), properties, b"identical")
        handler.handle(channel, delivery(tag=2, redelivered=True), properties, b"identical")
        self.assertEqual(len(channel.events), 2)
        self.assertTrue(all(item[1]["requeue"] for item in channel.events))

    def test_ack_failure_does_not_attempt_a_second_settlement(self):
        class FailingAckChannel(FakeChannel):
            def basic_ack(self, **kwargs):
                self.events.append(("ack", kwargs))
                raise RuntimeError("closed channel")

        channel = FailingAckChannel()
        handler = self.sensor.DeliveryHandler("queue", {}, lambda payload: 1, lambda seconds: None)
        with self.assertRaisesRegex(RuntimeError, "closed channel"):
            handler.handle(channel, delivery(), SimpleNamespace(headers={}, message_id="id-1"), b"body")
        self.assertEqual([event[0] for event in channel.events], ["ack"])

    def test_successful_message_id_is_deduplicated_in_memory(self):
        emitted = []
        channel = FakeChannel()
        handler = self.sensor.DeliveryHandler("queue", {}, lambda payload: emitted.append(payload) or 1, lambda seconds: None)
        properties = SimpleNamespace(headers={}, message_id="stable-id")
        handler.handle(channel, delivery(tag=1), properties, b"body")
        handler.handle(channel, delivery(tag=2, redelivered=True), properties, b"body")
        self.assertEqual(len(emitted), 1)
        self.assertEqual([event[0] for event in channel.events], ["ack", "ack"])

    def test_binary_and_oversized_bodies_are_safe(self):
        payloads = []
        binary_channel = FakeChannel()
        handler = self.sensor.DeliveryHandler("queue", {}, lambda payload: payloads.append(payload) or 1, lambda seconds: None)
        handler.handle(binary_channel, delivery(), SimpleNamespace(headers={}, message_id=None), b"\xff\x00")
        self.assertNotIn("body_text", payloads[0])
        self.assertEqual(payloads[0]["body_base64"], "/wA=")

        oversized_channel = FakeChannel()
        oversized = self.sensor.DeliveryHandler("queue", {"max_body_bytes": 1}, lambda payload: self.fail("must not emit"), lambda seconds: None)
        oversized.handle(oversized_channel, delivery(), SimpleNamespace(headers={}, message_id=None), b"xx")
        self.assertFalse(oversized_channel.events[0][1]["requeue"])

    def test_sensor_credentials_are_confined_and_size_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "secrets"
            root.mkdir()
            allowed = root / "rabbitmq.json"
            allowed.write_text(json.dumps({"amqp": {"host": "broker"}}), encoding="utf-8")
            outside = Path(directory) / "outside.json"
            outside.write_text("{}", encoding="utf-8")
            with patch.object(self.sensor, "SENSOR_CREDENTIALS_ROOT", root):
                self.assertEqual(self.sensor.read_credentials_file(str(allowed))["amqp"]["host"], "broker")
                with self.assertRaises(ValueError):
                    self.sensor.read_credentials_file(str(outside))
                allowed.write_text("x" * (self.sensor.MAX_CREDENTIAL_FILE_BYTES + 1), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "64 KiB"):
                    self.sensor.read_credentials_file(str(allowed))
            with self.assertRaises(ValueError):
                self.sensor.read_credentials_file("relative.json")

    def test_worker_stop_schedules_cancellation_on_connection_thread(self):
        rule = SimpleNamespace(rule_id=7, trigger_params={"queue": "events", "retry_delay_seconds": 0})
        worker = self.sensor.ConsumerWorker(rule, {"amqp": {"host": "broker"}}, Mock(), lambda payload: 1)
        channel = SimpleNamespace(is_open=True, stop_consuming=Mock())
        callbacks = []
        connection = SimpleNamespace(is_open=True, add_callback_threadsafe=callbacks.append)
        worker._channel = channel
        worker._connection = connection
        worker._thread = SimpleNamespace(join=Mock(), is_alive=lambda: False)
        worker.stop()
        self.assertEqual(len(callbacks), 1)
        callbacks[0]()
        channel.stop_consuming.assert_called_once_with()
        worker._thread.join.assert_called_once_with(timeout=10)

    def test_entrypoint_rejects_malformed_json_without_echoing_it(self):
        module = load_module("rabbitmq_action_test", PACK_ROOT / "actions" / "rabbitmq_action.py")
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(sys, "stdin", SimpleNamespace(read=lambda: '{"password":"synthetic-secret"')), redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(module.main(), 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertNotIn("synthetic-secret", stderr.getvalue())

    def test_no_unsafe_deserialization_or_live_credentials(self):
        forbidden = ["pickle" + ".loads", "guest" + ":guest", "amqp://" + "guest"]
        for path in PACK_ROOT.rglob("*"):
            if path.is_file() and path.suffix in {".py", ".yaml", ".md", ".txt"}:
                text = path.read_text(encoding="utf-8")
                self.assertFalse(any(value in text for value in forbidden), str(path))


if __name__ == "__main__":
    unittest.main()
