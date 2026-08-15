#!/usr/bin/env python3
"""Rule-targeted RabbitMQ consumer with manual delivery acknowledgement."""

from __future__ import annotations

import base64
import json
import os
import stat
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

_PACK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PACK_ROOT not in sys.path:
    sys.path.insert(0, _PACK_ROOT)

from lib.rabbitmq_client import amqp_config, connection_parameters

SENSOR_CREDENTIALS_ROOT = Path("/run/secrets")
MAX_CREDENTIAL_FILE_BYTES = 65536


def read_credentials_file(path_value: Any) -> dict[str, Any]:
    if not isinstance(path_value, str) or not os.path.isabs(path_value):
        raise ValueError("credential_file must be an absolute path")
    root = SENSOR_CREDENTIALS_ROOT.resolve()
    candidate = Path(path_value).resolve()
    if candidate == root or root not in candidate.parents:
        raise ValueError("credential_file must be below /run/secrets")
    try:
        metadata = candidate.stat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_CREDENTIAL_FILE_BYTES:
            raise ValueError("credential_file must be a regular JSON file no larger than 64 KiB")
        raw = candidate.read_text(encoding="utf-8")
    except ValueError:
        raise
    except (OSError, UnicodeError) as exc:
        raise ValueError("credential_file must contain a readable JSON object") from exc
    if len(raw.encode("utf-8")) > MAX_CREDENTIAL_FILE_BYTES:
        raise ValueError("credential_file must be a regular JSON file no larger than 64 KiB")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("credential_file must contain a readable JSON object") from exc
    if not isinstance(value, dict):
        raise ValueError("credential_file must contain a JSON object")  # noqa: TRY004
    if not isinstance(value.get("amqp", value), dict):
        raise ValueError("credential_file amqp value must be an object")  # noqa: TRY004
    return value


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return str(value)


def _broker_retry_state(properties: Any) -> tuple[int, bool]:
    headers = getattr(properties, "headers", None) or {}
    value = headers.get("x-delivery-count")
    if isinstance(value, int) and value >= 0:
        return value, True
    deaths = headers.get("x-death")
    if isinstance(deaths, list):
        counts = [item.get("count", 0) for item in deaths if isinstance(item, dict)]
        valid = [item for item in counts if isinstance(item, int) and item >= 0]
        if valid:
            return max(valid), True
    return 0, False


class DeliveryHandler:
    """Process one delivery and settle only that delivery tag."""

    def __init__(
        self,
        queue: str,
        config: Mapping[str, Any],
        emit: Callable[[dict[str, Any]], Any],
        sleeper: Callable[[float], None],
    ) -> None:
        self.queue = queue
        self.emit = emit
        self.sleeper = sleeper
        self.prefetch_count = self._int(config, "prefetch_count", 1, 1, 1000)
        self.max_retries = self._int(config, "max_retries", 3, 0, 100)
        self.max_body_bytes = self._int(config, "max_body_bytes", 1048576, 1, 16777216)
        delay = config.get("retry_delay_seconds", 1)
        if isinstance(delay, bool) or not isinstance(delay, (int, float)) or delay < 0 or delay > 30:
            raise ValueError("retry_delay_seconds must be between 0 and 30")
        self.retry_delay = float(delay)
        self.deduplicate = config.get("deduplicate_message_ids", True)
        if not isinstance(self.deduplicate, bool):
            raise ValueError("deduplicate_message_ids must be a boolean")  # noqa: TRY004
        self._attempts: dict[str, int] = {}
        self._seen: OrderedDict[str, None] = OrderedDict()

    @staticmethod
    def _int(config: Mapping[str, Any], name: str, default: int, minimum: int, maximum: int) -> int:
        value = config.get(name, default)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
            raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
        return value

    def _identity(self, properties: Any) -> str | None:
        message_id = getattr(properties, "message_id", None)
        if isinstance(message_id, str) and message_id:
            return f"id:{self.queue}:{message_id}"
        return None

    def _payload(self, method: Any, properties: Any, body: bytes, retry_count: int) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "queue": self.queue,
            "exchange": str(getattr(method, "exchange", "")),
            "routing_key": str(getattr(method, "routing_key", "")),
            "consumer_tag": str(getattr(method, "consumer_tag", "")),
            "redelivered": bool(getattr(method, "redelivered", False)),
            "retry_count": retry_count,
            "body_base64": base64.b64encode(body).decode("ascii"),
            "body_size": len(body),
            "headers": _plain(getattr(properties, "headers", None) or {}),
        }
        try:
            payload["body_text"] = body.decode("utf-8")
        except UnicodeDecodeError:
            pass
        for name in (
            "content_type",
            "content_encoding",
            "correlation_id",
            "message_id",
            "type",
            "app_id",
            "reply_to",
            "timestamp",
        ):
            value = getattr(properties, name, None)
            if value is not None:
                payload[name] = _plain(value)
        return payload

    def handle(self, channel: Any, method: Any, properties: Any, body: bytes) -> None:
        delivery_tag = method.delivery_tag
        identity = self._identity(properties)
        if len(body) > self.max_body_bytes:
            if identity is not None:
                self._attempts.pop(identity, None)
            channel.basic_nack(delivery_tag=delivery_tag, multiple=False, requeue=False)
            return
        if self.deduplicate and identity is not None and identity in self._seen:
            self._seen.move_to_end(identity)
            self._attempts.pop(identity, None)
            channel.basic_ack(delivery_tag=delivery_tag, multiple=False)
            return
        broker_count, broker_counted = _broker_retry_state(properties)
        retry_count = max(self._attempts.get(identity, 0) if identity is not None else 0, broker_count)
        try:
            event_id = self.emit(self._payload(method, properties, body, retry_count))
            if event_id is None:
                raise RuntimeError("Attune event emission failed")
        except Exception:  # noqa: BLE001
            attempts = retry_count + 1
            can_count_safely = identity is not None or broker_counted
            if attempts <= self.max_retries or not can_count_safely:
                if identity is not None:
                    self._attempts[identity] = attempts
                if self.retry_delay:
                    self.sleeper(self.retry_delay)
                channel.basic_nack(delivery_tag=delivery_tag, multiple=False, requeue=True)
            else:
                if identity is not None:
                    self._attempts.pop(identity, None)
                channel.basic_nack(delivery_tag=delivery_tag, multiple=False, requeue=False)
            return
        if self.deduplicate and identity is not None:
            self._seen[identity] = None
            self._seen.move_to_end(identity)
            while len(self._seen) > 10000:
                self._seen.popitem(last=False)
        if identity is not None:
            self._attempts.pop(identity, None)
        # Ack is deliberately outside the emit failure handler. If it fails,
        # channel recovery must redeliver rather than attempting two settlements.
        channel.basic_ack(delivery_tag=delivery_tag, multiple=False)


class ConsumerWorker:
    def __init__(self, rule: Any, credentials: Mapping[str, Any], logger: Any, emit: Callable[[dict[str, Any]], Any]) -> None:
        self.rule = rule
        self.rule_id = int(getattr(rule, "rule_id", 0) or 0)
        self.config = dict(rule.trigger_params or {})
        self.credentials = dict(credentials)
        self.logger = logger
        self.queue = self.config.get("queue")
        if not isinstance(self.queue, str) or not self.queue:
            raise ValueError("queue must be a non-empty string")
        self.exclusive = self.config.get("exclusive", False)
        if not isinstance(self.exclusive, bool):
            raise ValueError("exclusive must be a boolean")  # noqa: TRY004
        self.handler = DeliveryHandler(self.queue, self.config, emit, self._sleep_connection)
        self._stop_event = threading.Event()
        self._connection: Any = None
        self._channel: Any = None
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name=f"rabbitmq-rule-{self.rule_id}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _sleep_connection(self, seconds: float) -> None:
        connection = self._connection
        if connection is not None and getattr(connection, "is_open", False):
            connection.sleep(seconds)
        else:
            self._stop_event.wait(seconds)

    def _request_stop(self) -> None:
        channel = self._channel
        if channel is not None and getattr(channel, "is_open", False):
            channel.stop_consuming()

    def stop(self) -> bool:
        self._stop_event.set()
        with self._lock:
            connection = self._connection
        if connection is not None and getattr(connection, "is_open", False):
            try:
                connection.add_callback_threadsafe(self._request_stop)
            except Exception:  # noqa: BLE001,S110
                pass
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            self.logger.warning("rule %s RabbitMQ consumer did not stop within 10 seconds", self.rule_id)
            return False
        return True

    def _consume_once(self) -> None:
        import pika

        connection = pika.BlockingConnection(connection_parameters(amqp_config(self.credentials)))
        with self._lock:
            self._connection = connection
        try:
            channel = connection.channel()
            self._channel = channel
            channel.queue_declare(queue=self.queue, passive=True)
            channel.basic_qos(prefetch_count=self.handler.prefetch_count, global_qos=False)
            cancelled = threading.Event()
            channel.add_on_cancel_callback(lambda _frame: cancelled.set())

            def callback(ch: Any, method: Any, properties: Any, body: bytes) -> None:
                self.handler.handle(ch, method, properties, body)

            channel.basic_consume(
                queue=self.queue,
                on_message_callback=callback,
                auto_ack=False,
                exclusive=self.exclusive,
                consumer_tag=f"attune-rabbitmq-{self.rule_id}",
            )
            if self._stop_event.is_set():
                return
            channel.start_consuming()
            if cancelled.is_set() and not self._stop_event.is_set():
                raise RuntimeError("RabbitMQ cancelled the consumer")
        finally:
            self._channel = None
            if getattr(connection, "is_open", False):
                try:
                    connection.close()
                except Exception:  # noqa: BLE001,S110
                    pass
            with self._lock:
                self._connection = None

    def _run(self) -> None:
        failures = 0
        while not self._stop_event.is_set():
            try:
                self._consume_once()
                failures = 0
                if not self._stop_event.is_set():
                    raise RuntimeError("RabbitMQ consumer stopped unexpectedly")
            except Exception as exc:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                failures += 1
                delay = min(60.0, float(2 ** min(failures - 1, 6)))
                self.logger.warning("rule %s RabbitMQ consumer failed: %s", self.rule_id, type(exc).__name__)
                self._stop_event.wait(delay)


def _production_sensor() -> type:
    import attune

    class RabbitMQMessageSensor(attune.Sensor):
        def __init__(self) -> None:
            super().__init__()
            self._workers: dict[int, ConsumerWorker] = {}
            self._lock = threading.Lock()

        @staticmethod
        def _rule_id(rule: Any) -> int:
            return int(getattr(rule, "rule_id", 0) or 0)

        def _stop(self, rule_id: int) -> bool:
            with self._lock:
                worker = self._workers.get(rule_id)
            if worker is None:
                return True
            stopped = worker.stop()
            if stopped:
                with self._lock:
                    if self._workers.get(rule_id) is worker:
                        self._workers.pop(rule_id, None)
            return stopped

        def _start(self, rule: Any) -> None:
            rule_id = self._rule_id(rule)
            if not self._stop(rule_id):
                raise RuntimeError("existing RabbitMQ consumer is still stopping")
            config = dict(rule.trigger_params or {})
            credentials = read_credentials_file(config.get("credential_file"))

            def emit(payload: dict[str, Any]) -> Any:
                return self.emit(payload, rule=rule, target_rule=True)

            worker = ConsumerWorker(rule, credentials, self.logger, emit)
            with self._lock:
                self._workers[rule_id] = worker
            worker.start()

        def on_rule_created(self, rule: Any) -> None:
            self._start(rule)

        def on_rule_enabled(self, rule: Any) -> None:
            self._start(rule)

        def on_rule_updated(self, rule: Any, old_params: dict[str, Any]) -> None:
            self._start(rule)

        def on_rule_disabled(self, rule: Any) -> None:
            self._stop(self._rule_id(rule))

        def on_rule_deleted(self, rule: Any) -> None:
            self._stop(self._rule_id(rule))

        def run(self) -> None:
            while not self.is_shutting_down:
                time.sleep(1)

        def cleanup(self) -> None:
            with self._lock:
                rule_ids = list(self._workers)
            for rule_id in rule_ids:
                self._stop(rule_id)

    return RabbitMQMessageSensor


def main() -> None:
    import attune

    attune.run_sensor(_production_sensor())


if __name__ == "__main__":
    main()
