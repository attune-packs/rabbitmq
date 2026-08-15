"""Safe RabbitMQ AMQP and Management HTTP API clients.

Adapted from StackStorm Exchange's Apache-2.0 rabbitmq pack version 1.1.1.
"""

from __future__ import annotations

import base64
import json
import math
import ssl
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit


class RabbitMQPackError(RuntimeError):
    """Operator-facing error that does not include remote response content."""


def fetch_key(ref: str) -> dict[str, Any]:
    if not isinstance(ref, str) or not ref:
        raise RabbitMQPackError("credential_key must be a non-empty string")
    try:
        import attune
        from attune.api_client.api.secrets import get_key
    except ImportError as exc:
        raise RabbitMQPackError("attune-sdk is required to resolve credential_key") from exc
    try:
        response = get_key.sync_detailed(ref, client=attune.context.client, decrypt=True)
    except Exception as exc:
        raise RabbitMQPackError(f"unable to read credential Key {ref!r}") from exc
    status = int(response.status_code)
    if status == 404:
        raise RabbitMQPackError(f"credential Key {ref!r} was not found")
    if status >= 400 or not response.parsed:
        raise RabbitMQPackError(f"credential Key lookup failed with status {status}")
    value = response.parsed.data.value
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RabbitMQPackError("credential Key must contain a JSON object") from exc
    if not isinstance(value, dict):
        raise RabbitMQPackError("credential Key must contain an object")
    return value


def _number(config: Mapping[str, Any], name: str, default: float, minimum: float, maximum: float) -> float:
    value = config.get(name, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RabbitMQPackError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result < minimum or result > maximum:
        raise RabbitMQPackError(f"{name} must be between {minimum:g} and {maximum:g}")
    return result


def _integer(config: Mapping[str, Any], name: str, default: int, minimum: int, maximum: int) -> int:
    value = config.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        raise RabbitMQPackError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def amqp_config(credentials: Mapping[str, Any]) -> dict[str, Any]:
    value = credentials.get("amqp", credentials)
    if not isinstance(value, dict):
        raise RabbitMQPackError("credential Key amqp value must be an object")
    return dict(value)


def _ssl_context(config: Mapping[str, Any]) -> Any:
    tls = config.get("tls", True)
    verify = config.get("verify_tls", True)
    if not isinstance(tls, bool) or not isinstance(verify, bool):
        raise RabbitMQPackError("tls and verify_tls must be booleans")
    if not tls:
        return None
    ca_file = config.get("ca_file")
    if ca_file is not None and not isinstance(ca_file, str):
        raise RabbitMQPackError("ca_file must be a string")
    try:
        if verify:
            context = ssl.create_default_context(cafile=ca_file)
        else:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        cert_file = config.get("client_cert_file")
        key_file = config.get("client_key_file")
        if (cert_file is None) != (key_file is None):
            raise RabbitMQPackError("client_cert_file and client_key_file must be provided together")
        if cert_file is not None:
            if not isinstance(cert_file, str) or not isinstance(key_file, str):
                raise RabbitMQPackError("client certificate paths must be strings")
            context.load_cert_chain(cert_file, key_file, config.get("client_key_password"))
        return context
    except RabbitMQPackError:
        raise
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise RabbitMQPackError("unable to configure AMQP TLS credentials") from exc


def connection_parameters(config: Mapping[str, Any]) -> Any:
    try:
        import pika
    except ImportError as exc:
        raise RabbitMQPackError("pika is not installed") from exc
    host = config.get("host")
    vhost = config.get("vhost", "/")
    if not isinstance(host, str) or not host:
        raise RabbitMQPackError("AMQP credentials require host")
    if not isinstance(vhost, str) or not vhost:
        raise RabbitMQPackError("vhost must be a non-empty string")
    tls = config.get("tls", True)
    port = _integer(config, "port", 5671 if tls else 5672, 1, 65535)
    auth_method = config.get("auth_method", "plain")
    if auth_method == "plain":
        username, password = config.get("username"), config.get("password")
        if not isinstance(username, str) or not username or not isinstance(password, str):
            raise RabbitMQPackError("plain authentication requires username and password")
        credentials = pika.PlainCredentials(username, password)
    elif auth_method == "oauth2":
        token = config.get("token")
        if not isinstance(token, str) or not token:
            raise RabbitMQPackError("oauth2 authentication requires token")
        credentials = pika.PlainCredentials("", token)
    elif auth_method == "external":
        if not tls:
            raise RabbitMQPackError("external authentication requires TLS")
        if not config.get("client_cert_file") or not config.get("client_key_file"):
            raise RabbitMQPackError("external authentication requires a client certificate and key")
        credentials = pika.credentials.ExternalCredentials()
    else:
        raise RabbitMQPackError("auth_method must be plain, oauth2, or external")
    socket_timeout = _number(config, "socket_timeout_seconds", 10, 1, 300)
    stack_timeout = _number(config, "stack_timeout_seconds", 15, 1, 300)
    if stack_timeout < socket_timeout:
        raise RabbitMQPackError("stack_timeout_seconds must be greater than or equal to socket_timeout_seconds")
    context = _ssl_context(config)
    return pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=credentials,
        heartbeat=_integer(config, "heartbeat_seconds", 30, 5, 600),
        connection_attempts=_integer(config, "connection_attempts", 1, 1, 10),
        retry_delay=_number(config, "retry_delay_seconds", 1, 0, 30),
        socket_timeout=socket_timeout,
        stack_timeout=stack_timeout,
        blocked_connection_timeout=_number(config, "blocked_timeout_seconds", 30, 1, 300),
        ssl_options=pika.SSLOptions(context, host) if context is not None else None,
        client_properties={"connection_name": "attune-rabbitmq"},
    )


def _management_config(credentials: Mapping[str, Any]) -> dict[str, Any]:
    value = credentials.get("management")
    if not isinstance(value, dict):
        raise RabbitMQPackError("credential Key requires a management object")
    return dict(value)


def _management_url(config: Mapping[str, Any], segments: list[str]) -> str:
    value = config.get("url")
    parsed = urlsplit(value) if isinstance(value, str) else None
    if (
        parsed is None
        or parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise RabbitMQPackError("management.url must be an http(s) URL without credentials or a query")
    path = parsed.path.rstrip("/") + "/api/" + "/".join(quote(segment, safe="") for segment in segments)
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def management_request(credentials: Mapping[str, Any], segments: list[str], params: Mapping[str, Any] | None = None) -> Any:
    try:
        import requests
    except ImportError as exc:
        raise RabbitMQPackError("requests is not installed") from exc
    config = _management_config(credentials)
    url = _management_url(config, segments)
    auth_method = config.get("auth_method", "basic")
    headers = {"Accept": "application/json"}
    auth = None
    if auth_method == "basic":
        username, password = config.get("username"), config.get("password")
        if not isinstance(username, str) or not username or not isinstance(password, str):
            raise RabbitMQPackError("management basic authentication requires username and password")
        auth = (username, password)
    elif auth_method == "oauth2":
        token = config.get("token")
        if not isinstance(token, str) or not token:
            raise RabbitMQPackError("management oauth2 authentication requires token")
        headers["Authorization"] = f"Bearer {token}"
    elif auth_method != "mtls":
        raise RabbitMQPackError("management auth_method must be basic, oauth2, or mtls")
    verify_tls = config.get("verify_tls", True)
    if not isinstance(verify_tls, bool):
        raise RabbitMQPackError("management verify_tls must be a boolean")
    verify: bool | str = verify_tls
    if config.get("ca_file") is not None:
        if not verify_tls or not isinstance(config["ca_file"], str):
            raise RabbitMQPackError("management ca_file requires verify_tls and must be a string")
        verify = config["ca_file"]
    cert_file, key_file = config.get("client_cert_file"), config.get("client_key_file")
    if (cert_file is None) != (key_file is None):
        raise RabbitMQPackError("management client certificate and key must be provided together")
    if cert_file is not None and (not isinstance(cert_file, str) or not isinstance(key_file, str)):
        raise RabbitMQPackError("management client certificate paths must be strings")
    if auth_method == "mtls" and cert_file is None:
        raise RabbitMQPackError("management mtls authentication requires a client certificate and key")
    timeout = _number(config, "timeout_seconds", 30, 1, 300)
    try:
        response = requests.get(
            url,
            params=dict(params or {}),
            headers=headers,
            auth=auth,
            verify=verify,
            cert=(cert_file, key_file) if cert_file is not None else None,
            timeout=(timeout, timeout),
            allow_redirects=False,
        )
    except Exception as exc:
        raise RabbitMQPackError(f"RabbitMQ Management API request failed ({type(exc).__name__})") from exc
    if response.status_code < 200 or response.status_code >= 300:
        raise RabbitMQPackError(f"RabbitMQ Management API returned HTTP status {response.status_code}")
    try:
        return response.json()
    except (ValueError, TypeError) as exc:
        raise RabbitMQPackError("RabbitMQ Management API returned invalid JSON") from exc


def _list_paginated(credentials: Mapping[str, Any], segments: list[str], params: Mapping[str, Any]) -> dict[str, Any]:
    page_size = _integer(params, "page_size", 100, 1, 500)
    max_pages = _integer(params, "max_pages", 20, 1, 100)
    query: dict[str, Any] = {"page": 1, "page_size": page_size}
    for name in ("name", "use_regex", "disable_stats", "enable_queue_totals"):
        if name in params:
            value = params[name]
            query[name] = str(value).lower() if isinstance(value, bool) else value
    items: list[Any] = []
    pages = 0
    truncated = False
    while pages < max_pages:
        value = management_request(credentials, segments, query)
        pages += 1
        if isinstance(value, list):
            items.extend(value)
            break
        if not isinstance(value, dict) or not isinstance(value.get("items"), list):
            raise RabbitMQPackError("RabbitMQ Management API returned an unexpected listing")
        items.extend(value["items"])
        page_count = value.get("page_count")
        if not isinstance(page_count, int) or query["page"] >= page_count:
            break
        if pages == max_pages:
            truncated = True
            break
        query["page"] += 1
    return {"items": items, "count": len(items), "pages": pages, "truncated": truncated}


def list_exchanges(params: Mapping[str, Any]) -> dict[str, Any]:
    credentials = fetch_key(str(params.get("credential_key", "rabbitmq.credentials")))
    vhost = params.get("vhost", "/")
    if not isinstance(vhost, str) or not vhost:
        raise RabbitMQPackError("vhost must be a non-empty string")
    query = dict(params)
    query.setdefault("disable_stats", True)
    return _list_paginated(credentials, ["exchanges", vhost], query)


def list_queues(params: Mapping[str, Any]) -> dict[str, Any]:
    credentials = fetch_key(str(params.get("credential_key", "rabbitmq.credentials")))
    vhost = params.get("vhost", "/")
    if not isinstance(vhost, str) or not vhost:
        raise RabbitMQPackError("vhost must be a non-empty string")
    query = dict(params)
    query.setdefault("disable_stats", False)
    return _list_paginated(credentials, ["queues", vhost], query)


def list_bindings(params: Mapping[str, Any]) -> dict[str, Any]:
    credentials = fetch_key(str(params.get("credential_key", "rabbitmq.credentials")))
    vhost = params.get("vhost", "/")
    source, queue = params.get("source_exchange"), params.get("destination_queue")
    for name, value in (("vhost", vhost), ("source_exchange", source), ("destination_queue", queue)):
        if value is not None and (not isinstance(value, str) or (name == "vhost" and not value)):
            raise RabbitMQPackError(f"{name} must be a string")
    if source is not None and queue is not None:
        segments = ["bindings", vhost, "e", source, "q", queue]
    elif source is not None:
        segments = ["exchanges", vhost, source, "bindings", "source"]
    elif queue is not None:
        segments = ["queues", vhost, queue, "bindings"]
    else:
        return _list_paginated(credentials, ["bindings", vhost], params)
    value = management_request(credentials, segments)
    if not isinstance(value, list):
        raise RabbitMQPackError("RabbitMQ Management API returned an unexpected binding listing")
    return {"items": value, "count": len(value), "pages": 1, "truncated": False}


def _message_body(params: Mapping[str, Any]) -> bytes:
    message = params.get("message")
    if not isinstance(message, str):
        raise RabbitMQPackError("message must be a string")
    encoding = params.get("payload_encoding", "utf-8")
    if encoding not in {"utf-8", "base64"}:
        raise RabbitMQPackError("payload_encoding must be utf-8 or base64")
    try:
        body = message.encode("utf-8") if encoding == "utf-8" else base64.b64decode(message, validate=True)
    except (ValueError, UnicodeError) as exc:
        raise RabbitMQPackError("message is not valid for payload_encoding") from exc
    if len(body) > 16 * 1024 * 1024:
        raise RabbitMQPackError("message exceeds the 16 MiB action limit")
    return body


def publish_message(params: Mapping[str, Any]) -> dict[str, Any]:
    try:
        import pika
    except ImportError as exc:
        raise RabbitMQPackError("pika is not installed") from exc
    credentials = fetch_key(str(params.get("credential_key", "rabbitmq.credentials")))
    exchange, routing_key = params.get("exchange", ""), params.get("routing_key", "")
    if not isinstance(exchange, str) or not isinstance(routing_key, str):
        raise RabbitMQPackError("exchange and routing_key must be strings")
    headers = params.get("headers", {})
    if not isinstance(headers, dict):
        raise RabbitMQPackError("headers must be an object")
    persistent = params.get("persistent", True)
    mandatory = params.get("mandatory", True)
    if not isinstance(persistent, bool) or not isinstance(mandatory, bool):
        raise RabbitMQPackError("persistent and mandatory must be booleans")
    properties_values = {
        name: params[name]
        for name in ("content_type", "content_encoding", "correlation_id", "message_id", "type", "app_id", "reply_to", "expiration")
        if params.get(name) is not None
    }
    if any(not isinstance(value, str) for value in properties_values.values()):
        raise RabbitMQPackError("message property values must be strings")
    body = _message_body(params)
    properties = pika.BasicProperties(headers=headers, delivery_mode=2 if persistent else 1, **properties_values)
    connection = None
    try:
        connection = pika.BlockingConnection(connection_parameters(amqp_config(credentials)))
        channel = connection.channel()
        channel.confirm_delivery()
        confirmed = channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties,
            mandatory=mandatory,
        )
        if confirmed is False:
            raise RabbitMQPackError("RabbitMQ negatively acknowledged the publish")
        return {"confirmed": True, "persistent": persistent, "mandatory": mandatory}
    except RabbitMQPackError:
        raise
    except Exception as exc:
        raise RabbitMQPackError(f"RabbitMQ publish failed ({type(exc).__name__})") from exc
    finally:
        if connection is not None and getattr(connection, "is_open", False):
            try:
                connection.close()
            except Exception:  # noqa: BLE001,S110
                pass


OPERATIONS = {
    "publish_message": publish_message,
    "list_exchanges": list_exchanges,
    "list_queues": list_queues,
    "list_bindings": list_bindings,
}


def execute_action(operation: str, params: Mapping[str, Any]) -> dict[str, Any]:
    function = OPERATIONS.get(operation)
    if function is None:
        raise RabbitMQPackError("unknown RabbitMQ action")
    return function(params)
