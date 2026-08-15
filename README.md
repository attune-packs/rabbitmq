# RabbitMQ Attune Pack

Production-oriented RabbitMQ actions and message events for Attune. This is an
Apache-2.0 adaptation of
[`StackStorm-Exchange/stackstorm-rabbitmq`](https://github.com/StackStorm-Exchange/stackstorm-rabbitmq)
version 1.1.1 (`e79bcedf5cef611ff6088b79d9e5926a288fa951`).

## Scope And Requirements

- Python 3.10 or newer is required.
- Publishing and consumption use AMQP 0-9-1 through Pika. Inspection uses the
  RabbitMQ Management HTTP API directly; `rabbitmqadmin` is not required.
- The Management plugin must be enabled for inspection actions.
- Tests are deterministic and make no broker or network calls.
- Actions read a pack-owned encrypted Attune Key. The managed sensor reads a
  protected JSON file below `/run/secrets` because sensors cannot decrypt Keys.
- The implementation was reviewed against RabbitMQ 4.3 and current Pika docs
  on 2026-08-14. A live compatibility test remains required for every target
  RabbitMQ version, queue type, authentication backend, and proxy.

## Action Credentials

Create a pack-owned encrypted Key named `rabbitmq.credentials`. A complete
password-authenticated example is:

```json
{
  "amqp": {
    "host": "rabbitmq.example.net",
    "port": 5671,
    "vhost": "/production",
    "auth_method": "plain",
    "username": "attune-publisher",
    "password": "REDACTED",
    "tls": true,
    "verify_tls": true,
    "ca_file": "/run/secrets/rabbitmq/ca.pem",
    "heartbeat_seconds": 30,
    "socket_timeout_seconds": 10,
    "stack_timeout_seconds": 15,
    "blocked_timeout_seconds": 30,
    "connection_attempts": 1,
    "retry_delay_seconds": 1
  },
  "management": {
    "url": "https://rabbitmq.example.net:15671",
    "auth_method": "basic",
    "username": "attune-monitor",
    "password": "REDACTED",
    "verify_tls": true,
    "ca_file": "/run/secrets/rabbitmq/ca.pem",
    "timeout_seconds": 30
  }
}
```

TLS is enabled and verified by default. `ca_file` is optional when the broker's
certificate chains to the system trust store. For mTLS, set
`client_cert_file`, `client_key_file`, and optionally `client_key_password` in
`amqp`; choose AMQP `auth_method: external` when RabbitMQ's
`rabbitmq_auth_mechanism_ssl` plugin maps the client certificate identity. The
Management configuration accepts the same certificate and key field names and
`auth_method: mtls`. Disabling verification is supported only for controlled
diagnostics and is unsafe in production.

RabbitMQ OAuth 2.0 JWTs are supported with `auth_method: oauth2` and `token`.
For AMQP 0-9-1, the token is sent as the SASL PLAIN password and the username is
ignored by RabbitMQ, as documented by the OAuth backend. For Management API
calls it is sent as an HTTP Bearer token. The pack does not acquire or refresh
tokens; provision a valid token in the Key or sensor file and account for its
expiration. Plain AMQP credentials must only be used over verified TLS.

All numeric connection and request settings are bounded. Each action requests
only Attune's reserved `standard` scope to read its pack-owned Key. Credentials,
broker response bodies, URLs, and message bodies are never included in action
errors. RabbitMQ may still record usernames and client certificate identities
in its own logs.

## Actions

```bash
attune action execute rabbitmq.publish_message \
  --params-json '{"exchange":"events","routing_key":"deploy.complete","message":"{\"status\":\"ok\"}","content_type":"application/json","message_id":"deploy-123"}' --watch

attune action execute rabbitmq.list_queues \
  --params-json '{"vhost":"/production","page_size":100,"max_pages":20}' --watch

attune action execute rabbitmq.list_exchanges \
  --params-json '{"vhost":"/production","name":"events"}' --watch

attune action execute rabbitmq.list_bindings \
  --params-json '{"vhost":"/production","source_exchange":"events","destination_queue":"automation"}' --watch
```

`publish_message` does not declare or mutate topology. It enables publisher
confirms before publishing and uses `mandatory: true` by default so an
unroutable message fails. `persistent: true` sets delivery mode 2. Persistence
requires a durable target queue, and a confirmation only means the broker has
accepted responsibility, not that a consumer processed the message. If a
connection fails after acceptance but before the confirm reaches the action,
the outcome is unknown; retrying can duplicate the message. Use a stable
`message_id` and idempotent consumers. Payloads can be UTF-8 or base64 and are
limited to 16 MiB by the pack.

Inspection actions percent-encode every vhost and resource path segment,
including `/` as `%2F`, and disable redirects so credentials cannot be
forwarded unexpectedly. List endpoints request pages of at most 500 and stop at
`max_pages` (default 20); `result.truncated` reports a known remaining page.
Specific exchange/queue binding endpoints return one unpaginated list. The
configured Management `url` may contain a reverse-proxy prefix but no embedded
credentials, query, or fragment. Proxies must preserve encoded slashes.

## Sensor Setup

Mount a separate JSON credential file under `/run/secrets` on every sensor
worker. It must be a regular UTF-8 JSON file no larger than 64 KiB. It uses the
same `amqp` object as the action Key; Management credentials are unnecessary.
Keep it readable only by the sensor service account. Certificate paths and a
private-key password, if used, belong in this protected file. The sensor
deliberately cannot read an arbitrary path outside `/run/secrets`.

Create a rule for `rabbitmq.message` with parameters such as:

```json
{
  "credential_file": "/run/secrets/rabbitmq/consumer.json",
  "queue": "attune.events",
  "prefetch_count": 1,
  "max_retries": 3,
  "retry_delay_seconds": 1,
  "max_body_bytes": 1048576,
  "deduplicate_message_ids": true,
  "exclusive": false
}
```

The queue must already exist. The sensor makes a passive declaration and never
creates queues, exchanges, bindings, dead-letter exchanges, or policies. A
credentials-file or certificate rotation takes effect when the rule is
updated, disabled and enabled, or the sensor restarts. Do not place secrets in
trigger parameters: they are configuration metadata rather than protected Key
values.

## Delivery Semantics

The sensor is at-least-once, not exactly-once:

1. It consumes with `auto_ack=false`, applies bounded prefetch, emits only to
   the matching rule, then acknowledges that single delivery only after Attune
   returns a non-null event ID.
2. An emission failure nacks and requeues the delivery up to `max_retries` when
   a stable `message_id` or broker delivery counter makes the delivery safe to
   identify. The in-process count for message IDs is combined with RabbitMQ's
   `x-delivery-count` or `x-death` header when available. After the limit, the
   sensor nacks without requeue: the broker dead-letters the message if a DLX is
   configured, or discards it otherwise. Oversized bodies are terminally nacked
   immediately because retry cannot reduce their size.
3. Without a message ID or positive broker counter, failures are requeued
   instead of using a body hash that could conflate distinct identical
   messages. Such messages need a broker-enforced delivery limit to avoid a
   poison-message loop. The local retry count is lost on restart and is not
   shared by replicas. Configure a quorum queue delivery limit or a dead-letter
   retry topology for a durable policy. A sensor delay reduces tight local loops
   but is not a durable delayed-retry mechanism.
4. After successful emission, message IDs are retained in a 10,000-entry
   per-rule memory window. A redelivery with the same ID is acknowledged without
   another event. This closes the common emit-success/ack-loss window only while
   that worker remains alive. A crash in the window, another replica, or a
   message without an ID can still create duplicate events. Reusing a message
   ID for distinct messages can suppress a legitimate event; disable local
   deduplication if IDs are not unique.
5. On rule cancellation, the worker asks Pika's connection thread to cancel
   consumption, then closes the connection. Broker cancellation and connection
   failure also close the channel. Any unacknowledged delivery is requeued by
   RabbitMQ. A process kill has the same broker-side requeue behavior once the
   connection loss is detected.
6. One worker dispatches callbacks serially. `prefetch_count: 1`, one rule,
   one sensor replica, no priorities, and a single-active-consumer queue provide
   the strongest ordering. Requeue can move a failed message near the head,
   multiple consumers distribute deliveries, and retries can therefore reorder
   events. Each rule should normally use its own queue; two rules on one queue
   compete for messages rather than fan them out.

The event always contains base64 body data, body size, routing metadata,
headers, redelivery state, and retry count. Valid UTF-8 also appears as
`body_text`; no pickle or automatic JSON deserialization is performed. Headers
are normalized to JSON-safe values. Message bodies and headers may themselves
contain secrets and become Attune event data, so apply queue, rule, log
retention, and downstream access controls accordingly.

RabbitMQ acknowledgements are channel-scoped. The implementation always acks
or nacks on the delivery channel with `multiple=false`; it never acknowledges a
batch or acknowledges from a `finally` block. An acknowledgement can still be
lost after successful emission, which is why downstream actions must remain
idempotent.

## Source Fidelity

| Source resource | Attune target | Fidelity and differences |
|---|---|---|
| `publish_message` | `rabbitmq.publish_message` | AMQP publish retained; defaults, TLS, OAuth/EXTERNAL, bounded connection settings, confirms, mandatory routing, properties, base64, persistence, and safe errors added. Source topology declaration removed to avoid an implicit mutation. |
| `list_exchanges` | `rabbitmq.list_exchanges` | Replaced local `rabbitmqadmin` parsing with the current paginated HTTP API. |
| `list_queues` | `rabbitmq.list_queues` | Replaced local `rabbitmqadmin` parsing with vhost-scoped paginated HTTP inspection. |
| No source action | `rabbitmq.list_bindings` | Added useful exchange/queue binding inspection through documented HTTP endpoints. |
| Multi-queue polling sensor and embedded trigger | Managed `rabbitmq.rabbitmq_message` sensor and `rabbitmq.message` trigger | Rebuilt per active rule with long-lived consumers, passive queues, explicit settlement, bounded prefetch/retry, reconnect/cancellation handling, safe binary payloads, and protected file credentials. Unsafe pickle support removed. |
| Pack config | Encrypted action Key and protected sensor file | Secrets are no longer stored in pack configuration. |
| Rules, workflows, aliases, schedules | None | No such resources existed upstream. |

## Current API References

- [RabbitMQ 4.3 HTTP API reference](https://www.rabbitmq.com/docs/http-api-reference)
- [Management plugin, permissions, OAuth, HTTPS, and proxy behavior](https://www.rabbitmq.com/docs/management)
- [Consumer acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
- [Consumers, cancellation, prefetch, ordering, and acknowledgement timeout](https://www.rabbitmq.com/docs/consumers)
- [AMQP authentication and authorization](https://www.rabbitmq.com/docs/access-control)
- [OAuth 2.0 authentication backend](https://www.rabbitmq.com/docs/oauth2)
- [RabbitMQ TLS support](https://www.rabbitmq.com/docs/ssl)
- [Pika connection parameters](https://pika.readthedocs.io/en/stable/modules/parameters.html)

RabbitMQ's HTTP publish and queue-get endpoints are intentionally not used:
the current documentation identifies them as inefficient or troubleshooting
interfaces. Long-lived AMQP connections provide the required publish confirms,
manual acknowledgement, prefetch, redelivery, and cancellation semantics.

## Validation

```bash
python -m unittest discover -s tests -v
attune --output json pack check .
attune pack test . --detailed
```

These checks do not prove broker reachability, Management plugin availability,
permissions, certificate chains and hostname matching, proxy handling of `%2F`,
OAuth scopes/expiration, publisher-confirm ambiguity, DLX routing, queue-type
delivery limits, ordering under competing consumers, or recovery during broker
failover. Run those cases against a non-production cluster before release.

## License

The upstream and this adaptation are licensed under Apache License 2.0. See
[LICENSE](LICENSE), [NOTICE](NOTICE), and [SOURCE.md](SOURCE.md).
