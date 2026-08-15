# Source Metadata

- Reference project: `StackStorm-Exchange/stackstorm-rabbitmq`
- Source URL: https://github.com/StackStorm-Exchange/stackstorm-rabbitmq
- Source release: `v1.1.1`
- Source revision: `e79bcedf5cef611ff6088b79d9e5926a288fa951`
- Source revision date: 2022-12-18
- Source license: Apache License 2.0
- Translation date: 2026-08-14

The source release and commit were verified from the upstream Git repository.
The upstream `LICENSE` is the unmodified Apache License 2.0 reproduced in this
repository. The source had three actions (`publish_message`, `list_exchanges`,
and `list_queues`) and one multi-queue sensor/embedded trigger. It had no rules,
workflows, aliases, schedules, or policies.

Current behavior was checked against RabbitMQ 4.3 documentation for the AMQP
0-9-1 authentication mechanisms, OAuth 2.0 backend, TLS, consumers,
acknowledgements, publisher confirms, and Management HTTP API. Pika's current
connection, blocking-consumer, acknowledgement, and confirmation interfaces
were also checked. See the README for links and material behavior differences.
