#!/usr/bin/env python3
"""Shared stdin/JSON entry point for RabbitMQ actions."""

from __future__ import annotations

import json
import os
import sys

_PACK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PACK_ROOT not in sys.path:
    sys.path.insert(0, _PACK_ROOT)

from lib.rabbitmq_client import RabbitMQPackError, execute_action


def main() -> int:
    try:
        raw = sys.stdin.read()
        params = json.loads(raw) if raw.strip() else {}
        if not isinstance(params, dict):
            raise RabbitMQPackError("action parameters must be a JSON object")
        operation = os.environ.get("ATTUNE_ACTION", "").rsplit(".", 1)[-1]
        result = execute_action(operation, params)
        json.dump({"operation": operation, "result": result}, sys.stdout, separators=(",", ":"))
        sys.stdout.write("\n")
        return 0
    except (RabbitMQPackError, ValueError, TypeError, OSError) as exc:
        print(f"rabbitmq action failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    # Keep unexpected SDK/client exceptions opaque because they can embed credentials.
    except Exception as exc:  # noqa: BLE001
        print(f"rabbitmq action failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
