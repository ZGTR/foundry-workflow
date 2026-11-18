"""Envelope-neutral EventBridge transport for the ledger's existing outbox."""

import json


class EventBridgeBroker:
    def __init__(self, *, client, bus_name, producer, clock):
        self.client = client
        self.bus_name = bus_name
        self.producer = producer
        self.clock = clock

    def publish(self, *, key, event_class, payload):
        # Verify the bus before sending: PutEvents can otherwise acknowledge a missing bus.
        self.client.describe_event_bus(Name=self.bus_name)
        envelope = payload.get("envelope") or {
            "specversion": "workflow.event/v1",
            "event_id": key,
            "type": event_class,
            "producer": self.producer,
            "tenant_id": payload["org_id"],
            "occurred_at": payload.get("occurred_at"),
            "data": payload,
            "output_ref": payload.get("output_ref"),
        }
        detail = json.dumps(envelope, separators=(",", ":"))
        if len(detail.encode()) > 240_000:
            raise ValueError("workflow event too large; use an artifact reference")
        result = self.client.put_events(
            Entries=[
                {
                    "EventBusName": self.bus_name,
                    "Source": self.producer,
                    "DetailType": event_class,
                    "Detail": detail,
                }
            ]
        )
        if result.get("FailedEntryCount", 0):
            raise RuntimeError(f"EventBridge refused event: {result['Entries']}")
