"""SQS delivery adapter. Business retries remain in the workflow ledger."""

import json
import logging

from shepherd_contracts import PrincipalMinter

from shepherd_workflow_service.models.execution import EventEnvelope

logger = logging.getLogger(__name__)


class QueueExecutionConsumer:
    def __init__(self, *, client, queue_url, service, issuer, allowed_tenants):
        self.client = client
        self.queue_url = queue_url
        self.service = service
        self.minter = PrincipalMinter(issuer=issuer)
        self.allowed_tenants = frozenset(allowed_tenants)

    def poll(self):
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
        )
        failures = 0
        for message in response.get("Messages", []):
            try:
                wrapped = json.loads(message["Body"])
                event = EventEnvelope.model_validate_json(json.dumps(wrapped["detail"]))
                if (
                    wrapped["source"] != event.producer
                    or wrapped["detail-type"] != event.type
                    or event.tenant_id not in self.allowed_tenants
                ):
                    raise ValueError("untrusted event identity")
                # IAM restricts who can publish to the subscribed buses. Producer selectors
                # in definitions further restrict who can trigger/complete each stage.
                principal = self.minter.mint(
                    org_id=event.tenant_id, subject_id=event.producer, scopes=["jobs:write"]
                )
                self.service.receive(event, principal)
                self.client.delete_message(
                    QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"]
                )
            except Exception:
                failures += 1
                logger.exception("Workflow message failed: %s", message["MessageId"])
                count = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
                waits = (30, 120, 600, 1800, 3600, 7200)
                self.client.change_message_visibility(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                    VisibilityTimeout=waits[min(count - 1, len(waits) - 1)],
                )
        return failures
