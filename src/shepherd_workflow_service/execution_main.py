"""ECS worker: consume, recover due work, then drain the durable outbox."""
import logging

from shepherd_contracts.errors import ConfigurationError

from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container

logger = logging.getLogger(__name__)


def main():
    config = ServiceConfig.from_environment()
    container = build_container(config)
    if container.execution_consumer_factory is None or container.execution_service is None:
        raise ConfigurationError("execution worker requires WORKFLOW_CATALOG_PATH")
    consumer = container.execution_consumer_factory()
    while True:
        try:
            consumer.poll()
            container.execution_service.tick()
            container.drain_ledger_outbox()
        except Exception:
            # Polling uses long polling; failures back off without killing recovery forever.
            logger.exception("Workflow execution cycle failed")
            import time
            time.sleep(5)


if __name__ == "__main__":
    main()
