"""ECS worker with independent intake, recovery, publishing and telemetry."""
import logging
import time

from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container, build_execution_cycle

logger = logging.getLogger(__name__)


def main():
    cycle = build_execution_cycle(build_container(ServiceConfig.from_environment()))
    while True:
        try:
            failures = cycle.run()
        except Exception:
            logger.exception("Workflow cycle telemetry failed")
            failures = 1
        if failures:
            time.sleep(5)


if __name__ == "__main__":
    main()
