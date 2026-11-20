"""ECS worker with independent intake, recovery, publishing and telemetry."""
import logging
import signal
import time
from threading import Event

from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container, build_execution_cycle

logger = logging.getLogger(__name__)


def main():
    config = ServiceConfig.from_environment()
    cycle = build_execution_cycle(build_container(config))
    if config.tenant_http_profile:
        stop = Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        cycle.run_forever(stop)
        return
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
