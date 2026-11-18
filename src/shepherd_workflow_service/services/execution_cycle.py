"""Fault-isolated worker cycle: intake failure must not stop timeout recovery."""
import logging

logger = logging.getLogger(__name__)


class ExecutionCycle:
    def __init__(self, *, consumer, service, drain, observations, metrics):
        self.consumer = consumer
        self.service = service
        self.drain = drain
        self.observations = observations
        self.metrics = metrics

    def run(self):
        failures = 0
        for name, action in (
            ("consume", self.consumer.poll),
            ("recover", self.service.tick),
            ("publish", self.drain),
        ):
            try:
                result = action()
                if name == "consume" and isinstance(result, int):
                    failures += result
                if name == "publish" and (result.failed or result.ambiguous):
                    failures += 1
            except Exception:
                failures += 1
                logger.exception("Workflow cycle operation failed: %s", name)
        values = {"WorkerHeartbeat": 1, "WorkerCycleFailures": failures}
        try:
            values.update(self.observations.metrics())
        except Exception:
            values["WorkerCycleFailures"] += 1
            logger.exception("Workflow metrics read failed")
        self.metrics.emit(values)
        return values["WorkerCycleFailures"]
