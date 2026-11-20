"""Fault-isolated worker cycle: intake failure must not stop timeout recovery."""
import logging
from threading import Event, Lock, Thread

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


class ScheduledExecutionCycle:
    """Recovery, admission and network delivery each get their own work loop."""

    def __init__(self, *, service, drain, observations, metrics):
        self.service = service
        self.drain = drain
        self.observations = observations
        self.metrics = metrics
        self._failures = 0
        self._lock = Lock()

    def _failed(self, count=1):
        with self._lock:
            self._failures += count

    def _publish(self):
        report = self.drain(batch_size=16)
        self._failed(report.failed + report.ambiguous)

    def _loop(self, stop, name, action, interval):
        while not stop.is_set():
            try:
                action()
            except Exception:
                self._failed()
                logger.exception("Workflow worker operation failed: %s", name)
                stop.wait(5)
            else:
                stop.wait(interval)

    def _observe(self):
        with self._lock:
            failures, self._failures = self._failures, 0
        values = {"WorkerHeartbeat": 1, "WorkerCycleFailures": failures}
        try:
            values.update(self.observations.metrics())
            values.update(self.service.scheduler_metrics())
        except Exception:
            values["WorkerCycleFailures"] += 1
            logger.exception("Workflow metrics read failed")
        self.metrics.emit(values)

    def run_forever(self, stop: Event):
        workers = [
            Thread(target=self._loop, args=(stop, name, action, interval), daemon=True)
            for name, action, interval in (
                ("recover", self.service.tick, 1),
                ("admit", self.service.admit, 0.25),
                ("publish", self._publish, 0.25),
            )
        ]
        for worker in workers:
            worker.start()
        self._loop(stop, "observe", self._observe, 10)
