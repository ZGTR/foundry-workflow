"""CloudWatch Embedded Metric Format via the existing task log stream."""
import json


class EmfMetrics:
    def __init__(self, *, stream, service, environment, clock):
        self.stream = stream
        self.service = service
        self.environment = environment
        self.clock = clock

    def emit(self, values):
        definitions = [
            {"Name": name, "Unit": "Seconds" if name.endswith("Seconds") else "Count"}
            for name in values
        ]
        self.stream.write(json.dumps({
            "_aws": {
                "Timestamp": int(self.clock.now().timestamp() * 1000),
                "CloudWatchMetrics": [{
                    "Namespace": "Foundry/Workflow",
                    "Dimensions": [["Service", "Environment"]],
                    "Metrics": definitions,
                }],
            },
            "Service": self.service, "Environment": self.environment, **values,
        }) + "\n")
        self.stream.flush()
