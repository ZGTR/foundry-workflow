"""Recording execution repository for application service tests."""
class RecordingExecutionRepository:
    def __init__(self):
        self.events = []
        self.ticks = []

    def check_ready(self):
        pass

    def receive(self, event, principal):
        self.events.append((event, principal))
        return []

    def tick(self, limit=100):
        self.ticks.append(limit)
        return 0


class RecordingExecutionConsumer:
    def __init__(self, failures=0):
        self.failures = failures
        self.polls = 0

    def poll(self):
        self.polls += 1
        return self.failures
