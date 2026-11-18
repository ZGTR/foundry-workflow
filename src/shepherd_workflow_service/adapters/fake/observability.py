class RecordingMetrics:
    def __init__(self):
        self.samples = []

    def emit(self, values):
        self.samples.append(values)


class EmptyObservability:
    def metrics(self):
        return {}

    def history(self, job_id, principal, limit):
        return []
