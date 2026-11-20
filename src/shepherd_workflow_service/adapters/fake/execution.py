"""Recording execution repository for application service tests."""
class RecordingExecutionRepository:
    def __init__(self):
        self.events = []
        self.ticks = []
        self.admissions = []
        self.views = {}

    def check_ready(self):
        pass

    def receive(self, event, principal):
        self.events.append((event, principal))
        return []

    def tick(self, limit=100):
        self.ticks.append(limit)
        return 0

    def admit(self, limit=100):
        self.admissions.append(limit)
        return 0

    def scheduler_metrics(self):
        return {}

    def owns_job(self, job_id, principal):
        return bool(self.stages(job_id, principal))

    def get(self, stage_run_id, principal):
        return self.views.get((principal.org_id, stage_run_id))

    def stages(self, job_id, principal):
        return [
            view for (owner, _), view in self.views.items()
            if owner == principal.org_id and view.job_id == job_id
        ]


class RecordingExecutionConsumer:
    def __init__(self, failures=0):
        self.failures = failures
        self.polls = 0

    def poll(self):
        self.polls += 1
        return self.failures
