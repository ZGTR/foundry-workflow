"""Durable multi-stage job execution, with the decision separated from the storage.

``reduce()`` is pure and decides; a repository the caller supplies persists the
plan inside the caller's own transaction. The package owns no stage names: the
workflow arrives as a ``WorkflowDefinitionV1`` value.
"""

from shepherd_workflow.commands import CommandKind, CommandV1, LedgerSnapshotV1
from shepherd_workflow.dispatcher import Broker, DispatchReport, OutboxDispatcher
from shepherd_workflow.memory import InMemoryOutbox, InMemoryStageRepository, InMemoryUnitOfWork
from shepherd_workflow.ports import OWNED_TABLES, ConcurrentUpdate, OutboxSink, StageRepository
from shepherd_workflow.reducer import TransitionPlanV1, artifact_digest, dispatch_key, reduce
from shepherd_workflow.runner import ApplyResult, StageRunner
from shepherd_workflow.state import TERMINAL_STATES, RefusalClass, StageStatus, is_terminal
from shepherd_workflow.workflow import StageDefinitionV1, WorkflowDefinitionV1

__all__ = [
    "OWNED_TABLES",
    "TERMINAL_STATES",
    "ApplyResult",
    "Broker",
    "CommandKind",
    "CommandV1",
    "ConcurrentUpdate",
    "DispatchReport",
    "InMemoryOutbox",
    "InMemoryStageRepository",
    "InMemoryUnitOfWork",
    "LedgerSnapshotV1",
    "OutboxDispatcher",
    "OutboxSink",
    "StageRepository",
    "StageRunner",
    "RefusalClass",
    "StageDefinitionV1",
    "StageStatus",
    "TransitionPlanV1",
    "WorkflowDefinitionV1",
    "artifact_digest",
    "dispatch_key",
    "is_terminal",
    "reduce",
]
