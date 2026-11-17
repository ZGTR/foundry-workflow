"""Lazy catalog loading shared by runtime and the IaC subscription compiler."""
from pathlib import Path

from shepherd_contracts.errors import ConfigurationError

from shepherd_workflow_service.models.definition import Catalog


class JsonWorkflowCatalog:
    def __init__(self, path: str):
        self.path = path
        self._catalog = None

    def load(self) -> Catalog:
        if self._catalog is None:
            try:
                self._catalog = Catalog.model_validate_json(Path(self.path).read_text())
            except (ValueError, OSError) as exc:
                raise ConfigurationError(f"Invalid workflow catalog: {exc}") from exc
        return self._catalog
