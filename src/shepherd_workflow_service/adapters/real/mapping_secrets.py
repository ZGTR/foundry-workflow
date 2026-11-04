"""A `SecretsPort` over an explicit mapping supplied at construction.

REAL, not a fake: local and CI runs need genuine credentials from somewhere,
and reading them from a mapping the composition root passed in is a legitimate
production-shaped source (a mounted file, a task-definition environment).

What makes it real rather than a stub is that it does NOT read `os.environ`
itself. `config.py` is the single place in this package allowed to touch the
environment, so the caller decides what this adapter can see -- which is the
difference between a service that can be configured and one that picks up
whatever happens to be exported.
"""

from __future__ import annotations

from collections.abc import Mapping

from shepherd_workflow_service.ports.secrets import SecretNotFound


class MappingSecrets:
    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)

    def get(self, name: str) -> str:
        try:
            return self._values[name]
        except KeyError:
            raise SecretNotFound(
                f"secret {name!r} is not present in the supplied mapping"
            ) from None
