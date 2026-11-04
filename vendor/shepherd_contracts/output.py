"""Versioned, allowlisted structured-output contracts."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal

import pydantic
from pydantic import BaseModel, ConfigDict, RootModel, TypeAdapter, ValidationError

from shepherd_contracts.canon import canonical_digest
from shepherd_contracts.errors import ShepherdError

EnforcementMode = Literal["local_only", "provider_and_local"]
_ID = re.compile(r"^[a-z][a-z0-9.-]{0,127}$")
_UNSUPPORTED_KEYWORDS = frozenset(
    {
        "exclusiveMaximum",
        "exclusiveMinimum",
        "maxItems",
        "maxLength",
        "maximum",
        "minLength",
        "minimum",
        "multipleOf",
        "pattern",
        "uniqueItems",
    }
)


class OutputContractUnsupported(ShepherdError):
    """A contract identity or schema is not admitted."""


class OutputValidationFailed(ShepherdError):
    """Provider bytes did not satisfy their pinned local contract."""


class StrictOutputModel(BaseModel):
    """Base for models whose JSON objects reject undeclared fields."""

    model_config = ConfigDict(extra="forbid", strict=True)


class _BlogOutlineArgumentsV1(StrictOutputModel):
    title: str
    excerpt: str
    category: str
    tags: list[str]
    headings: list[str]


class _BlogSectionArgumentsV1(StrictOutputModel):
    heading: str
    body: str


class _BlogFinishArgumentsV1(StrictOutputModel):
    pass


class _BlogOutlineTurnV1(StrictOutputModel):
    protocol: Literal["shepherd.agent.turn.v1"]
    kind: Literal["tool_call"]
    name: Literal["record_outline"]
    arguments: _BlogOutlineArgumentsV1


class _BlogSectionTurnV1(StrictOutputModel):
    protocol: Literal["shepherd.agent.turn.v1"]
    kind: Literal["tool_call"]
    name: Literal["record_section"]
    arguments: _BlogSectionArgumentsV1


class _BlogFinishTurnV1(StrictOutputModel):
    protocol: Literal["shepherd.agent.turn.v1"]
    kind: Literal["tool_call"]
    name: Literal["finish_blog"]
    arguments: _BlogFinishArgumentsV1


class ContentBlogTurnOutputV1(
    RootModel[_BlogOutlineTurnV1 | _BlogSectionTurnV1 | _BlogFinishTurnV1]
):
    """Provider-constrained turn protocol for the registered blog capability."""


@dataclass(frozen=True, slots=True)
class OutputContractV1:
    contract_id: str
    version: int
    model: type[BaseModel]
    enforcement_mode: EnforcementMode
    pydantic_version: str = field(init=False)
    schema_digest: str = field(init=False)
    _schema: Mapping[str, object] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not _ID.fullmatch(self.contract_id) or self.version <= 0:
            raise OutputContractUnsupported("OUTPUT_CONTRACT_UNKNOWN")
        schema = self.model.model_json_schema(mode="validation")
        _validate_supported_schema(schema)
        version = pydantic.VERSION
        object.__setattr__(self, "pydantic_version", version)
        object.__setattr__(self, "_schema", MappingProxyType(deepcopy(schema)))
        object.__setattr__(
            self,
            "schema_digest",
            canonical_digest({"pydantic_version": version, "schema": schema}),
        )

    @property
    def schema(self) -> dict[str, object]:
        return deepcopy(dict(self._schema))

    def validate_json(self, payload: bytes | str) -> BaseModel:
        try:
            return TypeAdapter(self.model).validate_json(payload, strict=True)
        except (ValidationError, ValueError, TypeError) as error:
            raise OutputValidationFailed("OUTPUT_VALIDATION_FAILED") from error


class OutputContractRegistry:
    def __init__(self, contracts: Iterable[OutputContractV1]) -> None:
        values: dict[tuple[str, int], OutputContractV1] = {}
        for contract in contracts:
            key = (contract.contract_id, contract.version)
            if key in values:
                raise OutputContractUnsupported("OUTPUT_CONTRACT_DUPLICATE")
            values[key] = contract
        self._contracts = MappingProxyType(values)

    def resolve(self, contract_id: str, version: int) -> OutputContractV1:
        try:
            return self._contracts[(contract_id, version)]
        except KeyError as error:
            raise OutputContractUnsupported("OUTPUT_CONTRACT_UNKNOWN") from error


def _validate_supported_schema(schema: object) -> None:
    definitions = schema.get("$defs", {}) if isinstance(schema, Mapping) else {}
    refs: dict[str, set[str]] = {}

    def walk(value: object, owner: str = "#") -> None:
        if isinstance(value, Mapping):
            if _UNSUPPORTED_KEYWORDS.intersection(value) or value.get("minItems") not in (
                None,
                0,
                1,
            ):
                raise OutputContractUnsupported("OUTPUT_SCHEMA_UNSUPPORTED")
            if value.get("type") == "object" and value.get("additionalProperties") is not False:
                raise OutputContractUnsupported("OUTPUT_SCHEMA_UNSUPPORTED")
            ref = value.get("$ref")
            if ref is not None:
                if not isinstance(ref, str) or not ref.startswith("#/$defs/"):
                    raise OutputContractUnsupported("OUTPUT_SCHEMA_UNSUPPORTED")
                refs.setdefault(owner, set()).add(ref.removeprefix("#/$defs/"))
            for key, child in value.items():
                walk(child, key if owner == "$defs" else owner)
        elif isinstance(value, list):
            for child in value:
                walk(child, owner)

    if isinstance(definitions, Mapping):
        for name, definition in definitions.items():
            walk(definition, str(name))
    root_schema = (
        {key: value for key, value in schema.items() if key != "$defs"}
        if isinstance(schema, Mapping)
        else schema
    )
    walk(root_schema)

    def cyclic(node: str, trail: frozenset[str]) -> bool:
        if node in trail:
            return True
        return any(cyclic(child, trail | {node}) for child in refs.get(node, set()))

    if any(cyclic(node, frozenset()) for node in refs):
        raise OutputContractUnsupported("OUTPUT_SCHEMA_UNSUPPORTED")


CONTENT_BLOG_TURN_CONTRACT_V1 = OutputContractV1(
    contract_id="content.blog-draft.turn",
    version=1,
    model=ContentBlogTurnOutputV1,
    enforcement_mode="provider_and_local",
)


def default_output_contract_registry() -> OutputContractRegistry:
    return OutputContractRegistry([CONTENT_BLOG_TURN_CONTRACT_V1])
