"""Primitives every blackbox in this service shares.

Nothing here reads the environment, opens a socket, or knows an application's
domain types. Everything above this package depends on it; it depends on nothing.
"""

from shepherd_contracts.canon import canonical_digest, canonical_json
from shepherd_contracts.clock import Clock, FrozenClock, SystemClock
from shepherd_contracts.errors import (
    ConfigurationError,
    PolicyRefused,
    PossibleSend,
    ProviderFailed,
    ShepherdError,
    Unauthorized,
)
from shepherd_contracts.ids import derive_id, new_id
from shepherd_contracts.output import (
    CONTENT_BLOG_TURN_CONTRACT_V1,
    ContentBlogTurnOutputV1,
    OutputContractRegistry,
    OutputContractUnsupported,
    OutputContractV1,
    OutputValidationFailed,
    StrictOutputModel,
    default_output_contract_registry,
)
from shepherd_contracts.principal import CallerPrincipalV1, PrincipalMinter
from shepherd_contracts.receipt import ReceiptV1
from shepherd_contracts.unit_of_work import UnitOfWork

__all__ = [
    "CONTENT_BLOG_TURN_CONTRACT_V1",
    "ContentBlogTurnOutputV1",
    "ShepherdError",
    "CallerPrincipalV1",
    "Clock",
    "ConfigurationError",
    "FrozenClock",
    "PolicyRefused",
    "OutputContractRegistry",
    "OutputContractUnsupported",
    "OutputContractV1",
    "OutputValidationFailed",
    "PossibleSend",
    "ProviderFailed",
    "PrincipalMinter",
    "ReceiptV1",
    "SystemClock",
    "StrictOutputModel",
    "Unauthorized",
    "UnitOfWork",
    "canonical_digest",
    "default_output_contract_registry",
    "canonical_json",
    "derive_id",
    "new_id",
]
