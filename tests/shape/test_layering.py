"""The five checks that keep the template's shape a contract rather than a
starting position (design doc section 5 / build brief item 9):

1. no module outside `config.py` names the environment;
2. the domain layer (`models/`, `services/`) imports no adapter;
3. every port (`ports/*.py`, `repositories/*.py`) has a matching fake in
   `adapters/fake/`;
4. every route handler is under N=12 lines;
5. the container builds with zero network access.

Each check is a reusable function so this file can also prove the check
itself goes red on a deliberate violation (plan section 11, item 4: "a check
that has never gone red has not been tested") -- not just that today's
source happens to pass it.
"""

from __future__ import annotations

import ast
import re
import socket
from pathlib import Path

import pytest

SRC = Path(__file__).parent.parent.parent / "src/shepherd_workflow_service"
CONFIG_FILE = SRC / "config.py"
ROUTE_HANDLER_MAX_LINES = 12

#: Kept only for the ast-free fallback below; the real check parses.
_ENVIRON_PATTERN = re.compile(r"\bos\.environ\b|\bos\.getenv\(")


# ---------------------------------------------------------------------------
# Check 1: no module outside config.py names the environment
# ---------------------------------------------------------------------------


def _reads_the_environment(source: str) -> bool:
    """True only if the module actually READS the environment, per the AST.

    This parsed the raw text until 2026-09-04, which flagged any file that so
    much as NAMED `os.environ` -- including the docstring of a port whose whole
    purpose is to say "depend on this instead of reading `os.environ`". The rule
    then had a perverse shape: explaining it correctly broke it, and the cheap
    way out was to reword the prose until the matcher fell silent, leaving a
    weaker explanation and an unchanged gate.

    The general form -- a heuristic over a structured
    language must read that structure; for Python that means `ast`. A docstring
    is a `Constant`, never an `Attribute` or a `Call`, so parsing separates
    "mentions the environment" from "reads the environment" exactly.

    An unparseable file falls back to the text pattern: a file we cannot parse is
    the one case where guessing loudly beats staying quiet.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return bool(_ENVIRON_PATTERN.search(source))

    for node in ast.walk(tree):
        # os.environ / os.environ[...] / os.environ.get(...)
        if isinstance(node, ast.Attribute) and node.attr in {"environ", "getenv"}:
            value = node.value
            if isinstance(value, ast.Name) and value.id == "os":
                return True
        # from os import environ, getenv
        if isinstance(node, ast.ImportFrom) and node.module == "os":
            if any(alias.name in {"environ", "getenv"} for alias in node.names):
                return True
    return False


def _files_naming_the_environment(root: Path, *, exclude: set[Path]) -> list[Path]:
    violations = []
    for path in sorted(root.rglob("*.py")):
        if path in exclude:
            continue
        if _reads_the_environment(path.read_text()):
            violations.append(path)
    return violations


def test_no_module_outside_config_names_the_environment() -> None:
    violations = _files_naming_the_environment(SRC, exclude={CONFIG_FILE})
    assert violations == [], f"os.environ/os.getenv found outside config.py: {violations}"


def test_check_1_stays_quiet_on_a_file_that_only_MENTIONS_the_environment(
    tmp_path: Path,
) -> None:
    """The other direction, and the reason this check now parses.

    Every string below names `os.environ` without reading it. The text matcher
    flagged all four; a false red here is the worse failure, because it hands
    you a filename and the obvious response is to soften the prose until the
    gate goes quiet -- which weakens the documentation and leaves the matcher
    wrong. Real instances: `ports/secrets.py` and `adapters/real/mapping_secrets.py`,
    whose docstrings exist precisely to say "do not read `os.environ` here".
    """
    docstring_only = tmp_path / "explains.py"
    docstring_only.write_text(
        '"""Depend on SecretsPort rather than reading os.environ directly."""\n'
        "VALUE = 1\n"
    )
    comment_only = tmp_path / "commented.py"
    comment_only.write_text("# never call os.getenv( here\nVALUE = 2\n")
    string_literal = tmp_path / "literal.py"
    string_literal.write_text('BANNED_API = "os.environ"\nVALUE = 3\n')
    unrelated_attr = tmp_path / "other.py"
    unrelated_attr.write_text(
        "class Fake:\n    environ = {}\n\nfake = Fake()\nVALUE = fake.environ\n"
    )

    violations = _files_naming_the_environment(tmp_path, exclude=set())

    assert violations == [], (
        "these files only NAME the environment; flagging them is the false red that "
        f"pushed this gate's matcher from text to ast: {violations}"
    )


def test_check_1_still_catches_the_indirect_import(tmp_path: Path) -> None:
    """`from os import environ` reads the environment as surely as `os.environ`."""
    offender = tmp_path / "indirect.py"
    offender.write_text("from os import environ\n\nSTAGE = environ['STAGE']\n")

    violations = _files_naming_the_environment(tmp_path, exclude=set())

    assert violations == [offender]


def test_check_1_goes_red_on_a_deliberate_violation(tmp_path: Path) -> None:
    offender = tmp_path / "leaky.py"
    offender.write_text("import os\n\nSTAGE = os.environ['STAGE']\n")
    innocent = tmp_path / "clean.py"
    innocent.write_text("VALUE = 1\n")

    violations = _files_naming_the_environment(tmp_path, exclude=set())

    assert violations == [offender]


# ---------------------------------------------------------------------------
# Check 2: the domain layer imports no adapter
# ---------------------------------------------------------------------------

_DOMAIN_DIRS = ("models", "services")


def _imported_module_names(source: str) -> list[str]:
    tree = ast.parse(source)
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return names


def _domain_files_importing_adapters(root: Path) -> list[tuple[Path, str]]:
    violations = []
    for domain_dir in _DOMAIN_DIRS:
        for path in sorted((root / domain_dir).rglob("*.py")):
            for name in _imported_module_names(path.read_text()):
                if "adapters" in name.split("."):
                    violations.append((path, name))
    return violations


def test_domain_layer_imports_no_adapter() -> None:
    violations = _domain_files_importing_adapters(SRC)
    assert violations == [], f"domain module importing an adapter: {violations}"


def test_check_2_goes_red_on_a_deliberate_violation(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "services").mkdir()
    (tmp_path / "models" / "__init__.py").write_text("")
    (tmp_path / "services" / "__init__.py").write_text("")
    offender = tmp_path / "services" / "sneaky.py"
    offender.write_text(
        "from shepherd_workflow_service.adapters.real.sqs_event_bus import SqsPublisher\n"
    )

    violations = _domain_files_importing_adapters(tmp_path)

    assert violations == [(offender, "shepherd_workflow_service.adapters.real.sqs_event_bus")]


def test_controllers_and_handlers_import_no_adapter() -> None:
    """The plan's stricter sibling check (section 5, #2): inbound never
    reaches straight for infrastructure either."""
    violations = []
    for inbound_dir in ("controllers", "handlers"):
        for path in sorted((SRC / inbound_dir).rglob("*.py")):
            for name in _imported_module_names(path.read_text()):
                if "adapters" in name.split("."):
                    violations.append((path, name))
    assert violations == [], f"inbound module importing an adapter: {violations}"


def test_ports_directory_imports_no_vendor_sdk() -> None:
    """The plan's check #1: a port that imports a vendor is not a port."""
    banned = {"boto3", "sqlalchemy", "requests"}
    violations = []
    for path in sorted((SRC / "ports").rglob("*.py")):
        for name in _imported_module_names(path.read_text()):
            if name.split(".")[0] in banned:
                violations.append((path, name))
    assert violations == [], f"ports/ module importing a vendor SDK: {violations}"


# ---------------------------------------------------------------------------
# Check 3: every port has a matching fake
# ---------------------------------------------------------------------------


def _ports_missing_a_fake(root: Path) -> list[str]:
    fake_dir = root / "adapters" / "fake"
    fake_names = {p.name for p in fake_dir.glob("*.py") if p.name != "__init__.py"}
    missing = []
    for contract_dir in ("ports", "repositories"):
        for path in (root / contract_dir).glob("*.py"):
            if path.name == "__init__.py":
                continue
            if path.name not in fake_names:
                missing.append(f"{contract_dir}/{path.name}")
    return missing


def test_every_port_has_at_least_one_fake_implementation() -> None:
    missing = _ports_missing_a_fake(SRC)
    assert missing == [], f"no adapters/fake/ match for: {missing}"


def test_check_3_goes_red_on_a_deliberate_violation(tmp_path: Path) -> None:
    (tmp_path / "ports").mkdir()
    (tmp_path / "adapters" / "fake").mkdir(parents=True)
    (tmp_path / "ports" / "__init__.py").write_text("")
    (tmp_path / "ports" / "new_port.py").write_text("class NewPort: ...\n")
    # deliberately no adapters/fake/new_port.py

    missing = _ports_missing_a_fake(tmp_path)

    assert missing == ["ports/new_port.py"]


# ---------------------------------------------------------------------------
# Check 4: every route handler is under N lines
# ---------------------------------------------------------------------------


def _function_line_counts(source: str) -> dict[tuple[str, int], int]:
    """Line span for decorated HTTP handlers, including router factories."""
    tree = ast.parse(source)
    counts = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        routes = [
            decorator
            for decorator in node.decorator_list
            if isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr
            in {
                "api_route",
                "delete",
                "get",
                "head",
                "options",
                "patch",
                "post",
                "put",
                "trace",
                "websocket",
            }
        ]
        if routes:
            counts[(node.name, node.lineno)] = node.end_lineno - routes[0].lineno + 1
    return counts


def test_every_route_handler_is_under_the_line_limit() -> None:
    over_limit = []
    for path in sorted((SRC / "controllers").glob("*.py")):
        if path.name == "__init__.py":
            continue
        for (name, line), line_count in _function_line_counts(path.read_text()).items():
            if line_count > ROUTE_HANDLER_MAX_LINES:
                over_limit.append((path.name, name, line, line_count))
    assert over_limit == [], f"handlers over {ROUTE_HANDLER_MAX_LINES} lines: {over_limit}"


def test_check_4_goes_red_on_a_deliberate_violation() -> None:
    bloated_source = (
        "def first_router():\n"
        "    @router.websocket('/events')\n"
        "    async def handler():\n"
        + "\n".join(
            f"        x{i} = {i}" for i in range(ROUTE_HANDLER_MAX_LINES + 5)
        )
        + "\n\ndef second_router():\n"
        "    @router.get('/healthz')\n"
        "    def handler():\n"
        "        return {}\n"
    )

    counts = _function_line_counts(bloated_source)

    assert len(counts) == 2
    assert max(counts.values()) > ROUTE_HANDLER_MAX_LINES


# ---------------------------------------------------------------------------
# Check 5: the container builds with zero network access
# ---------------------------------------------------------------------------


def test_check_5_the_network_guard_mechanism_actually_traps_a_socket_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`tests/unit/test_container.py::test_container_builds_with_zero_network_access`
    is the real assertion against `build_container()`. This test proves the
    *mechanism* that check relies on -- patching `socket.socket` -- actually
    intercepts a call, so a future refactor that silently drops the
    monkeypatch would be caught here too.
    """

    def _no_sockets(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("socket.socket() was called")

    monkeypatch.setattr(socket, "socket", _no_sockets)

    with pytest.raises(AssertionError, match="socket.socket"):
        socket.socket()


def test_check_1_pattern_also_catches_getenv(tmp_path: Path) -> None:
    offender = tmp_path / "leaky.py"
    offender.write_text("import os\nSTAGE = os.getenv('STAGE')\n")

    violations = _files_naming_the_environment(tmp_path, exclude=set())

    assert violations == [offender]
