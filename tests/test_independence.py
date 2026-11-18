"""The verifier is the second implementation of the spec only if it shares no code with the
first. This fails the build the moment anything under src/verifier imports the migration."""

import ast
from pathlib import Path

VERIFIER = Path(__file__).resolve().parents[1] / "src" / "verifier"


def test_the_verifier_imports_nothing_from_the_migration():
    offenders = []
    for path in VERIFIER.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf8"))):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            offenders += [f"{path.name}: {n}" for n in names if n.split(".")[0] == "migration"]
    assert offenders == []
