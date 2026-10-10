"""App package initialization."""

import sys
from pathlib import Path

# Ensure repository root is on sys.path so 'shared' is accessible
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

