"""Compatibility wrapper; prefer python -m app.main login."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.main import main

if __name__ == "__main__":
    sys.argv.insert(1, "login")
    main()
