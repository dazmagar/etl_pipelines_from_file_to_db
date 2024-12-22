# git config alias.clear "!f() { python ./examples/clean_pycache.py; }; f"
import shutil
import sys
from pathlib import Path


def clear_caches(directory: Path, ignored: list):
    ignored_paths = [Path(ignore).resolve() for ignore in ignored]

    for path in directory.rglob("*"):
        if path.is_dir() and path.name in ("__pycache__", ".pytest_cache"):
            if any(path.resolve().is_relative_to(ignored_path) for ignored_path in ignored_paths):
                continue

            print(f"Remove: {path}")
            shutil.rmtree(path)


if __name__ == "__main__":
    root_dir = Path(".")
    ignored_dirs = ["venv"]
    clear_caches(root_dir, ignored_dirs)
    print("Clean up completed.", file=sys.stderr)
