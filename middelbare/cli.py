"""
CLI for regenerating the databases from source files.

Usage:
    uv run middelbare

This will:
1. Parse HTML files from html/ -> JSON files in json/
2. Create scholen.duckdb from JSON files
3. Update loting_matching.duckdb from json/matching_en_plaatsing/
"""

from pathlib import Path

from . import scholen
from . import loting


def main():
    """Regenerate all databases from source files."""
    base_dir = Path(".")

    print()
    print("=" * 60)
    print("  Middelbare Database Builder")
    print("=" * 60)
    print()

    # Build scholen.duckdb
    scholen.build(base_dir)

    print()

    # Update loting_matching.duckdb
    loting.build(base_dir)

    print()
    print("=" * 60)
    print("  All databases updated successfully!")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
