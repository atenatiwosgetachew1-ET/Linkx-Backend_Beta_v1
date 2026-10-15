"""Shared pytest configuration and fixtures for the LinkX test suite."""

import os
import sys

# Ensure the repository root is on sys.path so that
# `batch_manager`, `public`, etc. are importable.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
