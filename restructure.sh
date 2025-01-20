#!/usr/bin/env bash
set -euo pipefail

# 1) Create a new top-level src/ directory (if not exists)
if [ ! -d "src" ]; then
  mkdir src
fi

# 2) Move the whisp/ folder to src/, merging contents if needed
if [ -d "whisp" ]; then
  if [ -d "src/whisp" ]; then
    # Move files from whisp to src/whisp, merging contents
    mv whisp/* src/whisp/ || true
    mv whisp/.* src/whisp/ 2>/dev/null || true
    rmdir whisp || true
  else
    mv whisp src/
  fi
fi

# 3) Inside src/whisp, move Python files out of src/whisp/src/
if [ -d "src/whisp/src" ]; then
  mv src/whisp/src/* src/whisp/ || true
  mv src/whisp/src/.* src/whisp/ 2>/dev/null || true
  rmdir src/whisp/src || true
fi

# 4) Rename 'test' to 'tests' and relocate test_assess_risk.py
if [ -d "test" ]; then
  if [ ! -d "tests" ]; then
    mv test tests
  fi
  if [ -d "tests/src" ]; then
    mv tests/src/test_assess_risk.py tests/test_assess_risk.py || true
    rmdir tests/src || true
  fi
fi

# 5) Remove old build artifacts
rm -rf build UNKNOWN.egg-info dist || true

echo "Reorganization complete. New structure:"
tree -a -I '.git|.github|__pycache__|.pytest_cache'

