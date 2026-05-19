$ErrorActionPreference = "Stop"

$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = "1"
python -m pytest -q
python -m compileall custom_components tests
