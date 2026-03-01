"""
Conftest for pipeline topology tests.

Ensures ingestion/src is on sys.path and patches metadata.__init__
so pure-Pydantic connector modules (aci/, ado/) can be imported
without requiring generated OM schemas to be present.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

SRC_DIR = Path(__file__).parent.parent.parent.parent.parent / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Patch metadata.__init__ before it gets imported.
# The real __init__.py imports ProfilerProcessorConfig from metadata.generated,
# which only exists after `make generate`. Stub it out so our pure-Pydantic
# connector modules can be imported in environments without generated schemas.
_STUBS = [
    "metadata.profiler",
    "metadata.profiler.api",
    "metadata.profiler.api.models",
    "metadata.profiler.metrics",
    "metadata.profiler.metrics.registry",
    "metadata.profiler.registry",
    "metadata.profiler.source",
    "metadata.profiler.source.database",
    "metadata.profiler.source.database.base",
    "metadata.profiler.source.database.base.profiler_resolver",
    "metadata.utils.dependency_injector",
    "metadata.utils.dependency_injector.dependency_injector",
    "metadata.utils.service_spec",
    "metadata.utils.service_spec.service_spec",
    "metadata.generated",
    "metadata.generated.schema",
]

for _stub_name in _STUBS:
    if _stub_name not in sys.modules:
        sys.modules[_stub_name] = MagicMock()
