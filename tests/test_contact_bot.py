import importlib.util, pathlib, sys

repo_root = pathlib.Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

spec = importlib.util.spec_from_file_location(
    "contact_bot", repo_root / "contact_bot" / "contact_bot.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def test_imports():
    assert module is not None
