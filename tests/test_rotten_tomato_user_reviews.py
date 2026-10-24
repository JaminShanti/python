import importlib.util
import pathlib

def test_import_rotten_tomato_user_reviews():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "rotten_tomato_user_reviews.py"
    spec = importlib.util.spec_from_file_location("rotten_tomato_user_reviews", script_path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise AssertionError(f"Failed to import rotten_tomato_user_reviews: {e}")
    assert module is not None
