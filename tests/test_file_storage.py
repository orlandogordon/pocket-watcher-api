"""Unit tests for the local file-storage layer (#59, Phase 2).

Exercises LocalStorage against a tmp_path root (no env mutation) plus the key /
extension helpers and the path-traversal guard.
"""
from uuid import uuid4

import pytest

from src.services.file_storage import LocalStorage, build_key, safe_ext


def test_safe_ext_whitelists_and_lowercases():
    assert safe_ext("Statement.PDF") == ".pdf"
    assert safe_ext("export.csv") == ".csv"
    assert safe_ext("sketchy.exe") == ""
    assert safe_ext("noext") == ""


def test_build_key_layout():
    doc = uuid4()
    assert build_key(7, doc, "My Statement.pdf") == f"7/{doc}.pdf"
    # Unknown extension is dropped, not preserved.
    assert build_key(7, doc, "thing.exe") == f"7/{doc}"


def test_save_open_roundtrip(tmp_path):
    store = LocalStorage(tmp_path)
    key = build_key(1, uuid4(), "s.pdf")

    store.save(b"%PDF-1.4 hello", key)

    assert store.exists(key)
    with store.open(key) as f:
        assert f.read() == b"%PDF-1.4 hello"
    # Stored under the user-segmented key path.
    assert (tmp_path / key).is_file()


def test_save_accepts_file_object(tmp_path):
    import io
    store = LocalStorage(tmp_path)
    key = build_key(2, uuid4(), "s.csv")

    store.save(io.BytesIO(b"a,b,c"), key)

    with store.open(key) as f:
        assert f.read() == b"a,b,c"


def test_delete(tmp_path):
    store = LocalStorage(tmp_path)
    key = build_key(1, uuid4(), "s.pdf")
    store.save(b"x", key)

    assert store.delete(key) is True
    assert store.exists(key) is False
    # Deleting a missing key is a no-op, not an error.
    assert store.delete(key) is False


def test_resolve_rejects_traversal(tmp_path):
    store = LocalStorage(tmp_path)
    with pytest.raises(ValueError):
        store.exists("../../etc/passwd")
    with pytest.raises(ValueError):
        store.save(b"x", "../escape.pdf")
