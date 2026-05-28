"""Local file storage for uploaded statement documents (#59).

Bytes live on the server filesystem, not in the database — the DB only keeps a
``storage_key`` reference. A small ``StorageBackend`` interface keeps a future
swap (object store, etc.) cheap without building one now; ``LocalStorage`` is the
only implementation today.

Keys are built by us, never from a client filename, and are always
``{user_id}/{document_uuid}{ext}`` — user-segmented and UUID-named. Resolving a
key back to a path is guarded against escaping the storage root.
"""
from __future__ import annotations

import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO, Iterator, Union
from uuid import UUID

from src.logging_config import get_logger

logger = get_logger(__name__)

# Repo-root-relative default for dev; the C5 deploy sets UPLOAD_DIR to the
# mounted /data/uploads volume. `./data/` is gitignored.
_DEFAULT_ROOT = Path(__file__).resolve().parents[2] / "data" / "uploads"

# Extensions we serve back; anything else is stored without one (defense in
# depth — the upload endpoint already restricts to parseable statement types).
_ALLOWED_EXTS = {".pdf", ".csv"}


def safe_ext(filename: str) -> str:
    """Return a lowercased, whitelisted extension (incl. dot) or '' ."""
    ext = os.path.splitext(filename or "")[1].lower()
    return ext if ext in _ALLOWED_EXTS else ""


def build_key(user_id: int, document_uuid: Union[str, UUID], filename: str) -> str:
    """Compose the storage key for a document. Never incorporates the raw client
    filename beyond a sanitized extension."""
    return f"{user_id}/{document_uuid}{safe_ext(filename)}"


class StorageBackend(ABC):
    @abstractmethod
    def save(self, content: Union[bytes, BinaryIO], key: str) -> str: ...

    @abstractmethod
    def open(self, key: str) -> BinaryIO: ...

    @abstractmethod
    def delete(self, key: str) -> bool: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def iter_keys(self) -> Iterator[str]: ...

    @abstractmethod
    def modified_time(self, key: str) -> float: ...


class LocalStorage(StorageBackend):
    """Filesystem-backed storage rooted at ``root``."""

    def __init__(self, root: Union[str, Path]):
        self.root = Path(root).resolve()

    def _resolve(self, key: str) -> Path:
        target = (self.root / key).resolve()
        if target != self.root and not target.is_relative_to(self.root):
            raise ValueError(f"storage key escapes root: {key!r}")
        return target

    def save(self, content: Union[bytes, BinaryIO], key: str) -> str:
        target = self._resolve(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "wb") as f:
            if isinstance(content, (bytes, bytearray)):
                f.write(content)
            else:
                shutil.copyfileobj(content, f)
        return key

    def open(self, key: str) -> BinaryIO:
        return open(self._resolve(key), "rb")

    def delete(self, key: str) -> bool:
        target = self._resolve(key)
        if target.exists():
            target.unlink()
            return True
        return False

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def iter_keys(self) -> Iterator[str]:
        if not self.root.exists():
            return
        for path in self.root.rglob("*"):
            if path.is_file():
                yield path.relative_to(self.root).as_posix()

    def modified_time(self, key: str) -> float:
        """POSIX mtime (epoch seconds) of the stored file."""
        return self._resolve(key).stat().st_mtime


_storage: StorageBackend | None = None


def get_storage() -> StorageBackend:
    """Process-wide storage backend, rooted at $UPLOAD_DIR (default ./data/uploads)."""
    global _storage
    if _storage is None:
        root = os.environ.get("UPLOAD_DIR", str(_DEFAULT_ROOT))
        _storage = LocalStorage(root)
        logger.info("file_storage: LocalStorage root=%s", _storage.root)
    return _storage
