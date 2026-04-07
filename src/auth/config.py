"""Auth configuration.

Loaded at import time. Fails loudly on startup if `JWT_SECRET` is missing
or too short — there is no silent fallback to a weak default.
"""
import os

from dotenv import load_dotenv

load_dotenv()

JWT_SECRET: str = os.environ["JWT_SECRET"]  # KeyError if missing — app refuses to boot
if len(JWT_SECRET) < 32:
    raise RuntimeError("JWT_SECRET must be at least 32 characters")

JWT_ALGORITHM: str = "HS256"
JWT_EXPIRY_DAYS: int = int(os.environ.get("JWT_EXPIRY_DAYS", "30"))
