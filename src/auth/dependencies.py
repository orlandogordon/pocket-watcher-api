"""Auth dependencies for FastAPI routers.

This module is the single source of truth for the `get_current_user_id`
dependency used across all routers. Today it returns a hardcoded user id
(the existing placeholder behavior); todo #26 will swap the body of this
function for real JWT validation without touching any router code.
"""


def get_current_user_id() -> int:
    """Placeholder auth dependency. Returns the dev user id.

    Will be replaced with real JWT validation in todo #26.
    """
    return 1
