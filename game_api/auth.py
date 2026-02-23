"""Authentication helpers for game API endpoints."""

from fastapi import Header, HTTPException, status


def require_user(
    x_username: str | None = Header(default=None, alias="X-Username"),
    x_token: str | None = Header(default=None, alias="X-Token"),
) -> str:
    user = (x_username or "").strip()
    token = (x_token or "").strip()
    if not user or not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing auth headers: X-Username / X-Token",
        )
    try:
        import auth_utils as auth  # lazy import to avoid boot failure in thin env
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"auth backend unavailable: {exc}",
        ) from exc

    if not auth.check_token(user, token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or expired token",
        )
    return user
