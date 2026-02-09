"""FastAPI dependencies for authentication."""

from typing import Annotated, Optional

from fastapi import Cookie, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from cgd.db.deps import get_db

from .schemas import UserInfo
from .service import AuthService, AuthenticationError

# Security scheme for Swagger UI
security = HTTPBearer(auto_error=False)


async def get_current_user(
    db: Session = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    access_token: str | None = Cookie(default=None),
) -> UserInfo:
    """
    FastAPI dependency to get the current authenticated user.

    Checks for JWT token in:
    1. Authorization header (Bearer token)
    2. access_token cookie

    Args:
        db: Database session
        credentials: Bearer token from Authorization header
        access_token: Token from HttpOnly cookie

    Returns:
        UserInfo for the authenticated user

    Raises:
        HTTPException 401: If not authenticated
    """
    token = None

    # Prefer Authorization header
    if credentials:
        token = credentials.credentials
    # Fall back to cookie
    elif access_token:
        token = access_token

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    auth_service = AuthService(db)

    try:
        payload = auth_service.verify_token(token, token_type="access")
        return UserInfo(
            dbuser_no=payload["dbuser_no"],
            userid=payload["sub"],
            first_name=payload["first_name"],
            last_name=payload["last_name"],
            email=payload["email"],
            status="Current",  # Only current users can have valid tokens
        )
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user_optional(
    db: Session = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    access_token: str | None = Cookie(default=None),
) -> Optional[UserInfo]:
    """
    Optional version of get_current_user.

    Returns None instead of raising an exception if not authenticated.
    Useful for endpoints that behave differently for authenticated users.

    Args:
        db: Database session
        credentials: Bearer token from Authorization header
        access_token: Token from HttpOnly cookie

    Returns:
        UserInfo if authenticated, None otherwise
    """
    token = None

    if credentials:
        token = credentials.credentials
    elif access_token:
        token = access_token

    if not token:
        return None

    auth_service = AuthService(db)

    try:
        payload = auth_service.verify_token(token, token_type="access")
        return UserInfo(
            dbuser_no=payload["dbuser_no"],
            userid=payload["sub"],
            first_name=payload["first_name"],
            last_name=payload["last_name"],
            email=payload["email"],
            status="Current",
        )
    except AuthenticationError:
        return None


# Type aliases for cleaner dependency injection
CurrentUser = Annotated[UserInfo, Depends(get_current_user)]
OptionalUser = Annotated[Optional[UserInfo], Depends(get_current_user_optional)]
