"""Authentication API router."""

import logging
from datetime import timedelta

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from cgd.core.settings import settings
from cgd.db.deps import get_db

from .deps import CurrentUser, get_current_user
from .schemas import LoginRequest, LogoutResponse, TokenResponse, UserInfo
from .service import AuthService, AuthenticationError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])

security = HTTPBearer(auto_error=False)


@router.post("/login", response_model=TokenResponse)
async def login(
    request: Request,
    response: Response,
    login_data: LoginRequest,
    db: Session = Depends(get_db),
):
    """
    Authenticate curator and return JWT tokens.

    Validates credentials against Oracle database (same as legacy Perl system).
    Returns access token in response body and sets refresh token as HttpOnly cookie.

    - **username**: Oracle userid (case-insensitive)
    - **password**: Oracle password
    """
    auth_service = AuthService(db)

    try:
        # Authenticate user
        user = auth_service.authenticate(login_data.username, login_data.password)

        # Get client info for audit
        user_agent = request.headers.get("user-agent")
        client_ip = request.client.host if request.client else None

        # Create tokens
        access_token, access_expires = auth_service.create_access_token(
            user, user_agent=user_agent, ip_address=client_ip
        )
        refresh_token, refresh_expires = auth_service.create_refresh_token(user)

        # Set refresh token as HttpOnly cookie
        response.set_cookie(
            key="refresh_token",
            value=refresh_token,
            httponly=True,
            secure=True,  # Only send over HTTPS
            samesite="lax",
            max_age=int(timedelta(days=7).total_seconds()),
            path="/api/auth",  # Only sent to auth endpoints
        )

        # Also set access token as cookie for browser convenience
        response.set_cookie(
            key="access_token",
            value=access_token,
            httponly=True,
            secure=True,
            samesite="lax",
            max_age=int(timedelta(minutes=settings.jwt_access_token_expire_minutes).total_seconds()),
            path="/",
        )

        logger.info(f"Successful login for user: {user.userid}")

        return TokenResponse(
            access_token=access_token,
            token_type="bearer",
            expires_in=settings.jwt_access_token_expire_minutes * 60,
        )

    except AuthenticationError as e:
        logger.warning(f"Login failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.post("/logout", response_model=LogoutResponse)
async def logout(
    response: Response,
    current_user: CurrentUser,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Logout current user and revoke session.

    Clears auth cookies and revokes the server-side session.
    """
    auth_service = AuthService(db)

    # Try to revoke the session if we have the token
    if credentials:
        try:
            payload = auth_service.verify_token(credentials.credentials)
            session_id = payload.get("jti")
            if session_id:
                auth_service.revoke_session(session_id)
        except AuthenticationError:
            pass  # Token already invalid, just clear cookies

    # Clear cookies
    response.delete_cookie(key="access_token", path="/")
    response.delete_cookie(key="refresh_token", path="/api/auth")

    logger.info(f"User logged out: {current_user.userid}")

    return LogoutResponse()


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
):
    """
    Refresh access token using refresh token from cookie.

    Returns new access token. Refresh token remains valid until expiration.
    """
    refresh_token = request.cookies.get("refresh_token")

    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No refresh token provided",
        )

    auth_service = AuthService(db)

    try:
        # Verify refresh token
        payload = auth_service.verify_token(refresh_token, token_type="refresh")

        # Get user from database
        user = auth_service.get_user_by_userid(payload["sub"])
        if not user or user.status.lower() != "current":
            raise AuthenticationError("User account is not active")

        # Get client info for audit
        user_agent = request.headers.get("user-agent")
        client_ip = request.client.host if request.client else None

        # Create new access token
        access_token, access_expires = auth_service.create_access_token(
            user, user_agent=user_agent, ip_address=client_ip
        )

        # Set new access token cookie
        response.set_cookie(
            key="access_token",
            value=access_token,
            httponly=True,
            secure=True,
            samesite="lax",
            max_age=int(timedelta(minutes=settings.jwt_access_token_expire_minutes).total_seconds()),
            path="/",
        )

        return TokenResponse(
            access_token=access_token,
            token_type="bearer",
            expires_in=settings.jwt_access_token_expire_minutes * 60,
        )

    except AuthenticationError as e:
        # Clear invalid refresh token
        response.delete_cookie(key="refresh_token", path="/api/auth")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )


@router.get("/me", response_model=UserInfo)
async def get_current_user_info(current_user: CurrentUser):
    """
    Get current authenticated user's information.

    Returns user details from DBUSER table.
    """
    return current_user


@router.get("/check")
async def check_auth(current_user: CurrentUser):
    """
    Simple endpoint to check if user is authenticated.

    Returns 200 if authenticated, 401 otherwise.
    Useful for frontend auth state checks.
    """
    return {"authenticated": True, "userid": current_user.userid}
