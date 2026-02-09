"""Authentication service - business logic for curator login."""

import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from cgd.core.settings import settings
from cgd.models.misc_model import Dbuser

from .models import CuratorSession
from .schemas import UserInfo

logger = logging.getLogger(__name__)

# JWT configuration
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 7


class AuthenticationError(Exception):
    """Raised when authentication fails."""

    pass


class AuthService:
    """Service for handling curator authentication."""

    def __init__(self, db: Session):
        self.db = db

    def get_user_by_userid(self, userid: str) -> Optional[Dbuser]:
        """
        Look up a user in the DBUSER table.

        Args:
            userid: The curator's Oracle userid

        Returns:
            Dbuser object if found, None otherwise
        """
        return (
            self.db.query(Dbuser)
            .filter(Dbuser.userid == userid.upper())
            .first()
        )

    def validate_user_status(self, userid: str) -> Dbuser:
        """
        Verify user exists and has 'Current' status.

        Args:
            userid: The curator's Oracle userid

        Returns:
            Dbuser object if valid

        Raises:
            AuthenticationError: If user not found or not current
        """
        user = self.get_user_by_userid(userid)

        if not user:
            logger.warning(f"Login attempt for unknown user: {userid}")
            raise AuthenticationError("Invalid username or password")

        if user.status.lower() != "current":
            logger.warning(f"Login attempt for non-current user: {userid}")
            raise AuthenticationError("User account is not active")

        return user

    def validate_oracle_credentials(self, userid: str, password: str) -> bool:
        """
        Validate credentials by attempting to connect to Oracle.

        This mirrors the legacy Perl behavior of using the provided
        credentials to connect to the database.

        Args:
            userid: Oracle username
            password: Oracle password

        Returns:
            True if connection succeeds

        Raises:
            AuthenticationError: If connection fails
        """
        # Build connection string for the user's credentials
        # Parse the existing DATABASE_URL to get connection details
        base_url = settings.database_url

        # Handle different database URL formats
        if "oracle" in base_url.lower():
            # Extract the DSN portion from the URL
            # Format: oracle+oracledb://user:pass@host:port/?service_name=xxx
            # We need to replace user:pass with the provided credentials
            try:
                # Parse the URL to extract connection details
                from urllib.parse import urlparse, parse_qs

                parsed = urlparse(base_url)
                host = parsed.hostname
                port = parsed.port or 1521

                # Get service_name from query params
                query_params = parse_qs(parsed.query)
                service_name = query_params.get("service_name", [""])[0]

                if service_name:
                    user_url = (
                        f"oracle+oracledb://{userid}:{password}@"
                        f"{host}:{port}/?service_name={service_name}"
                    )
                else:
                    # Assume SID is in the path
                    sid = parsed.path.strip("/")
                    user_url = (
                        f"oracle+oracledb://{userid}:{password}@"
                        f"{host}:{port}/{sid}"
                    )

                # Attempt connection
                engine = create_engine(user_url, pool_pre_ping=True)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1 FROM DUAL"))

                return True

            except Exception as e:
                logger.warning(f"Oracle authentication failed for {userid}: {e}")
                raise AuthenticationError("Invalid username or password")
        else:
            # For non-Oracle databases (dev/test), just verify user exists
            # This allows testing without Oracle credentials
            logger.info(f"Non-Oracle DB: skipping credential validation for {userid}")
            return True

    def authenticate(self, userid: str, password: str) -> Dbuser:
        """
        Full authentication flow.

        1. Verify user exists in DBUSER with Current status
        2. Validate Oracle credentials by attempting connection

        Args:
            userid: Curator userid
            password: Oracle password

        Returns:
            Dbuser object for the authenticated user

        Raises:
            AuthenticationError: If authentication fails
        """
        # Step 1: Check user status
        user = self.validate_user_status(userid)

        # Step 2: Validate Oracle credentials
        self.validate_oracle_credentials(userid, password)

        logger.info(f"Successfully authenticated user: {userid}")
        return user

    def create_access_token(
        self,
        user: Dbuser,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> tuple[str, datetime]:
        """
        Create a JWT access token for the user.

        Args:
            user: Authenticated Dbuser object
            user_agent: Browser user agent
            ip_address: Client IP

        Returns:
            Tuple of (token string, expiration datetime)
        """
        session_id = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(
            minutes=ACCESS_TOKEN_EXPIRE_MINUTES
        )

        payload = {
            "sub": user.userid,
            "dbuser_no": user.dbuser_no,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "email": user.email,
            "jti": session_id,
            "exp": expires_at,
            "iat": datetime.now(timezone.utc),
            "type": "access",
        }

        token = jwt.encode(payload, settings.jwt_secret_key, algorithm=JWT_ALGORITHM)

        # Store session in database for revocation support
        session = CuratorSession(
            session_id=session_id,
            userid=user.userid,
            expires_at=expires_at.replace(tzinfo=None),  # Oracle doesn't like tzinfo
            user_agent=user_agent[:512] if user_agent else None,
            ip_address=ip_address,
        )
        self.db.add(session)
        self.db.commit()

        return token, expires_at

    def create_refresh_token(self, user: Dbuser) -> tuple[str, datetime]:
        """
        Create a refresh token (longer-lived).

        Args:
            user: Authenticated Dbuser object

        Returns:
            Tuple of (token string, expiration datetime)
        """
        expires_at = datetime.now(timezone.utc) + timedelta(
            days=REFRESH_TOKEN_EXPIRE_DAYS
        )

        payload = {
            "sub": user.userid,
            "dbuser_no": user.dbuser_no,
            "exp": expires_at,
            "iat": datetime.now(timezone.utc),
            "type": "refresh",
        }

        token = jwt.encode(payload, settings.jwt_secret_key, algorithm=JWT_ALGORITHM)
        return token, expires_at

    def verify_token(self, token: str, token_type: str = "access") -> dict:
        """
        Verify and decode a JWT token.

        Args:
            token: JWT token string
            token_type: Expected token type ("access" or "refresh")

        Returns:
            Decoded token payload

        Raises:
            AuthenticationError: If token is invalid or expired
        """
        try:
            payload = jwt.decode(
                token, settings.jwt_secret_key, algorithms=[JWT_ALGORITHM]
            )

            if payload.get("type") != token_type:
                raise AuthenticationError("Invalid token type")

            # For access tokens, verify session hasn't been revoked
            if token_type == "access":
                session_id = payload.get("jti")
                if session_id:
                    session = (
                        self.db.query(CuratorSession)
                        .filter(CuratorSession.session_id == session_id)
                        .first()
                    )
                    if session and session.revoked:
                        raise AuthenticationError("Session has been revoked")

            return payload

        except jwt.ExpiredSignatureError:
            raise AuthenticationError("Token has expired")
        except jwt.InvalidTokenError as e:
            raise AuthenticationError(f"Invalid token: {e}")

    def revoke_session(self, session_id: str) -> bool:
        """
        Revoke a session (logout).

        Args:
            session_id: The session ID (jti claim from token)

        Returns:
            True if session was found and revoked
        """
        session = (
            self.db.query(CuratorSession)
            .filter(CuratorSession.session_id == session_id)
            .first()
        )

        if session:
            session.revoked = True
            self.db.commit()
            logger.info(f"Revoked session {session_id} for user {session.userid}")
            return True

        return False

    def get_user_info(self, userid: str) -> UserInfo:
        """
        Get user information from DBUSER table.

        Args:
            userid: Curator userid

        Returns:
            UserInfo schema

        Raises:
            AuthenticationError: If user not found
        """
        user = self.get_user_by_userid(userid)
        if not user:
            raise AuthenticationError("User not found")

        return UserInfo(
            dbuser_no=user.dbuser_no,
            userid=user.userid,
            first_name=user.first_name,
            last_name=user.last_name,
            email=user.email,
            status=user.status,
        )

    def cleanup_expired_sessions(self) -> int:
        """
        Remove expired sessions from the database.

        Returns:
            Number of sessions removed
        """
        result = (
            self.db.query(CuratorSession)
            .filter(CuratorSession.expires_at < datetime.now())
            .delete()
        )
        self.db.commit()
        return result
