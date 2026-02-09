"""SQLAlchemy models for authentication/session management."""

from datetime import datetime

from sqlalchemy import DateTime, Index, PrimaryKeyConstraint, String, text
from sqlalchemy.orm import Mapped, mapped_column

from cgd.models import Base


class CuratorSession(Base):
    """
    Server-side session tracking for curator logins.

    This enables session revocation (logout) and audit trails.
    Sessions are stored in the database for persistence across server restarts.
    """

    __tablename__ = "curator_session"
    __table_args__ = (
        PrimaryKeyConstraint("session_id", name="curator_session_pk"),
        Index("curator_session_userid_idx", "userid"),
        Index("curator_session_expires_idx", "expires_at"),
        {"schema": "MULTI"},
    )

    session_id: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        comment="Unique session identifier (JWT jti claim)",
    )
    userid: Mapped[str] = mapped_column(
        String(12),
        nullable=False,
        comment="Curator userid from DBUSER table",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("SYSDATE"),
        comment="Session creation timestamp",
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Session expiration timestamp",
    )
    revoked: Mapped[bool] = mapped_column(
        default=False,
        comment="Whether session has been revoked (logout)",
    )
    user_agent: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
        comment="Browser user agent for audit",
    )
    ip_address: Mapped[str | None] = mapped_column(
        String(45),
        nullable=True,
        comment="Client IP address for audit",
    )
