"""Pydantic schemas for authentication."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    """Request schema for curator login."""

    username: str = Field(..., min_length=1, max_length=12, description="Oracle userid")
    password: str = Field(..., min_length=1, description="Oracle password")


class TokenResponse(BaseModel):
    """Response schema for successful login."""

    access_token: str
    token_type: str = "bearer"
    expires_in: int = Field(..., description="Token expiration time in seconds")


class UserInfo(BaseModel):
    """Schema for current user information."""

    dbuser_no: int
    userid: str
    first_name: str
    last_name: str
    email: str
    status: str

    class Config:
        from_attributes = True


class RefreshRequest(BaseModel):
    """Request schema for token refresh (uses HttpOnly cookie)."""

    pass


class LogoutResponse(BaseModel):
    """Response schema for logout."""

    message: str = "Successfully logged out"
