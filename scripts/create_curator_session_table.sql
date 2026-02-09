-- Create curator_session table for authentication session tracking
-- Run this as a user with CREATE TABLE privilege on the MULTI schema

CREATE TABLE MULTI.curator_session (
    session_id VARCHAR2(64) NOT NULL,
    userid VARCHAR2(12) NOT NULL,
    created_at DATE DEFAULT SYSDATE NOT NULL,
    expires_at DATE NOT NULL,
    revoked NUMBER(1) DEFAULT 0 NOT NULL,
    user_agent VARCHAR2(512),
    ip_address VARCHAR2(45),
    CONSTRAINT curator_session_pk PRIMARY KEY (session_id)
);

-- Create indexes for efficient queries
CREATE INDEX MULTI.curator_session_userid_idx ON MULTI.curator_session (userid);
CREATE INDEX MULTI.curator_session_expires_idx ON MULTI.curator_session (expires_at);

-- Add comments
COMMENT ON TABLE MULTI.curator_session IS 'Server-side session tracking for curator logins';
COMMENT ON COLUMN MULTI.curator_session.session_id IS 'Unique session identifier (JWT jti claim)';
COMMENT ON COLUMN MULTI.curator_session.userid IS 'Curator userid from DBUSER table';
COMMENT ON COLUMN MULTI.curator_session.created_at IS 'Session creation timestamp';
COMMENT ON COLUMN MULTI.curator_session.expires_at IS 'Session expiration timestamp';
COMMENT ON COLUMN MULTI.curator_session.revoked IS 'Whether session has been revoked (logout): 0=active, 1=revoked';
COMMENT ON COLUMN MULTI.curator_session.user_agent IS 'Browser user agent for audit';
COMMENT ON COLUMN MULTI.curator_session.ip_address IS 'Client IP address for audit';

-- Grant permissions (adjust as needed for your curators)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON MULTI.curator_session TO curator_role;
