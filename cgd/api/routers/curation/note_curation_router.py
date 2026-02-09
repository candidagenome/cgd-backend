"""
Note Curation Router - Endpoints for curator notes.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.note_curation_service import (
    NoteCurationService,
    NoteCurationError,
    NOTE_TYPES,
    LINKABLE_TABLES,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/note", tags=["curation-note"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class LinkedEntityOut(BaseModel):
    """Linked entity in note details."""

    note_link_no: int
    tab_name: str
    primary_key: int
    date_created: Optional[str]
    entity_name: Optional[str] = None
    gene_name: Optional[str] = None


class NoteDetailResponse(BaseModel):
    """Full note details."""

    note_no: int
    note: str
    note_type: str
    date_created: Optional[str]
    created_by: str
    linked_entities: list[LinkedEntityOut]


class NoteSearchItem(BaseModel):
    """Note item in search results."""

    note_no: int
    note: str
    note_type: str
    date_created: Optional[str]
    created_by: str


class NoteSearchResponse(BaseModel):
    """Response for note search."""

    notes: list[NoteSearchItem]
    total: int
    page: int
    page_size: int


class LinkedEntityInput(BaseModel):
    """Input for linking an entity to a note."""

    tab_name: str = Field(..., description="Table name (e.g., FEATURE)")
    primary_key: int = Field(..., description="Primary key in the table")


class CreateNoteRequest(BaseModel):
    """Request to create a new note."""

    note_text: str = Field(..., description="Note text")
    note_type: str = Field(..., description="Note type")
    linked_entities: Optional[list[LinkedEntityInput]] = Field(
        None, description="Entities to link to this note"
    )


class CreateNoteResponse(BaseModel):
    """Response for creating a note."""

    note_no: int
    message: str


class UpdateNoteRequest(BaseModel):
    """Request to update a note."""

    note_text: Optional[str] = Field(None, description="New note text")
    note_type: Optional[str] = Field(None, description="New note type")


class SuccessResponse(BaseModel):
    """Generic success response."""

    success: bool
    message: str


class LinkNoteRequest(BaseModel):
    """Request to link note to entity."""

    tab_name: str = Field(..., description="Table name")
    primary_key: int = Field(..., description="Primary key")


class LinkNoteResponse(BaseModel):
    """Response for linking note to entity."""

    note_link_no: int
    message: str


class NoteTypesResponse(BaseModel):
    """Response for available note types."""

    note_types: list[str]


class LinkableTablesResponse(BaseModel):
    """Response for linkable tables."""

    tables: list[str]


class EntityNotesResponse(BaseModel):
    """Response for notes linked to an entity."""

    notes: list[NoteSearchItem]


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/types", response_model=NoteTypesResponse)
def get_note_types(current_user: CurrentUser):
    """Get available note types."""
    return NoteTypesResponse(note_types=NOTE_TYPES)


@router.get("/linkable-tables", response_model=LinkableTablesResponse)
def get_linkable_tables(current_user: CurrentUser):
    """Get tables that can be linked to notes."""
    return LinkableTablesResponse(tables=LINKABLE_TABLES)


@router.get("/search", response_model=NoteSearchResponse)
def search_notes(
    current_user: CurrentUser,
    query: Optional[str] = Query(None, description="Search text in note"),
    note_type: Optional[str] = Query(None, description="Filter by note type"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Results per page"),
    db: Session = Depends(get_db),
):
    """Search notes by text or type."""
    service = NoteCurationService(db)

    notes, total = service.search_notes(query, note_type, page, page_size)

    return NoteSearchResponse(
        notes=[NoteSearchItem(**n) for n in notes],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{note_no}", response_model=NoteDetailResponse)
def get_note_details(
    note_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get full note details including linked entities."""
    service = NoteCurationService(db)

    try:
        details = service.get_note_details(note_no)
        return NoteDetailResponse(**details)
    except NoteCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.post("/", response_model=CreateNoteResponse)
def create_note(
    request: CreateNoteRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Create a new note with optional entity links."""
    service = NoteCurationService(db)

    try:
        linked_entities = None
        if request.linked_entities:
            linked_entities = [
                {"tab_name": e.tab_name, "primary_key": e.primary_key}
                for e in request.linked_entities
            ]

        note_no = service.create_note(
            request.note_text,
            request.note_type,
            current_user.userid,
            linked_entities,
        )

        return CreateNoteResponse(
            note_no=note_no,
            message="Note created successfully",
        )
    except NoteCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.put("/{note_no}", response_model=SuccessResponse)
def update_note(
    note_no: int,
    request: UpdateNoteRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Update note text and/or type."""
    service = NoteCurationService(db)

    try:
        service.update_note(
            note_no,
            current_user.userid,
            request.note_text,
            request.note_type,
        )

        return SuccessResponse(
            success=True,
            message="Note updated successfully",
        )
    except NoteCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/{note_no}", response_model=SuccessResponse)
def delete_note(
    note_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Delete a note and all its entity links."""
    service = NoteCurationService(db)

    try:
        service.delete_note(note_no, current_user.userid)

        return SuccessResponse(
            success=True,
            message="Note deleted successfully",
        )
    except NoteCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{note_no}/link", response_model=LinkNoteResponse)
def link_note_to_entity(
    note_no: int,
    request: LinkNoteRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Link a note to an entity."""
    service = NoteCurationService(db)

    try:
        note_link_no = service.link_note_to_entity(
            note_no,
            request.tab_name,
            request.primary_key,
            current_user.userid,
        )

        return LinkNoteResponse(
            note_link_no=note_link_no,
            message="Note linked to entity",
        )
    except NoteCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/link/{note_link_no}", response_model=SuccessResponse)
def unlink_note_from_entity(
    note_link_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Unlink a note from an entity."""
    service = NoteCurationService(db)

    try:
        service.unlink_note_from_entity(note_link_no, current_user.userid)

        return SuccessResponse(
            success=True,
            message="Note unlinked from entity",
        )
    except NoteCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/entity/{tab_name}/{primary_key}", response_model=EntityNotesResponse)
def get_notes_for_entity(
    tab_name: str,
    primary_key: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get all notes linked to a specific entity."""
    service = NoteCurationService(db)

    notes = service.get_notes_for_entity(tab_name, primary_key)

    return EntityNotesResponse(
        notes=[NoteSearchItem(**n) for n in notes]
    )
