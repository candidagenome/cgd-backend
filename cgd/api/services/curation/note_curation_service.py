"""
Note Curation Service - Business logic for curator notes.

Mirrors functionality from legacy NewNote.pm and UpdateNote.pm:
- Create notes with links to entities (features, colleagues, etc.)
- Edit note text and type
- Delete notes
- Link/unlink notes to entities
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    Note,
    NoteLink,
)

logger = logging.getLogger(__name__)

SOURCE = "CGD"

# Note types from legacy CODE table
NOTE_TYPES = [
    "Curator",
    "History",
    "Nomenclature",
    "Sequence",
    "Reference",
    "Gene product",
    "Literature guide",
    "Standard",
    "Other",
]

# Supported tables for linking
LINKABLE_TABLES = [
    "FEATURE",
    "COLLEAGUE",
    "REFERENCE",
    "DBXREF",
    "SEQ_CHANGE_ARCHIVE",
    "FEAT_ANNOTATION_ARCHIVE",
    "REF_PROPERTY",
    "REFPROP_FEAT",
    "FEAT_LOCATION",
]


class NoteCurationError(Exception):
    """Raised when note curation validation fails."""

    pass


class NoteCurationService:
    """Service for note curation operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_note_by_no(self, note_no: int) -> Optional[Note]:
        """Get note by note_no."""
        return (
            self.db.query(Note)
            .filter(Note.note_no == note_no)
            .first()
        )

    def get_note_details(self, note_no: int) -> dict:
        """
        Get full note details including linked entities.
        """
        note = self.get_note_by_no(note_no)
        if not note:
            raise NoteCurationError(f"Note {note_no} not found")

        # Get linked entities
        links = (
            self.db.query(NoteLink)
            .filter(NoteLink.note_no == note_no)
            .all()
        )

        linked_entities = []
        for link in links:
            entity_info = {
                "note_link_no": link.note_link_no,
                "tab_name": link.tab_name,
                "primary_key": link.primary_key,
                "date_created": link.date_created.isoformat()
                if link.date_created else None,
            }

            # Get entity name for display (for features)
            if link.tab_name == "FEATURE":
                feature = (
                    self.db.query(Feature)
                    .filter(Feature.feature_no == link.primary_key)
                    .first()
                )
                if feature:
                    entity_info["entity_name"] = feature.feature_name
                    entity_info["gene_name"] = feature.gene_name

            linked_entities.append(entity_info)

        return {
            "note_no": note.note_no,
            "note": note.note,
            "note_type": note.note_type,
            "date_created": note.date_created.isoformat()
            if note.date_created else None,
            "created_by": note.created_by,
            "linked_entities": linked_entities,
        }

    def search_notes(
        self,
        query: str = None,
        note_type: str = None,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[dict], int]:
        """
        Search notes by text or type.

        Returns (list of note dicts, total count).
        """
        base_query = self.db.query(Note)

        if query:
            base_query = base_query.filter(
                Note.note.ilike(f"%{query}%")
            )

        if note_type:
            base_query = base_query.filter(Note.note_type == note_type)

        base_query = base_query.order_by(Note.date_created.desc())

        total = base_query.count()
        results = base_query.offset((page - 1) * page_size).limit(page_size).all()

        return (
            [
                {
                    "note_no": n.note_no,
                    "note": n.note[:200] + "..." if len(n.note) > 200 else n.note,
                    "note_type": n.note_type,
                    "date_created": n.date_created.isoformat()
                    if n.date_created else None,
                    "created_by": n.created_by,
                }
                for n in results
            ],
            total,
        )

    def create_note(
        self,
        note_text: str,
        note_type: str,
        curator_userid: str,
        linked_entities: list[dict] = None,
    ) -> int:
        """
        Create a new note with optional entity links.

        linked_entities format: [{"tab_name": "FEATURE", "primary_key": 123}, ...]

        Returns note_no.
        """
        # Validate note type
        if note_type not in NOTE_TYPES:
            raise NoteCurationError(
                f"Invalid note type '{note_type}'. Valid types: {', '.join(NOTE_TYPES)}"
            )

        # Check for duplicate note
        existing = (
            self.db.query(Note)
            .filter(
                Note.note_type == note_type,
                Note.note == note_text,
            )
            .first()
        )
        if existing:
            raise NoteCurationError(
                f"A note with this type and text already exists (note_no: {existing.note_no})"
            )

        # Create note
        note = Note(
            note=note_text,
            note_type=note_type,
            created_by=curator_userid[:12],
        )
        self.db.add(note)
        self.db.flush()

        # Create entity links
        if linked_entities:
            for entity in linked_entities:
                tab_name = entity.get("tab_name", "").upper()
                primary_key = entity.get("primary_key")

                if tab_name not in LINKABLE_TABLES:
                    logger.warning(f"Skipping invalid table name: {tab_name}")
                    continue

                link = NoteLink(
                    note_no=note.note_no,
                    tab_name=tab_name,
                    primary_key=primary_key,
                    created_by=curator_userid[:12],
                )
                self.db.add(link)

        self.db.commit()

        logger.info(f"Created note {note.note_no} by {curator_userid}")

        return note.note_no

    def update_note(
        self,
        note_no: int,
        curator_userid: str,
        note_text: str = None,
        note_type: str = None,
    ) -> bool:
        """
        Update note text and/or type.

        Returns True on success.
        """
        note = self.get_note_by_no(note_no)
        if not note:
            raise NoteCurationError(f"Note {note_no} not found")

        if note_type and note_type not in NOTE_TYPES:
            raise NoteCurationError(
                f"Invalid note type '{note_type}'. Valid types: {', '.join(NOTE_TYPES)}"
            )

        if note_text is not None:
            note.note = note_text
        if note_type is not None:
            note.note_type = note_type

        self.db.commit()

        logger.info(f"Updated note {note_no} by {curator_userid}")

        return True

    def delete_note(self, note_no: int, curator_userid: str) -> bool:
        """
        Delete a note and all its entity links.

        Returns True on success.
        """
        note = self.get_note_by_no(note_no)
        if not note:
            raise NoteCurationError(f"Note {note_no} not found")

        # Delete all entity links first (cascade should handle this, but be explicit)
        self.db.query(NoteLink).filter(NoteLink.note_no == note_no).delete()

        # Delete the note
        self.db.delete(note)
        self.db.commit()

        logger.info(f"Deleted note {note_no} by {curator_userid}")

        return True

    def link_note_to_entity(
        self,
        note_no: int,
        tab_name: str,
        primary_key: int,
        curator_userid: str,
    ) -> int:
        """
        Link a note to an entity.

        Returns note_link_no.
        """
        note = self.get_note_by_no(note_no)
        if not note:
            raise NoteCurationError(f"Note {note_no} not found")

        tab_name = tab_name.upper()
        if tab_name not in LINKABLE_TABLES:
            raise NoteCurationError(
                f"Invalid table name '{tab_name}'. Valid tables: {', '.join(LINKABLE_TABLES)}"
            )

        # Check for existing link
        existing = (
            self.db.query(NoteLink)
            .filter(
                NoteLink.note_no == note_no,
                NoteLink.tab_name == tab_name,
                NoteLink.primary_key == primary_key,
            )
            .first()
        )
        if existing:
            raise NoteCurationError(
                f"Note is already linked to this entity"
            )

        link = NoteLink(
            note_no=note_no,
            tab_name=tab_name,
            primary_key=primary_key,
            created_by=curator_userid[:12],
        )
        self.db.add(link)
        self.db.commit()

        logger.info(
            f"Linked note {note_no} to {tab_name}:{primary_key} by {curator_userid}"
        )

        return link.note_link_no

    def unlink_note_from_entity(
        self,
        note_link_no: int,
        curator_userid: str,
    ) -> bool:
        """
        Unlink a note from an entity.

        Returns True on success.
        """
        link = (
            self.db.query(NoteLink)
            .filter(NoteLink.note_link_no == note_link_no)
            .first()
        )
        if not link:
            raise NoteCurationError(f"Note link {note_link_no} not found")

        self.db.delete(link)
        self.db.commit()

        logger.info(f"Unlinked note link {note_link_no} by {curator_userid}")

        return True

    def get_notes_for_entity(
        self,
        tab_name: str,
        primary_key: int,
    ) -> list[dict]:
        """
        Get all notes linked to a specific entity.
        """
        tab_name = tab_name.upper()

        results = (
            self.db.query(Note, NoteLink)
            .join(NoteLink, Note.note_no == NoteLink.note_no)
            .filter(
                NoteLink.tab_name == tab_name,
                NoteLink.primary_key == primary_key,
            )
            .order_by(Note.date_created.desc())
            .all()
        )

        return [
            {
                "note_no": note.note_no,
                "note": note.note,
                "note_type": note.note_type,
                "date_created": note.date_created.isoformat()
                if note.date_created else None,
                "created_by": note.created_by,
                "note_link_no": link.note_link_no,
            }
            for note, link in results
        ]
