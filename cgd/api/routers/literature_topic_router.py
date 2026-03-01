"""
Literature Topic Router - API endpoints for literature topic search.
"""
from __future__ import annotations

import logging
import traceback
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import literature_topic_service
from cgd.schemas.literature_topic_schema import (
    LiteratureTopicTreeResponse,
    LiteratureTopicSearchResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/literature-topic", tags=["literature-topic"])


@router.get("/tree", response_model=LiteratureTopicTreeResponse)
def get_literature_topic_tree(db: Session = Depends(get_db)):
    """
    Get hierarchical tree of literature topics.

    Returns a tree structure of literature topics with reference counts
    for each topic. The tree is organized hierarchically based on
    CV term relationships.

    Returns:
        Tree structure of literature topics with reference counts.
    """
    try:
        return literature_topic_service.get_literature_topic_tree(db)
    except Exception as e:
        logger.error(f"Error in get_literature_topic_tree: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search", response_model=LiteratureTopicSearchResponse)
def search_by_topics(
    topics: str = Query(
        ...,
        description="Comma-separated list of cv_term_no values for topics to search"
    ),
    db: Session = Depends(get_db),
):
    """
    Search references by literature topics.

    Args:
        topics: Comma-separated list of cv_term_no values

    Returns:
        References and genes associated with each selected topic.
    """
    try:
        # Parse comma-separated topic IDs
        topic_ids = []
        for t in topics.split(','):
            t = t.strip()
            if t:
                try:
                    topic_ids.append(int(t))
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid topic ID: {t}. Must be an integer."
                    )

        if not topic_ids:
            raise HTTPException(
                status_code=400,
                detail="No valid topic IDs provided"
            )

        return literature_topic_service.search_by_topics(db, topic_ids)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in search_by_topics: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search", response_model=LiteratureTopicSearchResponse)
def search_by_topics_post(
    topic_cv_term_nos: List[int],
    db: Session = Depends(get_db),
):
    """
    Search references by literature topics (POST endpoint for large lists).

    Args:
        topic_cv_term_nos: List of cv_term_no values for topics to search

    Returns:
        References and genes associated with each selected topic.
    """
    try:
        if not topic_cv_term_nos:
            raise HTTPException(
                status_code=400,
                detail="No topic IDs provided"
            )

        return literature_topic_service.search_by_topics(db, topic_cv_term_nos)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in search_by_topics_post: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
