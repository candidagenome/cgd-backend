from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, text
from fastapi import HTTPException

from cgd.schemas.chromosome_schema import (
    ChromosomeResponse,
    ChromosomeOut,
    AliasOut,
    HistorySummary,
    ChromosomeHistoryResponse,
    ChromosomeHistoryOut,
    SequenceChangeOut,
    AnnotationChangeOut,
    CuratorialNoteOut,
    ChromosomeReferencesResponse,
    ReferenceForChromosome,
    ChromosomeSummaryNotesResponse,
    SummaryNoteOut,
    ChromosomeListResponse,
    OrganismChromosomes,
    ChromosomeListItem,
)
from cgd.models.models import (
    Feature,
    FeatAlias,
    Alias,
    FeatLocation,
    Seq,
    SeqChangeArchive,
    Note,
    NoteLink,
    FeatPara,
    Paragraph,
    RefLink,
    Reference,
    Organism,
)


def _get_organism_info(feature) -> tuple[str, int]:
    """Extract organism name and taxon_id from a feature."""
    org = feature.organism
    organism_name = None
    taxon_id = 0
    if org is not None:
        organism_name = getattr(org, "organism_name", None)
        taxon_id = getattr(org, "taxon_id", 0) or 0
    if not organism_name:
        organism_name = str(feature.organism_no)
    return organism_name, taxon_id


def _get_chromosome_by_name(db: Session, name: str) -> Feature:
    """Get a chromosome/contig feature by name, raise 404 if not found or not a root feature."""
    feature = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_alias).joinedload(FeatAlias.alias),
            joinedload(Feature.seq),
        )
        .filter(func.upper(Feature.feature_name) == func.upper(name.strip()))
        .first()
    )
    if feature is None:
        raise HTTPException(status_code=404, detail=f"Chromosome/contig '{name}' not found")

    # Check if it's a root feature (chromosome or contig)
    if feature.feature_type not in ("chromosome", "contig"):
        raise HTTPException(
            status_code=400,
            detail=f"Feature '{name}' is not a chromosome or contig (type: {feature.feature_type})"
        )

    return feature


def _get_current_location(db: Session, feature_no: int) -> tuple[int, int, str]:
    """Get current coordinates for a chromosome feature."""
    loc = (
        db.query(FeatLocation)
        .filter(
            FeatLocation.feature_no == feature_no,
            FeatLocation.is_loc_current == "Y",
        )
        .first()
    )
    if loc:
        return loc.start_coord, loc.stop_coord, None
    return None, None, None


def _get_seq_source(db: Session, feature_no: int) -> str:
    """Get the sequence source (assembly) for a chromosome."""
    seq = (
        db.query(Seq)
        .filter(
            Seq.feature_no == feature_no,
            Seq.is_seq_current == "Y",
        )
        .first()
    )
    if seq:
        return seq.source
    return None


def _get_history_summary(db: Session, feature_no: int) -> HistorySummary:
    """Get summary counts for sequence/annotation history."""
    # Get seq_no for this chromosome
    seq = (
        db.query(Seq)
        .filter(Seq.feature_no == feature_no)
        .first()
    )

    seq_updates = 0
    seq_last_date = None
    if seq:
        # Count sequence changes
        seq_changes = (
            db.query(SeqChangeArchive)
            .filter(SeqChangeArchive.seq_no == seq.seq_no)
            .order_by(SeqChangeArchive.date_created.desc())
            .all()
        )
        seq_updates = len(seq_changes)
        if seq_changes:
            seq_last_date = seq_changes[0].date_created.strftime("%Y-%m-%d")

    # Count annotation changes via note_link
    annot_notes = (
        db.query(NoteLink)
        .join(Note, NoteLink.note_no == Note.note_no)
        .filter(
            NoteLink.tab_name == "FEATURE",
            NoteLink.primary_key == feature_no,
            Note.note_type == "Annotation change",
        )
        .all()
    )
    annot_updates = len(annot_notes)
    annot_last_date = None
    if annot_notes:
        # Get the most recent date
        latest_note = (
            db.query(Note)
            .join(NoteLink, Note.note_no == NoteLink.note_no)
            .filter(
                NoteLink.tab_name == "FEATURE",
                NoteLink.primary_key == feature_no,
                Note.note_type == "Annotation change",
            )
            .order_by(Note.date_created.desc())
            .first()
        )
        if latest_note:
            annot_last_date = latest_note.date_created.strftime("%Y-%m-%d")

    # Count curatorial notes
    curatorial_notes = (
        db.query(NoteLink)
        .join(Note, NoteLink.note_no == Note.note_no)
        .filter(
            NoteLink.tab_name == "FEATURE",
            NoteLink.primary_key == feature_no,
            Note.note_type == "Curatorial",
        )
        .count()
    )

    return HistorySummary(
        sequence_updates=seq_updates,
        sequence_last_update=seq_last_date,
        annotation_updates=annot_updates,
        annotation_last_update=annot_last_date,
        curatorial_notes=curatorial_notes,
    )


def get_chromosome(db: Session, name: str) -> ChromosomeResponse:
    """
    Get basic chromosome info by name.
    """
    feature = _get_chromosome_by_name(db, name)

    organism_name, taxon_id = _get_organism_info(feature)

    # Get aliases
    aliases = []
    for fa in feature.feat_alias:
        alias = fa.alias
        if alias:
            aliases.append(AliasOut(
                alias_name=alias.alias_name,
                alias_type=alias.alias_type,
            ))

    # Get coordinates
    start_coord, stop_coord, _ = _get_current_location(db, feature.feature_no)

    # Get seq_source
    seq_source = _get_seq_source(db, feature.feature_no)

    # Get history summary
    history_summary = _get_history_summary(db, feature.feature_no)

    result = ChromosomeOut(
        feature_no=feature.feature_no,
        feature_name=feature.feature_name,
        feature_type=feature.feature_type,
        dbxref_id=feature.dbxref_id,
        organism_name=organism_name,
        taxon_id=taxon_id,
        headline=feature.headline,
        start_coord=start_coord,
        stop_coord=stop_coord,
        seq_source=seq_source,
        aliases=aliases,
        history_summary=history_summary,
    )

    return ChromosomeResponse(result=result)


def get_chromosome_history(db: Session, name: str) -> ChromosomeHistoryResponse:
    """
    Get chromosome history: sequence changes, annotation changes, curatorial notes.
    """
    feature = _get_chromosome_by_name(db, name)

    # Get sequence changes
    sequence_changes = []
    seq = (
        db.query(Seq)
        .filter(Seq.feature_no == feature.feature_no)
        .first()
    )
    if seq:
        sca_records = (
            db.query(SeqChangeArchive)
            .filter(SeqChangeArchive.seq_no == seq.seq_no)
            .order_by(SeqChangeArchive.date_created.desc())
            .all()
        )

        for sca in sca_records:
            # Get affected features via note_link
            affected_features = []
            note_links = (
                db.query(NoteLink)
                .join(Note, NoteLink.note_no == Note.note_no)
                .filter(
                    NoteLink.tab_name == "SEQ_CHANGE_ARCHIVE",
                    NoteLink.primary_key == sca.seq_change_archive_no,
                    Note.note_type == "Sequence change",
                )
                .all()
            )

            # Get the note text
            note_text = None
            if note_links:
                note_obj = note_links[0].note
                if note_obj:
                    note_text = note_obj.note

                # Get affected feature names from related note_links
                for nl in note_links:
                    related_links = (
                        db.query(NoteLink)
                        .join(Note, NoteLink.note_no == Note.note_no)
                        .filter(
                            NoteLink.tab_name == "FEATURE",
                            Note.note == nl.note.note,
                        )
                        .all()
                    )
                    for rl in related_links:
                        feat = db.query(Feature).filter(Feature.feature_no == rl.primary_key).first()
                        if feat and feat.feature_name not in affected_features:
                            affected_features.append(feat.feature_name)

            sequence_changes.append(SequenceChangeOut(
                date=sca.date_created.strftime("%Y-%m-%d"),
                affected_features=affected_features,
                start_coord=sca.change_start_coord,
                stop_coord=sca.change_stop_coord,
                change_type=sca.seq_change_type,
                old_seq=sca.old_seq[:100] if sca.old_seq and len(sca.old_seq) > 100 else sca.old_seq,
                new_seq=sca.new_seq[:100] if sca.new_seq and len(sca.new_seq) > 100 else sca.new_seq,
                note=note_text,
            ))

    # Get annotation changes
    annotation_changes = []
    annot_note_links = (
        db.query(NoteLink)
        .join(Note, NoteLink.note_no == Note.note_no)
        .filter(
            NoteLink.tab_name == "FEATURE",
            NoteLink.primary_key == feature.feature_no,
            Note.note_type == "Annotation change",
        )
        .all()
    )

    seen_notes = set()
    for nl in annot_note_links:
        note_obj = nl.note
        if note_obj and note_obj.note_no not in seen_notes:
            seen_notes.add(note_obj.note_no)

            # Get affected features with the same note text
            affected_features = []
            related_links = (
                db.query(NoteLink)
                .join(Note, NoteLink.note_no == Note.note_no)
                .filter(
                    NoteLink.tab_name == "FEATURE",
                    Note.note == note_obj.note,
                )
                .all()
            )
            for rl in related_links:
                feat = db.query(Feature).filter(Feature.feature_no == rl.primary_key).first()
                if feat and feat.feature_name not in affected_features:
                    affected_features.append(feat.feature_name)

            annotation_changes.append(AnnotationChangeOut(
                date=note_obj.date_created.strftime("%Y-%m-%d"),
                affected_features=affected_features,
                note=note_obj.note,
            ))

    # Get curatorial notes
    curatorial_notes = []
    curatorial_note_links = (
        db.query(NoteLink)
        .join(Note, NoteLink.note_no == Note.note_no)
        .filter(
            NoteLink.tab_name == "FEATURE",
            NoteLink.primary_key == feature.feature_no,
            Note.note_type == "Curatorial",
        )
        .all()
    )

    seen_curatorial = set()
    for nl in curatorial_note_links:
        note_obj = nl.note
        if note_obj and note_obj.note_no not in seen_curatorial:
            seen_curatorial.add(note_obj.note_no)
            curatorial_notes.append(CuratorialNoteOut(
                date=note_obj.date_created.strftime("%Y-%m-%d"),
                note=note_obj.note,
            ))

    result = ChromosomeHistoryOut(
        reference_no=feature.feature_no,
        feature_name=feature.feature_name,
        sequence_changes=sequence_changes,
        annotation_changes=annotation_changes,
        curatorial_notes=curatorial_notes,
    )

    return ChromosomeHistoryResponse(result=result)


def get_chromosome_references(db: Session, name: str) -> ChromosomeReferencesResponse:
    """
    Get references for a chromosome.
    """
    feature = _get_chromosome_by_name(db, name)

    # Get references via ref_link
    ref_links = (
        db.query(RefLink)
        .options(joinedload(RefLink.reference))
        .filter(
            RefLink.tab_name == "FEATURE",
            RefLink.primary_key == feature.feature_no,
        )
        .all()
    )

    references = []
    seen_refs = set()
    for rl in ref_links:
        ref = rl.reference
        if ref and ref.reference_no not in seen_refs:
            seen_refs.add(ref.reference_no)
            references.append(ReferenceForChromosome(
                reference_no=ref.reference_no,
                pubmed=ref.pubmed,
                citation=ref.citation,
                title=ref.title,
                year=ref.year,
            ))

    return ChromosomeReferencesResponse(
        reference_no=feature.feature_no,
        feature_name=feature.feature_name,
        references=references,
    )


def get_chromosome_summary_notes(db: Session, name: str) -> ChromosomeSummaryNotesResponse:
    """
    Get summary notes/paragraphs for a chromosome.
    """
    feature = _get_chromosome_by_name(db, name)

    # Get paragraphs via feat_para
    feat_paras = (
        db.query(FeatPara)
        .options(joinedload(FeatPara.paragraph))
        .filter(FeatPara.feature_no == feature.feature_no)
        .order_by(FeatPara.paragraph_order)
        .all()
    )

    summary_notes = []
    for fp in feat_paras:
        para = fp.paragraph
        if para:
            summary_notes.append(SummaryNoteOut(
                paragraph_no=para.paragraph_no,
                paragraph_text=para.paragraph_text,
                paragraph_order=fp.paragraph_order,
                date_edited=para.date_edited,
            ))

    return ChromosomeSummaryNotesResponse(
        reference_no=feature.feature_no,
        feature_name=feature.feature_name,
        summary_notes=summary_notes,
    )


def list_chromosomes(db: Session) -> ChromosomeListResponse:
    """
    List all chromosomes/contigs grouped by organism.
    """
    # Get all chromosome/contig features with their organisms
    features = (
        db.query(Feature)
        .options(joinedload(Feature.organism))
        .filter(Feature.feature_type.in_(["chromosome", "contig"]))
        .order_by(Feature.organism_no, Feature.feature_name)
        .all()
    )

    # Group by organism
    organisms_dict = {}
    for feature in features:
        org = feature.organism
        if not org:
            continue

        org_no = org.organism_no
        if org_no not in organisms_dict:
            organisms_dict[org_no] = {
                "organism_no": org_no,
                "organism_name": org.organism_name,
                "organism_abbrev": org.organism_abbrev,
                "chromosomes": [],
            }

        # Get length from location
        loc = (
            db.query(FeatLocation)
            .filter(
                FeatLocation.feature_no == feature.feature_no,
                FeatLocation.is_loc_current == "Y",
            )
            .first()
        )
        length = None
        if loc and loc.stop_coord:
            length = loc.stop_coord - (loc.start_coord or 0) + 1

        organisms_dict[org_no]["chromosomes"].append(
            ChromosomeListItem(
                feature_no=feature.feature_no,
                feature_name=feature.feature_name,
                feature_type=feature.feature_type,
                length=length,
            )
        )

    # Convert to list and sort chromosomes within each organism
    organisms_list = []
    for org_data in organisms_dict.values():
        # Sort chromosomes by name
        org_data["chromosomes"].sort(key=lambda x: x.feature_name)
        organisms_list.append(OrganismChromosomes(**org_data))

    # Sort organisms by name
    organisms_list.sort(key=lambda x: x.organism_name)

    return ChromosomeListResponse(organisms=organisms_list)
