"""Bronze bid-opening detail to Silver domain transformation (in progress)."""

from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, ClassVar

from procurement.silver.domain.contracts import VERSION
from procurement.silver.domain.parsers import (
    clean_text,
    lookup_path,
    parse_boolean,
    parse_date,
    parse_iso_datetime,
    parse_money,
)
from procurement.silver.domain.protocols import (
    ExtractedChild,
    QualityIssue,
    RawRecordEnvelope,
    TransformedRecord,
)
from procurement.silver.domain.registry import register_transformer
from procurement.silver.domain.selection import digest
from procurement.silver.domain.transformers.base import (
    BaseResourceTransformer,
    canonical_json,
)

OPENING_MAPPING_VERSION = "opening-1"

OPENING_ROUND_PATHS = {
    "ldt_single_envelope": "roundmng.bidoBidroundMngViewDTO",
    "ldt_two_envelope": "roundmng.bidoBidroundMngViewDTO",
    "khac_single_envelope": "opening_out.bidoBidroundOutMngViewDTO",
    "khac_two_envelope": "opening_out_technical.bidoBidroundOutMngViewDTO",
    "adb_wb_single_envelope": "opening_adb_wb.bidoBidroundMngViewDTO",
}


def classify_opening_shape(payload: dict[str, Any]) -> str | None:
    """Identify supported opening shapes from the Bronze component names."""
    parts = set(payload)

    if parts == {"opening_adb_wb"}:
        return "adb_wb_single_envelope"

    if parts == {"opening_out"}:
        return "khac_single_envelope"

    if parts in (
        {"opening_out_technical"},
        {"opening_out_technical", "opening_out_financial"},
    ):
        return "khac_two_envelope"

    base = {"notify", "roundmng"}

    if not base.issubset(parts):
        return None

    components = parts - base - {"cancellation_notice"}

    if components == {"bid_open", "lot_open_detail"}:
        return "ldt_single_envelope"

    if components in (
        {"bid_open_technical", "lot_open_detail_technical"},
        {
            "bid_open_technical",
            "lot_open_detail_technical",
            "bid_open_financial",
            "lot_open_detail_financial",
        },
    ):
        return "ldt_two_envelope"

    return None


def infer_collection_status(
    payload: dict[str, Any],
    shape: str | None,
    root: dict[str, Any] | None,
) -> str | None:
    """Conservatively infer collection status from observed components."""

    if shape is None:
        return None

    # Cancellation requires separate business validation.
    if "cancellation_notice" in payload:
        return None

    if isinstance(root, dict) and (
        root.get("cancelDate") not in (None, "")
        or root.get("cancelDecisionNo") not in (None, "")
        or root.get("status") == "03"
    ):
        return None

    round_info = lookup_path(
        payload,
        OPENING_ROUND_PATHS[shape],
    )

    if not isinstance(round_info, dict):
        return None

    def has_submission_list(part_name: str) -> bool:
        part = payload.get(part_name)

        if not isinstance(part, dict):
            return False

        if part.get("error") or part.get("success") is False:
            return False

        submissions = lookup_path(
            payload,
            f"{part_name}."
            "bidSubmissionByContractorViewResponse."
            "bidSubmissionDTOList",
        )

        return isinstance(submissions, list) and all(
            isinstance(item, dict) for item in submissions
        )

    if shape == "ldt_single_envelope":
        lots = payload.get("lot_open_detail")
        if not isinstance(lots, list) or any(not isinstance(row, dict) for row in lots):
            return None

    single_parts = {
        "ldt_single_envelope": "bid_open",
        "khac_single_envelope": "opening_out",
        "adb_wb_single_envelope": "opening_adb_wb",
    }

    if shape in single_parts:
        return "complete" if has_submission_list(single_parts[shape]) else None

    dual_parts = {
        "ldt_two_envelope": (
            "bid_open_technical",
            "bid_open_financial",
        ),
        "khac_two_envelope": (
            "opening_out_technical",
            "opening_out_financial",
        ),
    }

    technical_part, financial_part = dual_parts[shape]

    if shape == "ldt_two_envelope":
        technical_lots = payload.get("lot_open_detail_technical")

        if not isinstance(technical_lots, list) or any(
            not isinstance(row, dict) for row in technical_lots
        ):
            return None

    if not has_submission_list(technical_part):
        return None

    if "successBidOpenDateTc" not in round_info:
        return None

    financial_date = round_info["successBidOpenDateTc"]

    if financial_date is None:
        if financial_part in payload:
            return None

        if shape == "ldt_two_envelope" and "lot_open_detail_financial" in payload:
            return None

        return "awaiting_financial"

    try:
        published = parse_iso_datetime(financial_date)
    except (TypeError, ValueError):
        return None

    if published is None:
        return None

    if shape == "ldt_two_envelope":
        financial_lots = payload.get("lot_open_detail_financial")

        if not isinstance(financial_lots, list) or any(
            not isinstance(row, dict) for row in financial_lots
        ):
            return None

    return "complete" if has_submission_list(financial_part) else None


@register_transformer("bid_opening_detail")
class BidOpeningTransformer(BaseResourceTransformer):
    """Transform a single Bronze bid opening into its Silver revision (initial mapping)."""

    mapping_version = OPENING_MAPPING_VERSION

    revision_contracts: ClassVar[dict[str, str]] = {
        **VERSION,
        "opening": OPENING_MAPPING_VERSION,
    }

    @property
    def table_name(self) -> str:
        return "bid_opening_detail"

    @property
    def entity_type(self) -> str:
        return "opening"

    @property
    def id_scheme(self) -> str:
        return "notify_no"

    def extract_root(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        root_paths = (
            "notify.bidNoContractorResponse.bidNotification",
            "opening_out.bidoNotifyContractorP",
            "opening_out_technical.bidoNotifyContractorP",
            "opening_adb_wb.bidNoContractorResponse.bidNotification",
        )

        for path in root_paths:
            root = lookup_path(payload, path)
            if isinstance(root, dict) and clean_text(root.get("notifyNo")):
                return root

        return None

    def extract_source_identity(self, root: dict[str, Any]) -> str | None:
        return clean_text(root.get("notifyNo"))

    def extract_attributes(
        self,
        root: dict[str, Any],
        revision_id: str,
        entity_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> dict[str, Any]:
        notify_no = clean_text(root.get("notifyNo"))
        public_date_raw = clean_text(root.get("publicDate"))

        return {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            "notify_no": notify_no,
            "notify_version": clean_text(root.get("notifyVersion")),
            "business_number": notify_no,
            "title": clean_text(root.get("bidName")),
            "process_apply": clean_text(root.get("processApply")),
            "bid_form": clean_text(root.get("bidForm")),
            "bid_mode": clean_text(root.get("bidMode")),
            "package_no": clean_text(root.get("bidNo")),
            "package_id": clean_text(root.get("bidId")),
            "opening_shape": None,
            "is_multi_lot": None,
            "scheduled_open_date": None,
            "technical_open_date": None,
            "financial_open_date": None,
            "actual_open_date": None,
            "cancel_date": None,
            "cancel_decision_date": None,
            "cancel_decision_no": None,
            "cancel_reason": None,
            "collection_status": None,
            "total_bidders": None,
            "public_date": parse_date(public_date_raw),
            "public_date_raw": public_date_raw,
            "status_raw": clean_text(root.get("status")),
            "root_json": canonical_json(root),
        }

    def transform(self, envelope: RawRecordEnvelope) -> TransformedRecord:
        result = super().transform(envelope)
        attrs = result.typed_attributes

        if attrs is None:
            return result

        obs_id = result.observation["observation_id"]
        shape = classify_opening_shape(envelope.payload)
        attrs["opening_shape"] = shape

        if shape is None:
            result.issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, "unknown_opening_shape"]),
                    observation_id=obs_id,
                    code="unknown_opening_shape",
                    path="payload",
                    severity="warn",
                )
            )
            return result

        root = self.extract_root(envelope.payload)

        attrs["collection_status"] = infer_collection_status(
            envelope.payload,
            shape,
            root,
        )

        # The scheduled opening is part of the notice, not the round response.
        if isinstance(root, dict):
            scheduled_raw = root.get("bidOpenDate")
            if scheduled_raw not in (None, ""):
                try:
                    scheduled = parse_iso_datetime(scheduled_raw)
                    attrs["scheduled_open_date"] = (
                        scheduled.isoformat() if scheduled else None
                    )
                except (TypeError, ValueError):
                    result.issues.append(
                        QualityIssue(
                            issue_id=digest([obs_id, "invalid_scheduled_open_date"]),
                            observation_id=obs_id,
                            code="invalid_datetime",
                            path="bidOpenDate",
                            severity="warn",
                        )
                    )

        # Cancellation data may remain available even when the round response is null.
        cancellation = lookup_path(
            envelope.payload,
            "cancellation_notice.bidCancelingResponse",
        )
        cancel_source = (
            cancellation
            if isinstance(cancellation, dict)
            else root if isinstance(root, dict) else {}
        )

        attrs["cancel_decision_no"] = clean_text(cancel_source.get("cancelDecisionNo"))
        attrs["cancel_reason"] = clean_text(cancel_source.get("cancelReason"))

        cancel_date_raw = cancel_source.get("cancelDate")
        if cancel_date_raw not in (None, ""):
            try:
                cancelled = parse_iso_datetime(cancel_date_raw)
                attrs["cancel_date"] = cancelled.isoformat() if cancelled else None
            except (TypeError, ValueError):
                result.issues.append(
                    QualityIssue(
                        issue_id=digest([obs_id, "invalid_cancel_date"]),
                        observation_id=obs_id,
                        code="invalid_datetime",
                        path="cancelDate",
                        severity="warn",
                    )
                )

        decision_date_raw = cancel_source.get("cancelDecisionDate")
        if decision_date_raw not in (None, ""):
            try:
                attrs["cancel_decision_date"] = parse_date(decision_date_raw)
            except (TypeError, ValueError):
                result.issues.append(
                    QualityIssue(
                        issue_id=digest([obs_id, "invalid_cancel_decision_date"]),
                        observation_id=obs_id,
                        code="invalid_date",
                        path="cancelDecisionDate",
                        severity="warn",
                    )
                )

        round_path = OPENING_ROUND_PATHS[shape]
        round_info = lookup_path(envelope.payload, round_path)

        if not isinstance(round_info, dict):
            return result

        def read_datetime(field: str) -> str | None:
            value = round_info.get(field)
            if value is None or value == "":
                return None

            try:
                parsed = parse_iso_datetime(value)
            except (TypeError, ValueError):
                result.issues.append(
                    QualityIssue(
                        issue_id=digest(
                            [obs_id, "invalid_datetime", round_path, field]
                        ),
                        observation_id=obs_id,
                        code="invalid_datetime",
                        path=f"{round_path}.{field}",
                        severity="warn",
                    )
                )
                return None

            return parsed.isoformat() if parsed else None

        actual_open = read_datetime("successBidOpenDate")
        attrs["actual_open_date"] = actual_open

        if shape in {"ldt_two_envelope", "khac_two_envelope"}:
            attrs["technical_open_date"] = actual_open
            attrs["financial_open_date"] = read_datetime("successBidOpenDateTc")

        count = round_info.get("countContractor")
        if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
            attrs["total_bidders"] = count

        multi_lot = round_info.get("isMultiLot")
        if multi_lot is not None:
            try:
                attrs["is_multi_lot"] = parse_boolean(multi_lot)
            except (TypeError, ValueError):
                result.issues.append(
                    QualityIssue(
                        issue_id=digest(
                            [obs_id, "invalid_boolean", round_path, "isMultiLot"]
                        ),
                        observation_id=obs_id,
                        code="invalid_boolean",
                        path=f"{round_path}.isMultiLot",
                        severity="warn",
                    )
                )

        return result

    def extract_children(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> list[ExtractedChild]:
        """Extract opening phases from the observed Bronze components."""

        shape = classify_opening_shape(payload)
        if shape is None:
            return []

        phase_sources = {
            "ldt_single_envelope": [
                ("single", "bid_open", "roundmng.bidoBidroundMngViewDTO"),
            ],
            "ldt_two_envelope": [
                ("technical", "bid_open_technical", "roundmng.bidoBidroundMngViewDTO"),
                ("financial", "bid_open_financial", "roundmng.bidoBidroundMngViewDTO"),
            ],
            "khac_single_envelope": [
                ("single", "opening_out", "opening_out.bidoBidroundOutMngViewDTO"),
            ],
            "khac_two_envelope": [
                (
                    "technical",
                    "opening_out_technical",
                    "opening_out_technical.bidoBidroundOutMngViewDTO",
                ),
                (
                    "financial",
                    "opening_out_financial",
                    "opening_out_technical.bidoBidroundOutMngViewDTO",
                ),
            ],
            "adb_wb_single_envelope": [
                (
                    "single",
                    "opening_adb_wb",
                    "opening_adb_wb.bidoBidroundMngViewDTO",
                ),
            ],
        }

        children: list[ExtractedChild] = []
        missing = object()

        for ordinal, (phase, part_name, round_path) in enumerate(phase_sources[shape]):
            part = payload.get(part_name)
            round_info = lookup_path(payload, round_path)

            source_path = (
                f"{part_name}."
                "bidSubmissionByContractorViewResponse.bidSubmissionDTOList"
            )

            # Distinguish absent response, null response and actual lists.
            if part_name not in payload:
                submission_shape = "not_fetched"
            elif not isinstance(part, dict):
                submission_shape = "missing"
            else:
                response = part.get("bidSubmissionByContractorViewResponse", missing)

                if response is missing:
                    submission_shape = "missing"
                elif response is None:
                    submission_shape = "null"
                elif isinstance(response, dict):
                    submissions = response.get("bidSubmissionDTOList", missing)

                    if submissions is missing:
                        submission_shape = "missing"
                    elif submissions is None:
                        submission_shape = "null"
                    elif isinstance(submissions, list):
                        submission_shape = "list"
                    else:
                        submission_shape = "missing"
                        issues.append(
                            QualityIssue(
                                issue_id=digest(
                                    [obs_id, "invalid_phase_submissions", source_path]
                                ),
                                observation_id=obs_id,
                                code="invalid_phase_submissions",
                                path=source_path,
                                severity="error",
                            )
                        )
                else:
                    submission_shape = "missing"
                    issues.append(
                        QualityIssue(
                            issue_id=digest(
                                [obs_id, "invalid_phase_response", source_path]
                            ),
                            observation_id=obs_id,
                            code="invalid_phase_response",
                            path=source_path,
                            severity="error",
                        )
                    )

            # Resolve the phase opening timestamp.
            opened_at = None
            reported_count = None

            if isinstance(round_info, dict):
                date_field = (
                    "successBidOpenDateTc"
                    if phase == "financial"
                    else "successBidOpenDate"
                )

                raw_date = round_info.get(date_field)

                if raw_date not in (None, ""):
                    try:
                        parsed = parse_iso_datetime(raw_date)
                        opened_at = parsed.isoformat() if parsed else None
                    except (TypeError, ValueError):
                        issues.append(
                            QualityIssue(
                                issue_id=digest(
                                    [
                                        obs_id,
                                        "invalid_phase_date",
                                        round_path,
                                        date_field,
                                    ]
                                ),
                                observation_id=obs_id,
                                code="invalid_phase_date",
                                path=f"{round_path}.{date_field}",
                                severity="warn",
                            )
                        )

                # The round's count applies to the initial participant set.
                # Financial submissions may contain a smaller subset.
                if phase != "financial":
                    count = round_info.get("countContractor")
                    if (
                        isinstance(count, int)
                        and not isinstance(count, bool)
                        and count >= 0
                    ):
                        reported_count = count

            phase_id = digest([revision_id, "opening_phase", phase])
            raw_json = canonical_json(part)

            typed_attributes = {
                "phase_id": phase_id,
                "revision_id": revision_id,
                "phase": phase,
                "source_path": source_path,
                "opened_at": opened_at,
                "reported_contractor_count": reported_count,
                "submission_shape": submission_shape,
                "phase_status_raw": None,
                "raw_json": raw_json,
            }

            children.append(
                ExtractedChild(
                    child_id=phase_id,
                    revision_id=revision_id,
                    kind="opening_phase",
                    role=phase,
                    path=source_path,
                    ordinal=ordinal,
                    source_id=None,
                    payload_json=raw_json,
                    typed_table="opening_phase",
                    typed_attributes=typed_attributes,
                )
            )

        submission_children = self._extract_submissions(
            payload,
            revision_id,
            issues,
            obs_id,
            phase_sources[shape],
        )

        children.extend(submission_children)

        children.extend(
            self._extract_ldt_lots(
                payload,
                revision_id,
                issues,
                obs_id,
                phase_sources[shape],
            )
        )

        if shape.startswith("khac_") or shape == "adb_wb_single_envelope":
            children.extend(
                self._extract_nested_lots(
                    payload,
                    revision_id,
                    issues,
                    obs_id,
                    phase_sources[shape],
                    submission_children,
                )
            )
        return children

    def _extract_submissions(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
        phase_sources: list[tuple[str, str, str]],
    ) -> list[ExtractedChild]:
        """Project bidder submissions from each opening phase."""

        children: list[ExtractedChild] = []

        for phase, part_name, _ in phase_sources:
            list_path = (
                f"{part_name}."
                "bidSubmissionByContractorViewResponse.bidSubmissionDTOList"
            )
            submissions = lookup_path(payload, list_path)

            # Null/missing phases are represented in opening_phase.
            if not isinstance(submissions, list):
                continue

            for index, row in enumerate(submissions):
                row_path = f"{list_path}[{index}]"

                if not isinstance(row, dict):
                    issues.append(
                        QualityIssue(
                            issue_id=digest([obs_id, "invalid_submission", row_path]),
                            observation_id=obs_id,
                            code="invalid_submission",
                            path=row_path,
                            severity="error",
                        )
                    )
                    continue

                contractor_code = clean_text(row.get("contractorCode"))

                if contractor_code is None:
                    issues.append(
                        QualityIssue(
                            issue_id=digest(
                                [obs_id, "missing_contractor_code", row_path]
                            ),
                            observation_id=obs_id,
                            code="missing_contractor_code",
                            path=row_path,
                            severity="error",
                        )
                    )
                    continue

                source_id = clean_text(row.get("id"))
                submission_id = digest(
                    [revision_id, "opening_submission", phase, index, source_id]
                )

                def number(
                    row: dict[str, Any],
                    row_path: str,
                    field: str,
                    precision: int,
                    scale: int,
                ) -> str | None:
                    try:
                        value = parse_money(row.get(field))
                        if value is None:
                            return None

                        with localcontext() as ctx:
                            ctx.prec = 50
                            numeric = Decimal(value)
                            quantum = Decimal(1).scaleb(-scale)

                            if numeric != numeric.quantize(quantum):
                                raise ValueError("Excess decimal places")

                            if abs(numeric) >= Decimal(10) ** (precision - scale):
                                raise ValueError("Decimal precision exceeded")

                            return format(numeric.quantize(quantum), "f")

                    except (ValueError, TypeError, InvalidOperation):
                        issues.append(
                            QualityIssue(
                                issue_id=digest(
                                    [obs_id, "invalid_numeric", row_path, field]
                                ),
                                observation_id=obs_id,
                                code="invalid_numeric",
                                path=f"{row_path}.{field}",
                                severity="warn",
                            )
                        )
                        return None

                submission_time = None
                raw_time = row.get("createdDateBidOpen")

                if raw_time not in (None, ""):
                    try:
                        parsed = parse_iso_datetime(raw_time)
                        submission_time = parsed.isoformat() if parsed else None
                    except (ValueError, TypeError):
                        issues.append(
                            QualityIssue(
                                issue_id=digest(
                                    [obs_id, "invalid_submission_time", row_path]
                                ),
                                observation_id=obs_id,
                                code="invalid_submission_time",
                                path=f"{row_path}.createdDateBidOpen",
                                severity="warn",
                            )
                        )

                raw_json = canonical_json(row)

                attrs = {
                    "submission_id": submission_id,
                    "revision_id": revision_id,
                    "opening_revision_id": revision_id,
                    "source_submission_id": source_id,
                    "source_path": row_path,
                    "phase": phase,
                    "lot_no": clean_text(row.get("lotNo")),
                    "contractor_code": contractor_code,
                    "contractor_name": clean_text(row.get("contractorName")),
                    "is_consortium": None,
                    "venture_code": clean_text(row.get("ventureCode")),
                    "venture_name": clean_text(row.get("ventureName")),
                    "bid_price": number(row, row_path, "bidPrice", 38, 6),
                    "currency": clean_text(row.get("bidPriceUnit")),
                    "discount_rate": number(row, row_path, "saleNumber", 10, 4),
                    "final_bid_price": number(row, row_path, "bidFinalPrice", 38, 6),
                    "tech_result": clean_text(row.get("techResult")),
                    "tech_score": number(row, row_path, "techScore", 18, 6),
                    "submission_time": submission_time,
                    "raw_json": raw_json,
                }

                children.append(
                    ExtractedChild(
                        child_id=submission_id,
                        revision_id=revision_id,
                        kind="opening_submission",
                        role=phase,
                        path=row_path,
                        ordinal=index,
                        source_id=source_id,
                        payload_json=raw_json,
                        typed_table="opening_submission",
                        typed_attributes=attrs,
                    )
                )

        return children

    def _extract_ldt_lots(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
        phase_sources: list[tuple[str, str, str]],
    ) -> list[ExtractedChild]:
        """Extract LDT lot details without inventing submission relationships."""

        children: list[ExtractedChild] = []

        def issue(code: str, path: str) -> None:
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, code, path]),
                    observation_id=obs_id,
                    code=code,
                    path=path,
                    severity="warn",
                )
            )

        def decimal_value(
            value: Any,
            precision: int,
            scale: int,
            path: str,
        ) -> str | None:
            if value is None or value == "":
                return None

            try:
                parsed = parse_money(value)
                if parsed is None:
                    return None

                with localcontext() as ctx:
                    ctx.prec = 50
                    number = Decimal(parsed)
                    quantum = Decimal(1).scaleb(-scale)

                    if number != number.quantize(quantum):
                        raise ValueError("Excess decimal places")

                    if abs(number) >= Decimal(10) ** (precision - scale):
                        raise ValueError("Decimal precision exceeded")

                    return format(number.quantize(quantum), "f")

            except (ValueError, TypeError, InvalidOperation):
                issue("invalid_lot_numeric", path)
                return None

        for phase, part_name, _ in phase_sources:
            if not part_name.startswith("bid_open"):
                continue

            suffix = part_name.removeprefix("bid_open")
            lot_part = f"lot_open_detail{suffix}"
            lots = payload.get(lot_part)

            if not isinstance(lots, list):
                if lots is not None:
                    issue("invalid_lot_list", lot_part)
                continue

            submissions_path = (
                f"{part_name}."
                "bidSubmissionByContractorViewResponse.bidSubmissionDTOList"
            )
            submissions = lookup_path(payload, submissions_path)

            by_contractor: dict[str, list[str]] = {}

            if isinstance(submissions, list):
                for sub_index, submission in enumerate(submissions):
                    if not isinstance(submission, dict):
                        continue

                    code = clean_text(submission.get("contractorCode"))
                    if code is None:
                        continue

                    source_id = clean_text(submission.get("id"))
                    sub_id = digest(
                        [
                            revision_id,
                            "opening_submission",
                            phase,
                            sub_index,
                            source_id,
                        ]
                    )
                    by_contractor.setdefault(code, []).append(sub_id)

            for index, row in enumerate(lots):
                row_path = f"{lot_part}[{index}]"

                if not isinstance(row, dict):
                    issue("invalid_lot_row", row_path)
                    continue

                contractor_code = clean_text(row.get("contractorCode"))
                matching = (
                    by_contractor.get(contractor_code, [])
                    if contractor_code is not None
                    else []
                )
                submission_id = matching[0] if len(matching) == 1 else None

                lot_id = digest(
                    [
                        revision_id,
                        "opening_lot_submission",
                        phase,
                        row_path,
                    ]
                )

                passed_yn = None
                if row.get("passedYn") is not None:
                    try:
                        passed_yn = parse_boolean(row["passedYn"])
                    except (ValueError, TypeError):
                        issue("invalid_passed_yn", f"{row_path}.passedYn")

                rank = None
                raw_rank = row.get("ranking")
                if raw_rank is not None and raw_rank != "":
                    if isinstance(raw_rank, bool):
                        issue("invalid_evaluation_rank", f"{row_path}.ranking")
                    elif isinstance(raw_rank, int) and raw_rank >= 0:
                        rank = raw_rank
                    elif isinstance(raw_rank, str) and raw_rank.isdecimal():
                        rank = int(raw_rank)
                    else:
                        issue("invalid_evaluation_rank", f"{row_path}.ranking")

                attrs = {
                    "lot_submission_id": lot_id,
                    "revision_id": revision_id,
                    "submission_id": submission_id,
                    "phase": phase,
                    "source_path": row_path,
                    "source_lot_id": clean_text(row.get("id")),
                    "bid_no": clean_text(row.get("bidNo")),
                    "lot_no": clean_text(row.get("lotNo")),
                    "lot_name": clean_text(row.get("lotName")),
                    "contractor_code": contractor_code,
                    "contractor_name": clean_text(row.get("contractorName")),
                    "lot_price": decimal_value(
                        row.get("lotPrice"), 38, 6, f"{row_path}.lotPrice"
                    ),
                    "lot_final_price": decimal_value(
                        row.get("lotFinalPrice"), 38, 6, f"{row_path}.lotFinalPrice"
                    ),
                    "discount_rate": decimal_value(
                        row.get("discountPercent"), 10, 4, f"{row_path}.discountPercent"
                    ),
                    "currency": clean_text(row.get("lotPriceUnit")),
                    "passed_yn": passed_yn,
                    "tech_score": decimal_value(
                        row.get("techScore"), 18, 6, f"{row_path}.techScore"
                    ),
                    "evaluation_price": decimal_value(
                        row.get("evalPrice"), 38, 6, f"{row_path}.evalPrice"
                    ),
                    "evaluation_rank": rank,
                    "raw_json": canonical_json(row),
                }

                children.append(
                    ExtractedChild(
                        child_id=lot_id,
                        revision_id=revision_id,
                        kind="opening_lot_submission",
                        role=phase,
                        path=row_path,
                        ordinal=index,
                        source_id=attrs["source_lot_id"],
                        payload_json=attrs["raw_json"],
                        typed_table="opening_lot_submission",
                        typed_attributes=attrs,
                    )
                )

        return children

    def _extract_nested_lots(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
        phase_sources: list[tuple[str, str, str]],
        submission_children: list[ExtractedChild],
    ) -> list[ExtractedChild]:
        """Extract lots nested inside KHAC and ADB/WB submissions."""

        children: list[ExtractedChild] = []

        submission_ids = {
            (child.role, child.ordinal): child.child_id
            for child in submission_children
            if child.typed_table == "opening_submission"
        }

        def add_issue(code: str, path: str) -> None:
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, code, path]),
                    observation_id=obs_id,
                    code=code,
                    path=path,
                    severity="warn",
                )
            )

        def decimal_value(
            value: Any,
            precision: int,
            scale: int,
            path: str,
        ) -> str | None:
            if value is None or value == "":
                return None

            try:
                parsed = parse_money(value)
                if parsed is None:
                    return None

                with localcontext() as ctx:
                    ctx.prec = 50
                    numeric = Decimal(parsed)
                    quantum = Decimal(1).scaleb(-scale)

                    if numeric != numeric.quantize(quantum):
                        raise ValueError("Excess decimal places")

                    if abs(numeric) >= Decimal(10) ** (precision - scale):
                        raise ValueError("Precision exceeded")

                    return format(numeric.quantize(quantum), "f")

            except (TypeError, ValueError, InvalidOperation):
                add_issue("invalid_lot_numeric", path)
                return None

        for phase, part_name, _ in phase_sources:
            list_path = (
                f"{part_name}."
                "bidSubmissionByContractorViewResponse.bidSubmissionDTOList"
            )
            submissions = lookup_path(payload, list_path)

            if not isinstance(submissions, list):
                continue

            for sub_index, submission in enumerate(submissions):
                if not isinstance(submission, dict):
                    continue

                lots = submission.get("lotNoValueDTOList")

                if lots is None:
                    continue

                if not isinstance(lots, list):
                    add_issue(
                        "invalid_nested_lot_list",
                        f"{list_path}[{sub_index}].lotNoValueDTOList",
                    )
                    continue

                parent_submission_id = submission_ids.get((phase, sub_index))

                expanded_lots: list[tuple[dict[str, Any], dict[str, Any], str]] = []

                for parent_index, parent in enumerate(lots):
                    parent_path = (
                        f"{list_path}[{sub_index}]"
                        f".lotNoValueDTOList[{parent_index}]"
                    )

                    if not isinstance(parent, dict):
                        add_issue("invalid_nested_lot", parent_path)
                        continue

                    views = parent.get("bidOpenView")

                    if views is None or views == []:
                        expanded_lots.append((parent, parent, parent_path))
                        continue

                    if not isinstance(views, list):
                        add_issue(
                            "invalid_bid_open_view",
                            f"{parent_path}.bidOpenView",
                        )
                        continue

                    for view_index, view in enumerate(views):
                        view_path = f"{parent_path}.bidOpenView[{view_index}]"

                        if not isinstance(view, dict):
                            add_issue("invalid_bid_open_view", view_path)
                            continue

                        expanded_lots.append((view, parent, view_path))

                seen_lot_numbers: set[str] = set()

                for lot_index, (lot, parent_lot, lot_path) in enumerate(expanded_lots):
                    lot_no = clean_text(lot.get("lotNo"))

                    if lot_no is not None and lot_no in seen_lot_numbers:
                        add_issue("duplicate_nested_lot", lot_path)
                        continue

                    if lot_no is not None:
                        seen_lot_numbers.add(lot_no)

                    lot_id = digest(
                        [
                            revision_id,
                            "opening_lot_submission",
                            phase,
                            lot_path,
                        ]
                    )

                    passed_yn = None
                    if lot.get("passedYn") is not None:
                        try:
                            passed_yn = parse_boolean(lot["passedYn"])
                        except (TypeError, ValueError):
                            add_issue(
                                "invalid_passed_yn",
                                f"{lot_path}.passedYn",
                            )

                    rank = None
                    raw_rank = lot.get("ranking")

                    if isinstance(raw_rank, int) and not isinstance(raw_rank, bool):
                        if raw_rank >= 0:
                            rank = raw_rank
                        else:
                            add_issue("invalid_evaluation_rank", lot_path)
                    elif isinstance(raw_rank, str) and raw_rank.isdecimal():
                        rank = int(raw_rank)
                    elif raw_rank not in (None, ""):
                        add_issue("invalid_evaluation_rank", lot_path)

                    contractor_code = clean_text(submission.get("contractorCode"))

                    lot_contractor_code = clean_text(
                        lot.get("contractorCode") or parent_lot.get("contractorCode")
                    )

                    if (
                        lot_contractor_code is not None
                        and lot_contractor_code != contractor_code
                    ):
                        add_issue("lot_contractor_mismatch", lot_path)
                        linked_submission_id = None
                    else:
                        linked_submission_id = parent_submission_id

                    bid_open_id = clean_text(lot.get("bidOpenId"))
                    source_submission_id = clean_text(submission.get("id"))

                    if bid_open_id is not None and bid_open_id != source_submission_id:
                        add_issue("lot_bid_open_id_mismatch", lot_path)
                        linked_submission_id = None

                    attrs = {
                        "lot_submission_id": lot_id,
                        "revision_id": revision_id,
                        "submission_id": linked_submission_id,
                        "phase": phase,
                        "source_path": lot_path,
                        "source_lot_id": clean_text(lot.get("id")),
                        "bid_no": clean_text(
                            lot.get("bidNo") or parent_lot.get("bidNo")
                        ),
                        "lot_no": lot_no,
                        "lot_name": clean_text(lot.get("lotName")),
                        "contractor_code": lot_contractor_code or contractor_code,
                        "contractor_name": clean_text(lot.get("contractorName"))
                        or clean_text(submission.get("contractorName")),
                        "lot_price": decimal_value(
                            lot.get("lotPrice"),
                            38,
                            6,
                            f"{lot_path}.lotPrice",
                        ),
                        "lot_final_price": decimal_value(
                            lot.get("lotFinalPrice"),
                            38,
                            6,
                            f"{lot_path}.lotFinalPrice",
                        ),
                        "discount_rate": decimal_value(
                            lot.get(
                                "discountPercent"
                                if "discountPercent" in lot
                                else "saleNumber"
                            ),
                            10,
                            4,
                            f"{lot_path}.discount",
                        ),
                        "currency": clean_text(
                            lot.get("lotPriceUnit") or parent_lot.get("lotPriceUnit")
                        ),
                        "passed_yn": passed_yn,
                        "tech_score": decimal_value(
                            lot.get("techScore"),
                            18,
                            6,
                            f"{lot_path}.techScore",
                        ),
                        "evaluation_price": decimal_value(
                            lot.get("evalPrice"),
                            38,
                            6,
                            f"{lot_path}.evalPrice",
                        ),
                        "evaluation_rank": rank,
                        "raw_json": canonical_json(lot),
                    }

                    children.append(
                        ExtractedChild(
                            child_id=lot_id,
                            revision_id=revision_id,
                            kind="opening_lot_submission",
                            role=phase,
                            path=lot_path,
                            ordinal=lot_index,
                            source_id=attrs["source_lot_id"],
                            payload_json=attrs["raw_json"],
                            typed_table="opening_lot_submission",
                            typed_attributes=attrs,
                        )
                    )

        self._reconcile_auxiliary_lots(
            payload,
            phase_sources,
            children,
            submission_children,
            issues,
            obs_id,
        )
        return children

    def _reconcile_auxiliary_lots(
        self,
        payload: dict[str, Any],
        phase_sources: list[tuple[str, str, str]],
        lot_children: list[ExtractedChild],
        submission_children: list[ExtractedChild],
        issues: list[QualityIssue],
        obs_id: str,
    ) -> None:
        """Reconcile auxiliary lots without duplicating bidder-lot records."""

        by_pair: dict[tuple[str, str, str], list[ExtractedChild]] = {}
        by_source_submission: dict[tuple[str, str], list[ExtractedChild]] = {}

        def add_issue(code: str, path: str, severity: str = "error") -> None:
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, code, path]),
                    observation_id=obs_id,
                    code=code,
                    path=path,
                    severity=severity,
                )
            )

        for child in submission_children:
            if child.typed_table != "opening_submission":
                continue
            if child.source_id is not None:
                key = (child.role, child.source_id)
                by_source_submission.setdefault(key, []).append(child)

        for child in lot_children:
            attrs = child.typed_attributes
            if child.typed_table != "opening_lot_submission" or attrs is None:
                continue

            lot_no = clean_text(attrs.get("lot_no"))
            contractor = clean_text(attrs.get("contractor_code"))

            if lot_no is None or contractor is None:
                continue

            key = (child.role, lot_no, contractor)
            by_pair.setdefault(key, []).append(child)

        missing = object()

        def normalize(value: Any, field: str, scale: int | None) -> Any:
            if value is None or value == "":
                return None

            if field == "passed_yn":
                return parse_boolean(value)

            if field == "evaluation_rank":
                if isinstance(value, bool):
                    raise ValueError("Invalid rank")
                if isinstance(value, int) and 0 <= value < 2**63:
                    return value
                if (
                    isinstance(value, str)
                    and value.strip().isdecimal()
                    and int(value.strip()) < 2**63
                ):
                    return int(value.strip())
                raise ValueError("Invalid rank")

            if scale is not None:
                parsed = parse_money(value)
                if parsed is None:
                    return None

                with localcontext() as ctx:
                    ctx.prec = 50
                    numeric = Decimal(parsed)
                    quantum = Decimal(1).scaleb(-scale)

                    if numeric != numeric.quantize(quantum):
                        raise ValueError("Excess decimal places")

                    if field == "discount_rate" and abs(numeric) >= 10**6:
                        raise ValueError("Discount precision exceeded")

                    if field == "tech_score" and abs(numeric) >= 10**12:
                        raise ValueError("Score precision exceeded")

                    return format(numeric.quantize(quantum), "f")

            return clean_text(value)

        fields = (
            ("lot_price", "lotPrice", 6),
            ("lot_final_price", "lotFinalPrice", 6),
            ("discount_rate", "discountPercent", 4),
            ("currency", "lotPriceUnit", None),
            ("passed_yn", "passedYn", None),
            ("tech_score", "techScore", 6),
            ("evaluation_price", "evalPrice", 6),
            ("evaluation_rank", "ranking", None),
        )

        for phase, part_name, _ in phase_sources:
            path = (
                f"{part_name}."
                "bidSubmissionByContractorViewResponse."
                "bidoLotOpenDetailDTOS"
            )

            container = lookup_path(
                payload,
                path.rsplit(".", 1)[0],
            )

            if not isinstance(container, dict):
                continue

            if "bidoLotOpenDetailDTOS" not in container:
                continue

            details = container["bidoLotOpenDetailDTOS"]

            if details is None:
                continue

            if not isinstance(details, list):
                add_issue("invalid_auxiliary_lot_list", path)
                continue

            seen: set[tuple[str | None, str, str]] = set()

            for index, detail in enumerate(details):
                detail_path = f"{path}[{index}]"

                if not isinstance(detail, dict):
                    add_issue("invalid_auxiliary_lot", detail_path)
                    continue

                lot_no = clean_text(detail.get("lotNo"))
                contractor = clean_text(detail.get("contractorCode"))
                bid_open_id = clean_text(detail.get("bidOpenId"))

                if lot_no is None or contractor is None:
                    add_issue("missing_auxiliary_lot_identity", detail_path)
                    continue

                identity = (bid_open_id, lot_no, contractor)

                if identity in seen:
                    add_issue("duplicate_auxiliary_lot", detail_path)
                    continue

                seen.add(identity)

                candidates = by_pair.get((phase, lot_no, contractor), [])

                if bid_open_id is not None:
                    same_phase = [
                        sub
                        for sub in by_source_submission.get((phase, bid_open_id), [])
                        if sub.typed_attributes is not None
                        and sub.typed_attributes.get("contractor_code") == contractor
                    ]

                    if len(same_phase) > 1:
                        add_issue("ambiguous_auxiliary_submission", detail_path)
                        continue

                    if len(same_phase) == 1:
                        expected_id = same_phase[0].child_id
                        candidates = [
                            child
                            for child in candidates
                            if child.typed_attributes is not None
                            and child.typed_attributes.get("submission_id")
                            == expected_id
                        ]

                    elif phase == "financial":
                        # Financial auxiliary data may reference
                        # the original technical submission ID.
                        technical_sources = [
                            sub
                            for sub in by_source_submission.get(
                                ("technical", bid_open_id), []
                            )
                            if sub.typed_attributes is not None
                            and sub.typed_attributes.get("contractor_code")
                            == contractor
                        ]

                        technical_lots = [
                            child
                            for child in by_pair.get(
                                ("technical", lot_no, contractor), []
                            )
                            if child.typed_attributes is not None
                        ]

                        technical_attrs = (
                            technical_lots[0].typed_attributes
                            if len(technical_lots) == 1
                            else None
                        )

                        verified_technical_link = (
                            len(technical_sources) == 1
                            and technical_attrs is not None
                            and technical_attrs.get("submission_id")
                            == technical_sources[0].child_id
                        )

                        if not verified_technical_link:
                            add_issue("unmatched_auxiliary_lot", detail_path)
                            continue

                        if not candidates:
                            # Present in technical, but not represented
                            # as a financial submission.
                            add_issue(
                                "auxiliary_technical_only_reference",
                                detail_path,
                                severity="warn",
                            )
                            continue

                        # Candidate must still match the current
                        # financial phase, lot number and contractor.
                        # Cardinality is checked below.

                    else:
                        add_issue("unmatched_auxiliary_lot", detail_path)
                        continue

                if not candidates:
                    add_issue("unmatched_auxiliary_lot", detail_path)
                    continue

                if len(candidates) != 1:
                    add_issue("ambiguous_auxiliary_lot", detail_path)
                    continue

                attrs = candidates[0].typed_attributes
                if attrs is None:
                    add_issue("invalid_auxiliary_target", detail_path)
                    continue

                updates: dict[str, Any] = {}
                has_conflict = False

                for target_field, source_field, scale in fields:
                    raw_value = detail.get(source_field)

                    if raw_value is None or raw_value == "":
                        continue

                    field_path = f"{detail_path}.{source_field}"

                    try:
                        value = normalize(raw_value, target_field, scale)
                    except (ValueError, TypeError, InvalidOperation):
                        add_issue("invalid_auxiliary_field", field_path)
                        has_conflict = True
                        continue

                    if value is None:
                        continue

                    existing = attrs.get(target_field, missing)

                    if existing is missing or existing is None:
                        updates[target_field] = value

                    elif existing != value:
                        # Technical prices can be zero placeholders.
                        # Only accept this difference when a unique
                        # financial lot confirms the auxiliary value.
                        if (
                            phase == "technical"
                            and target_field
                            in {
                                "lot_price",
                                "lot_final_price",
                                "discount_rate",
                            }
                            and Decimal(str(existing)) == 0
                            and Decimal(str(value)) > 0
                        ):
                            financial_lots = by_pair.get(
                                ("financial", lot_no, contractor), []
                            )

                            if len(financial_lots) == 1:
                                financial_attrs = financial_lots[0].typed_attributes

                                confirmed = (
                                    financial_attrs is not None
                                    and financial_attrs.get(target_field) == value
                                )

                                # If financial has no explicit discount rate,
                                # verify it using the published financial prices.
                                if (
                                    not confirmed
                                    and financial_attrs is not None
                                    and target_field == "discount_rate"
                                    and financial_attrs.get("discount_rate") is None
                                    and financial_attrs.get("lot_price") is not None
                                    and financial_attrs.get("lot_final_price")
                                    is not None
                                ):
                                    with localcontext() as ctx:
                                        ctx.prec = 50

                                        financial_price = Decimal(
                                            str(financial_attrs["lot_price"])
                                        )
                                        financial_final = Decimal(
                                            str(financial_attrs["lot_final_price"])
                                        )

                                        if (
                                            financial_price > 0
                                            and 0 <= financial_final <= financial_price
                                        ):
                                            calculated_discount = (
                                                (financial_price - financial_final)
                                                * Decimal(100)
                                                / financial_price
                                            ).quantize(Decimal("0.0001"))

                                            confirmed = calculated_discount == Decimal(
                                                str(value)
                                            )

                                if confirmed:
                                    add_issue(
                                        "technical_zero_vs_financial_value",
                                        field_path,
                                        severity="warn",
                                    )
                                    continue

                        add_issue(
                            "conflicting_auxiliary_lot",
                            field_path,
                        )
                        has_conflict = True

                if not has_conflict and updates:
                    attrs.update(updates)
                    add_issue(
                        "auxiliary_lot_enriched",
                        detail_path,
                        severity="warn",
                    )
