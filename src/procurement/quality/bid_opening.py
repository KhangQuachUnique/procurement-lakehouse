"""Validate the assembly without rewriting any of its source responses."""

import json
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from procurement.common.dates import VIETNAM_TZ
from procurement.quality.contracts import finish, issue, lookup, validate_detail
from procurement.quality.opening_phases import (
    financial_published,
    opening_parts,
)

KHAC_ROOTS = [
    "opening_out.bidoNotifyContractorP",
    "opening_out.bidoBidroundOutMngViewDTO",
]

KHAC_SUBMISSIONS = (
    "opening_out.bidSubmissionByContractorViewResponse" ".bidSubmissionDTOList"
)


def _khac_cancelled_multilot_technical_verified(details, submissions, notice, context):
    """Verify unevaluated technical lot rows against bidder-level views."""
    if not isinstance(details, list) or not details:
        return False

    expected_pairs = set()
    bidder_codes = {}

    for bidder in submissions:
        if not isinstance(bidder, dict):
            return False

        bid_open_id = bidder.get("id")
        code = bidder.get("contractorCode")

        if (
            not isinstance(bid_open_id, str)
            or not bid_open_id
            or not isinstance(code, str)
            or not code
            or bid_open_id in bidder_codes
        ):
            return False

        bidder_codes[bid_open_id] = code

        package_lots = bidder.get("lotNoValueDTOList")
        if not isinstance(package_lots, list) or len(package_lots) != 1:
            return False

        package_lot = package_lots[0]
        if not isinstance(package_lot, dict):
            return False

        views = package_lot.get("bidOpenView")
        if not isinstance(views, list) or not views:
            return False

        for view in views:
            if not isinstance(view, dict):
                return False

            lot_no = view.get("lotNo")

            if (
                not isinstance(lot_no, str)
                or not lot_no
                or view.get("bidOpenId") != bid_open_id
                or view.get("bidNo") != notice["bidNo"]
                or view.get("contractorCode") != code
                or view.get("lotPriceUnit") != bidder.get("bidPriceUnit")
            ):
                return False

            # The nested lot must not contain evaluated outcomes.
            if any(
                view.get(field) is not None
                for field in (
                    "passedYn",
                    "techScore",
                    "status",
                    "result",
                    "evalType",
                    "bidLotDetail",
                )
            ):
                return False

            if any(
                isinstance(view.get(field), bool) or view.get(field) not in (None, 0)
                for field in (
                    "lotPrice",
                    "lotFinalPrice",
                    "discountPercent",
                )
            ):
                return False

            pair = (bid_open_id, lot_no, code)
            if pair in expected_pairs:
                return False

            expected_pairs.add(pair)

    # Only accept the verified multi-lot shape.
    if len({lot_no for _, lot_no, _ in expected_pairs}) < 2:
        return False

    actual_pairs = set()
    detail_ids = set()

    for row in details:
        if not isinstance(row, dict):
            return False

        row_id = row.get("id")
        bid_open_id = row.get("bidOpenId")
        lot_no = row.get("lotNo")
        code = row.get("contractorCode")

        if (
            not isinstance(row_id, str)
            or not row_id
            or row_id in detail_ids
            or bid_open_id not in bidder_codes
            or bidder_codes[bid_open_id] != code
            or not isinstance(lot_no, str)
            or not lot_no
            or row.get("notifyId") != context.get("id")
            or row.get("bidNo") != notice["bidNo"]
        ):
            return False

        if any(
            row.get(field) is not None
            for field in (
                "eligibilityYn",
                "experienceYn",
                "passedYn",
                "evalPrice",
                "ranking",
                "succBidderYn",
                "succBidderPrice",
                "techScore",
                "noPassedRson",
                "noSuccBidderRson",
            )
        ):
            return False

        if any(
            isinstance(row.get(field), bool) or row.get(field) not in (None, 0)
            for field in (
                "lotPrice",
                "lotFinalPrice",
                "discountPercent",
            )
        ):
            return False

        detail_ids.add(row_id)
        pair = (bid_open_id, lot_no, code)

        if pair in actual_pairs:
            return False

        actual_pairs.add(pair)

    # Both representations must contain exactly the same
    # bidder-lot relationships.
    return actual_pairs == expected_pairs


def _khac_cancelled_verified_pattern(
    payload,
    context,
    *,
    priced_financial=False,
    mixed_timeline=False,
):
    """Verify a narrow, source-backed KHAC cancellation pattern in both phases."""

    if (
        context.get("processApply") != "KHAC"
        or context.get("bidMode") != "1_HTHS"
        or not isinstance(payload, dict)
        or set(payload)
        != {
            "opening_out_technical",
            "opening_out_financial",
        }
    ):
        return False

    technical = payload["opening_out_technical"]
    financial = payload["opening_out_financial"]

    notices = []
    rounds = []

    # Verify both responses independently.
    for part in (technical, financial):
        if (
            not isinstance(part, dict)
            or part.get("error")
            or part.get("success") is False
        ):
            return False

        notice = part.get("bidoNotifyContractorP")
        round_info = part.get("bidoBidroundOutMngViewDTO")

        if not isinstance(notice, dict) or not isinstance(round_info, dict):
            return False

        for root in (notice, round_info):
            if any(
                not context.get(key) or root.get(key) != context.get(key)
                for key in ("id", "notifyNo", "notifyVersion")
            ):
                return False

            if root.get("processApply") != "KHAC" or root.get("bidMode") != "1_HTHS":
                return False

        if not notice.get("bidNo") or round_info.get("bidNo") != notice["bidNo"]:
            return False

        notices.append(notice)
        rounds.append(round_info)

    original, financial_notice = notices
    technical_round, financial_round = rounds

    if original["bidNo"] != financial_notice["bidNo"]:
        return False

    # Both endpoints must report the same cancellation.
    cancellation_fields = (
        "cancelDate",
        "cancelDecisionDate",
        "cancelDecisionNo",
        "cancelReason",
        "cancelFileAttachName",
    )

    for field in cancellation_fields:
        if not original.get(field) or original[field] != financial_notice.get(field):
            return False

    # Restrict this rule to the verified notice status.
    if original.get("status") != "03" or financial_notice.get("status") != "03":
        return False

    scheduled = _event_time(original.get("bidOpenDate"))
    cancelled = _event_time(original.get("cancelDate"))
    decision = _event_time(original.get("cancelDecisionDate"))

    opened_technical = _event_time(technical_round.get("successBidOpenDate"))
    opened_financial = _event_time(technical_round.get("successBidOpenDateTc"))

    if (
        scheduled is None
        or cancelled is None
        or decision is None
        or opened_technical is None
        or opened_financial is None
    ):
        return False

    # This particular source anomaly:
    # scheduled opening < cancellation < recorded opening timestamps.
    if mixed_timeline:
        # Verified anomalous KHAC timeline:
        # financial marker < cancellation decision <= cancellation
        # < technical marker.
        chronology_verified = (
            priced_financial
            and scheduled < opened_financial < decision
            and decision <= cancelled < opened_technical
        )
    else:
        chronology_verified = (
            scheduled < cancelled
            and decision <= cancelled < opened_technical < opened_financial
        )

    if not chronology_verified:
        return False

    # Round timestamps must be consistent across both responses.
    for field in ("successBidOpenDate", "successBidOpenDateTc"):
        if technical_round.get(field) != financial_round.get(field):
            return False

    technical_container = technical.get("bidSubmissionByContractorViewResponse")
    financial_container = financial.get("bidSubmissionByContractorViewResponse")

    if not isinstance(technical_container, dict) or not isinstance(
        financial_container, dict
    ):
        return False

    # Auxiliary evaluation collections must be explicitly null, not absent.
    for field in ("bidoLotOpenDetailDTOS", "bidOpenAdditionList"):
        if field not in financial_container or financial_container[field] is not None:
            return False

    if "bidSubmissionDTOList" not in financial_container:
        return False
    financial_submissions = financial_container["bidSubmissionDTOList"]
    if priced_financial:
        # Keep the existing narrowly verified one-bidder priced-financial case.
        if (
            not isinstance(financial_submissions, list)
            or len(financial_submissions) != 1
        ):
            return False
    elif financial_submissions is not None:
        # A cancelled unevaluated financial phase must explicitly be null.
        return False

    # Additional technical results must not exist.
    if (
        "bidOpenAdditionList" not in technical_container
        or technical_container["bidOpenAdditionList"] is not None
        or "bidoLotOpenDetailDTOS" not in technical_container
    ):
        return False

    technical_details = technical_container["bidoLotOpenDetailDTOS"]

    # Non-null technical lot details are supported only for
    # a strictly verified unevaluated multi-lot cancellation.
    if technical_details is not None and (
        priced_financial
        or not isinstance(technical_details, list)
        or not technical_details
    ):
        return False

    submissions = technical_container.get("bidSubmissionDTOList")
    if not isinstance(submissions, list) or not submissions:
        return False

    # For a null financial phase, both endpoints must agree with the complete
    # technical bidder list. The separately verified priced case stays single.
    if priced_financial and len(submissions) != 1:
        return False

    count = len(submissions)
    for round_info in (technical_round, financial_round):
        declared = round_info.get("countContractor")
        if (
            not isinstance(declared, int)
            or isinstance(declared, bool)
            or declared != count
        ):
            return False

    seen_submission_ids = set()
    seen_contractor_codes = set()
    for bidder in submissions:
        if not isinstance(bidder, dict):
            return False

        submission_id = bidder.get("id")
        contractor_code = bidder.get("contractorCode")
        if (
            not isinstance(submission_id, str)
            or not submission_id
            or not isinstance(contractor_code, str)
            or not contractor_code
            or submission_id in seen_submission_ids
            or contractor_code in seen_contractor_codes
        ):
            return False
        seen_submission_ids.add(submission_id)
        seen_contractor_codes.add(contractor_code)

        if any(
            bidder.get(field) is not None
            for field in ("techResult", "techScore", "createdDateBidOpen")
        ):
            return False

        if any(
            bidder.get(field) not in (None, 0)
            for field in ("bidPrice", "bidFinalPrice")
        ):
            return False

        lots = bidder.get("lotNoValueDTOList")
        if not isinstance(lots, list) or len(lots) != 1:
            return False
        lot = lots[0]
        if not isinstance(lot, dict):
            return False

        # The duplicate identifiers may be explicitly null in source data.
        # When present, they must agree with the notice / enclosing bidder.
        if (
            lot.get("lotNo") != original["bidNo"]
            or "bidNo" not in lot
            or "contractorCode" not in lot
            or lot["bidNo"] not in (None, original["bidNo"])
            or lot["contractorCode"] not in (None, contractor_code)
        ):
            return False

        # No evaluation outcome should be present in the technical lot.
        for field in (
            "passedYn",
            "techResult",
            "techScore",
            "evalPrice",
            "ranking",
            "succBidderYn",
        ):
            if lot.get(field) is not None:
                return False

        if any(
            lot.get(field) not in (None, 0) for field in ("lotPrice", "lotFinalPrice")
        ):
            return False

    if isinstance(
        technical_details, list
    ) and not _khac_cancelled_multilot_technical_verified(
        technical_details,
        submissions,
        original,
        context,
    ):
        return False

    if not priced_financial:
        return True

    # There is exactly one technical bidder in the priced-financial case.
    bidder = submissions[0]

    # Financial submissions must contain exactly one bidder.
    if not isinstance(financial_submissions, list) or len(financial_submissions) != 1:
        return False

    # The source may publish a real financial-level bid despite a notice
    # whose cancellation date predates recorded opening timestamps. This is
    # NOT an unevaluated/null-financial case: retain the positive bid price.
    financial_bidder = financial_submissions[0]
    if (
        not isinstance(financial_bidder, dict)
        or not financial_bidder.get("id")
        or financial_bidder.get("contractorCode") != bidder["contractorCode"]
        or any(
            financial_bidder.get(key) is not None
            for key in ("techResult", "techScore", "createdDateBidOpen")
        )
    ):
        return False

    def finite_decimal(value):
        if value is None or isinstance(value, bool):
            return None
        try:
            parsed = Decimal(str(value))
        except (ValueError, InvalidOperation):
            return None
        return parsed if parsed.is_finite() else None

    price = finite_decimal(financial_bidder.get("bidPrice"))
    final_price = finite_decimal(financial_bidder.get("bidFinalPrice"))
    discount = finite_decimal(financial_bidder.get("saleNumber"))
    currency = financial_bidder.get("bidPriceUnit")
    if (
        price is None
        or price <= 0
        or final_price != price
        or discount != 0
        or not isinstance(currency, str)
        or not currency.strip()
    ):
        return False

    financial_lots = financial_bidder.get("lotNoValueDTOList")
    if not isinstance(financial_lots, list) or len(financial_lots) != 1:
        return False
    financial_lot = financial_lots[0]
    if not isinstance(financial_lot, dict):
        return False

    if (
        financial_lot.get("lotNo") != original["bidNo"]
        or "bidNo" not in financial_lot
        or "contractorCode" not in financial_lot
        or financial_lot["bidNo"] not in (None, original["bidNo"])
        or financial_lot["contractorCode"] not in (None, bidder["contractorCode"])
        or financial_lot.get("lotPriceUnit") != currency
        or finite_decimal(financial_lot.get("lotPrice")) != 0
        or finite_decimal(financial_lot.get("lotFinalPrice")) != 0
        or finite_decimal(financial_lot.get("saleNumber")) != 0
    ):
        return False

    # Do not classify a genuinely evaluated or awarded financial lot as
    # this limited source anomaly.
    return not any(
        financial_lot.get(key) is not None
        for key in (
            "passedYn",
            "techResult",
            "techScore",
            "evalPrice",
            "ranking",
            "succBidderYn",
            "succBidderPrice",
        )
    )


def _khac_cancelled_without_verified_evaluation(payload, context):
    return _khac_cancelled_verified_pattern(payload, context)


def _khac_cancelled_with_priced_financial(payload, context):
    return _khac_cancelled_verified_pattern(payload, context, priced_financial=True)


def _khac_cancelled_with_priced_financial_mixed_timeline(payload, context):
    return _khac_cancelled_verified_pattern(
        payload,
        context,
        priced_financial=True,
        mixed_timeline=True,
    )


def _khac_financial_null_marker_conflict(payload, context):
    """Verify a KHAC opening with a stale financial marker and no financial bids."""

    if (
        context.get("processApply") != "KHAC"
        or context.get("bidMode") != "1_HTHS"
        or not isinstance(payload, dict)
        or set(payload)
        != {
            "opening_out_technical",
            "opening_out_financial",
        }
    ):
        return False

    technical = payload["opening_out_technical"]
    financial = payload["opening_out_financial"]

    if not isinstance(technical, dict):
        return False
    if not isinstance(financial, dict):
        return False

    for part in (technical, financial):
        if part.get("error") or part.get("success") is False:
            return False

    tech_notice = technical.get("bidoNotifyContractorP")
    tech_round = technical.get("bidoBidroundOutMngViewDTO")
    tech_container = technical.get("bidSubmissionByContractorViewResponse")

    fin_notice = financial.get("bidoNotifyContractorP")
    fin_round = financial.get("bidoBidroundOutMngViewDTO")
    fin_container = financial.get("bidSubmissionByContractorViewResponse")

    # Explicit type narrowing for Pylance.
    if not isinstance(tech_notice, dict):
        return False
    if not isinstance(tech_round, dict):
        return False
    if not isinstance(tech_container, dict):
        return False
    if not isinstance(fin_notice, dict):
        return False
    if not isinstance(fin_round, dict):
        return False
    if not isinstance(fin_container, dict):
        return False

    bid_no = tech_notice.get("bidNo")
    if not isinstance(bid_no, str) or not bid_no:
        return False

    # Verify notice identities and business workflow.
    for root in (tech_notice, tech_round, fin_notice, fin_round):
        if any(
            not context.get(key) or root.get(key) != context.get(key)
            for key in ("id", "notifyNo", "notifyVersion")
        ):
            return False

        if (
            root.get("processApply") != "KHAC"
            or root.get("bidMode") != "1_HTHS"
            or root.get("bidNo") != bid_no
        ):
            return False

    # Neither response may indicate a cancellation.
    for notice in (tech_notice, fin_notice):
        if notice.get("status") != "01":
            return False

        for field in (
            "cancelDate",
            "cancelDecisionDate",
            "cancelDecisionNo",
            "cancelReason",
        ):
            if field not in notice or notice[field] is not None:
                return False

        if notice.get("cancelFileAttachName") is not None:
            return False

    if tech_notice.get("bidOpenDate") != fin_notice.get("bidOpenDate"):
        return False

    # Both API phases must agree on the round metadata.
    for field in (
        "successBidOpenDate",
        "successBidOpenDateTc",
        "countContractor",
        "bidStatus",
        "evalStatus",
        "isMultiLot",
    ):
        if tech_round.get(field) != fin_round.get(field):
            return False

    for round_info in (tech_round, fin_round):
        if (
            type(round_info.get("countContractor")) is not int
            or round_info["countContractor"] != 1
            or round_info.get("bidStatus") != "OPEN_DXKT"
            or round_info.get("evalStatus") is not None
            or round_info.get("isMultiLot") is not None
        ):
            return False

    # The financial marker predates the scheduled technical opening.
    scheduled = _event_time(tech_notice.get("bidOpenDate"))
    technical_at = _event_time(tech_round.get("successBidOpenDate"))
    financial_marker = _event_time(tech_round.get("successBidOpenDateTc"))

    if (
        scheduled is None
        or technical_at is None
        or financial_marker is None
        or not (financial_marker < scheduled <= technical_at)
    ):
        return False

    # Explicit nulls, not missing keys or empty lists.
    for key in (
        "bidSubmissionDTOList",
        "bidoLotOpenDetailDTOS",
        "bidOpenAdditionList",
    ):
        if key not in fin_container or fin_container[key] is not None:
            return False

    # No auxiliary technical evaluation results.
    for key in ("bidoLotOpenDetailDTOS", "bidOpenAdditionList"):
        if key not in tech_container or tech_container[key] is not None:
            return False

    submissions = tech_container.get("bidSubmissionDTOList")
    if not isinstance(submissions, list) or len(submissions) != 1:
        return False

    bidder = submissions[0]
    if not isinstance(bidder, dict):
        return False

    if (
        not bidder.get("id")
        or not bidder.get("contractorCode")
        or bidder.get("bidPrice") != 0
        or bidder.get("bidFinalPrice") not in (None, 0)
        or bidder.get("techResult") is not None
        or bidder.get("techScore") is not None
    ):
        return False

    lots = bidder.get("lotNoValueDTOList")
    if not isinstance(lots, list) or len(lots) != 1:
        return False

    lot = lots[0]
    if not isinstance(lot, dict):
        return False

    if (
        lot.get("lotNo") != bid_no
        or lot.get("bidNo") not in (None, bid_no)
        or lot.get("contractorCode")
        not in (
            None,
            bidder["contractorCode"],
        )
        or lot.get("lotPrice") != 0
        or lot.get("lotFinalPrice") not in (None, 0)
    ):
        return False

    # Reject any observed evaluation or award result.
    return not any(
        lot.get(field) is not None
        for field in (
            "passedYn",
            "techResult",
            "techScore",
            "evalPrice",
            "ranking",
            "succBidderYn",
            "succBidderPrice",
        )
    )


def validate_khac_assembly(payload, context, contract, *, thresholds=None):
    mode = context.get("bidMode")

    # 1. Determine the primary component
    if mode == "1_MTHS":
        primary = "opening_out"
    elif mode == "1_HTHS":
        primary = "opening_out_technical"
    else:
        raise ValueError("unsupported_khac_opening_mode")

    root_names = (
        "bidoNotifyContractorP",
        "bidoBidroundOutMngViewDTO",
    )

    roots = [f"{primary}.{root_name}" for root_name in root_names]

    raw_contract = contract.model_copy(
        update={
            "assembly": False,
            "roots": roots,
        }
    )

    # 2. Validate primary identity
    result = validate_detail(
        payload,
        context,
        raw_contract,
        thresholds=thresholds,
    )

    problems = result["issues"]

    if not isinstance(payload, dict):
        return finish(result)

    if result["identity"].get("notifyVersion") is None:
        problems.append(
            issue(
                "missing_assembly_version",
                "fail",
                "notifyVersion",
            )
        )

    # 3. Determine expected components
    expected = {primary}
    pending = False

    if mode == "1_HTHS":
        round_info = lookup(
            payload,
            f"{primary}.bidoBidroundOutMngViewDTO",
        )

        try:
            published = financial_published(round_info)

        except (ValueError, TypeError) as exc:
            problems.append(
                issue(
                    "invalid_financial_publication_marker",
                    "fail",
                    f"{primary}.bidoBidroundOutMngViewDTO",
                    reason=str(exc),
                )
            )
            return finish(result)

        pending = not published

        if published:
            expected.add("opening_out_financial")

    if set(payload) != expected:
        problems.append(
            issue(
                "unexpected_opening_components",
                "fail",
                expected=sorted(expected),
                actual=sorted(payload),
            )
        )

    cancelled_no_evaluation = _khac_cancelled_without_verified_evaluation(
        payload, context
    )
    cancelled_priced_financial = (
        mode == "1_HTHS"
        and not cancelled_no_evaluation
        and _khac_cancelled_with_priced_financial(payload, context)
    )

    financial_null_conflict = (
        mode == "1_HTHS"
        and not cancelled_no_evaluation
        and not cancelled_priced_financial
        and _khac_financial_null_marker_conflict(payload, context)
    )

    cancelled_mixed_timeline = (
        mode == "1_HTHS"
        and not cancelled_no_evaluation
        and not cancelled_priced_financial
        and _khac_cancelled_with_priced_financial_mixed_timeline(payload, context)
    )

    # 4. Validate each component independently
    for part_name in sorted(expected):
        part = payload.get(part_name)

        if (
            not isinstance(part, dict)
            or part.get("error")
            or part.get("success") is False
        ):
            problems.append(
                issue(
                    "invalid_component",
                    "fail",
                    part_name,
                )
            )
            continue

        for root_name in root_names:
            root_path = f"{part_name}.{root_name}"
            root = lookup(payload, root_path)

            if not isinstance(root, dict) or not root:
                problems.append(
                    issue(
                        "missing_component_root",
                        "fail",
                        root_path,
                    )
                )
                continue

            # Compare this root with Search API identity
            checked = validate_detail(
                payload,
                context,
                contract.model_copy(
                    update={
                        "assembly": False,
                        "roots": [root_path],
                    }
                ),
                thresholds=thresholds,
            )

            problems.extend(checked["issues"])

            if root.get("bidMode") != mode:
                problems.append(
                    issue(
                        "opening_mode_mismatch",
                        "fail",
                        f"{root_path}.bidMode",
                    )
                )

            if root.get("processApply") != "KHAC":
                problems.append(
                    issue(
                        "process_apply_mismatch",
                        "fail",
                        f"{root_path}.processApply",
                    )
                )

        # 5. Validate bidder submissions
        submissions_path = (
            f"{part_name}."
            "bidSubmissionByContractorViewResponse."
            "bidSubmissionDTOList"
        )

        submissions = lookup(payload, submissions_path)

        # Financial submissions may be null only when the
        # cancellation has passed all strict business checks.
        if (
            (cancelled_no_evaluation or financial_null_conflict)
            and part_name == "opening_out_financial"
            and submissions is None
        ):
            continue

        if not isinstance(submissions, list) or any(
            not isinstance(row, dict) for row in submissions
        ):
            problems.append(
                issue(
                    "invalid_component_list",
                    "fail",
                    submissions_path,
                )
            )
            continue

        # Technical and single-envelope participant counts.
        # Financial participants may be a subset.
        if part_name != "opening_out_financial":
            count = lookup(
                payload,
                f"{part_name}." "bidoBidroundOutMngViewDTO.countContractor",
            )

            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                problems.append(
                    issue(
                        "invalid_contractor_count",
                        "fail",
                        part_name,
                    )
                )

            elif count != len(submissions):
                problems.append(
                    issue(
                        "contractor_count_mismatch",
                        "warn",
                        submissions_path,
                        expected=count,
                        actual=len(submissions),
                    )
                )

        # 6. Validate nested lot submissions
        for index, submission in enumerate(submissions):
            lots = submission.get("lotNoValueDTOList")

            if lots is not None and (
                not isinstance(lots, list)
                or any(not isinstance(row, dict) for row in lots)
            ):
                problems.append(
                    issue(
                        "invalid_lot_submission_list",
                        "fail",
                        f"{submissions_path}.{index}." "lotNoValueDTOList",
                    )
                )

    # A cancellation before the recorded technical opening requires a
    # strictly verified exception, even if the financial submission list
    # is syntactically valid (for example, []). Check each source response
    # independently so a mismatched cancellation cannot silently pass.
    if mode == "1_HTHS" and not (
        cancelled_no_evaluation
        or cancelled_priced_financial
        or cancelled_mixed_timeline
    ):
        technical_round = lookup(
            payload,
            "opening_out_technical.bidoBidroundOutMngViewDTO",
        )
        if isinstance(technical_round, dict):
            technical_at = _event_time(technical_round.get("successBidOpenDate"))
            if technical_at is not None:
                for part_name in (
                    "opening_out_technical",
                    "opening_out_financial",
                ):
                    notice = lookup(
                        payload,
                        f"{part_name}.bidoNotifyContractorP",
                    )
                    if not isinstance(notice, dict):
                        continue
                    cancelled_at = _event_time(notice.get("cancelDate"))
                    if cancelled_at is not None and cancelled_at < technical_at:
                        problems.append(
                            issue(
                                "unverified_cancelled_khac_opening",
                                "fail",
                                f"{part_name}.bidoNotifyContractorP.cancelDate",
                            )
                        )
                        break
    if cancelled_no_evaluation:
        problems.append(
            issue(
                "opening_dates_after_cancellation",
                "warn",
                "opening_out_technical.bidoBidroundOutMngViewDTO",
            )
        )
    elif cancelled_priced_financial:
        problems.append(
            issue(
                "cancelled_khac_priced_financial_after_cancellation",
                "warn",
                "opening_out_technical.bidoNotifyContractorP.cancelDate",
            )
        )
        problems.append(
            issue(
                "financial_bid_price_vs_zero_lot_price",
                "warn",
                "opening_out_financial.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
            )
        )
    elif cancelled_mixed_timeline:
        problems.append(
            issue(
                "cancelled_khac_mixed_opening_timeline",
                "warn",
                "opening_out_technical.bidoBidroundOutMngViewDTO",
            )
        )
        problems.append(
            issue(
                "financial_bid_price_vs_zero_lot_price",
                "warn",
                "opening_out_financial.bidSubmissionByContractorViewResponse.bidSubmissionDTOList",
            )
        )

    if financial_null_conflict:
        problems.append(
            issue(
                "khac_financial_null_with_early_publication_marker",
                "warn",
                "opening_out_financial."
                "bidSubmissionByContractorViewResponse."
                "bidSubmissionDTOList",
            )
        )

    result = finish(result)

    if result["status"] in {"pass", "warn"}:
        if cancelled_no_evaluation:
            result["collection_status"] = "cancelled_without_verified_evaluation"
        elif cancelled_priced_financial:
            result["collection_status"] = "cancelled_priced_financial_timeline_conflict"
        elif financial_null_conflict:
            result["collection_status"] = "financial_source_conflict"
        elif cancelled_mixed_timeline:
            result["collection_status"] = "cancelled_priced_financial_mixed_timeline"
        else:
            result["collection_status"] = (
                "awaiting_financial" if pending else "complete"
            )

    return result


def _event_time(value):
    """Interpret source event timestamps in Vietnam local time."""
    if not isinstance(value, str) or not value:
        return None

    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        return None

    if result.tzinfo is None:
        result = result.replace(tzinfo=VIETNAM_TZ)

    return result.astimezone(UTC)


def _financial_lots_are_technical_qualifiers(payload, context):
    """Verify financial lots only mirror qualified technical rows."""

    technical_lots = payload.get("lot_open_detail_technical")
    financial_lots = payload.get("lot_open_detail_financial")
    financial = payload.get("bid_open_financial")

    if (
        not isinstance(technical_lots, list)
        or not isinstance(financial_lots, list)
        or not financial_lots
        or not isinstance(financial, dict)
    ):
        return False

    if any(not isinstance(row, dict) for row in technical_lots + financial_lots):
        return False

    # Do not accept genuine financial submissions.
    for key in (
        "bidNoContractorResponse",
        "bidoBidroundMngViewDTO",
        "bidSubmissionByContractorViewResponse",
    ):
        if key not in financial or financial[key] is not None:
            return False

    if any(row.get("passedYn") not in ("Y", "N") for row in technical_lots):
        return False

    qualified = [row for row in technical_lots if row["passedYn"] == "Y"]

    if len(qualified) != len(financial_lots):
        return False

    notice = lookup(
        payload,
        "notify.bidNoContractorResponse.bidNotification",
    )

    if not isinstance(notice, dict):
        return False

    def identities(rows):
        keys = []

        for row in rows:
            if (
                row.get("notifyId") != context.get("id")
                or row.get("bidNo") != notice.get("bidNo")
                or row.get("bidId") != notice.get("bidId")
                or not row.get("lotNo")
                or not row.get("contractorCode")
            ):
                return None

            keys.append(
                (
                    row["lotNo"],
                    row["contractorCode"],
                )
            )

        return Counter(keys)

    qualified_pairs = identities(qualified)
    financial_pairs = identities(financial_lots)

    if (
        qualified_pairs is None
        or financial_pairs is None
        or qualified_pairs != financial_pairs
    ):
        return False

    # Financial lots must contain only technical-stage results.
    for row in financial_lots:
        if row.get("passedYn") != "Y":
            return False

        if any(
            row.get(field) not in (None, 0) for field in ("lotPrice", "lotFinalPrice")
        ):
            return False

        if any(
            row.get(field) is not None
            for field in (
                "evalPrice",
                "ranking",
                "succBidderPrice",
                "succBidderYn",
            )
        ):
            return False

    return True


def _cancelled_after_technical(payload, context):
    """Verify a cancelled LDT notice with technical opening only."""

    if context.get("processApply") not in (None, "LDT"):
        return False

    notice = lookup(
        payload,
        "cancellation_notice." "bidNoContractorResponse.bidNotification",
    )

    cancellation = lookup(
        payload,
        "cancellation_notice.bidCancelingResponse",
    )

    plan = lookup(
        payload,
        "cancellation_notice.bidpPlanDetail",
    )

    technical = lookup(
        payload,
        "bid_open_technical."
        "bidSubmissionByContractorViewResponse."
        "bidSubmissionDTOList",
    )

    financial_response = payload.get("bid_open_financial")
    cancellation_response = payload.get("cancellation_notice")

    if (
        not isinstance(notice, dict)
        or not isinstance(cancellation, dict)
        or not isinstance(plan, dict)
        or not isinstance(financial_response, dict)
        or not isinstance(cancellation_response, dict)
    ):
        return False

    if (
        financial_response.get("error")
        or financial_response.get("success") is False
        or cancellation_response.get("error")
        or cancellation_response.get("success") is False
    ):
        return False

    # Only accept an explicitly returned null financial root.
    if (
        "bidSubmissionByContractorViewResponse" not in financial_response
        or financial_response["bidSubmissionByContractorViewResponse"] is not None
    ):
        return False

    # There must be no unexplained financial lot data.
    financial_lots = payload.get("lot_open_detail_financial")

    if financial_lots != [] and not _financial_lots_are_technical_qualifiers(
        payload, context
    ):
        return False

    # Validate the original notice identity.
    for root in (notice, cancellation):
        if root.get("id") != context.get("id") or root.get("notifyNo") != context.get(
            "notifyNo"
        ):
            return False

    if notice.get("notifyVersion") != context.get("notifyVersion"):
        return False

    if (
        notice.get("processApply") != "LDT"
        or notice.get("bidMode") != "1_HTHS"
        or cancellation.get("bidNo") != notice.get("bidNo")
    ):
        return False

    # Require an actual cancellation event.
    cancelled_at = _event_time(cancellation.get("cancelDate"))

    if cancelled_at is None or not cancellation.get("cancelDecisionNo"):
        return False

    # Technical submissions must exist before cancellation.
    if not isinstance(technical, list) or not technical:
        return False

    technical_times = []

    for row in technical:
        if not isinstance(row, dict):
            return False

        opened_at = _event_time(row.get("createdDateBidOpen"))

        if opened_at is None:
            return False

        technical_times.append(opened_at)

    if max(technical_times) > cancelled_at:
        return False

    # Verify that the linked notice belongs to the same package.
    # A self-link is accepted only after stricter unevaluated checks.
    if plan.get("id") != notice.get("bidId"):
        return False

    try:
        linked = json.loads(plan["linkNotifyInfo"])
    except (KeyError, TypeError, ValueError):
        return False

    if not isinstance(linked, dict):
        return False

    if (
        not linked.get("notifyId")
        or not linked.get("notifyNo")
        or linked.get("bidId") != notice.get("bidId")
    ):
        return False

    same_id = linked["notifyId"] == context.get("id")
    same_no = linked["notifyNo"] == context.get("notifyNo")

    # Reject partially matching identities.
    if same_id != same_no:
        return False

    if same_id and same_no:
        # Self-linked notice: verify an unevaluated technical
        # opening and explicit absence of financial results.
        technical_lots = payload.get("lot_open_detail_technical")

        if (
            not isinstance(technical_lots, list)
            or not technical_lots
            or financial_lots != []
        ):
            return False

        for key in (
            "bidNoContractorResponse",
            "bidoBidroundMngViewDTO",
            "bidSubmissionByContractorViewResponse",
        ):
            if key not in financial_response or financial_response[key] is not None:
                return False

        contractor_codes = {row.get("contractorCode") for row in technical}

        for row in technical_lots:
            if not isinstance(row, dict):
                return False

            if (
                row.get("notifyId") != context.get("id")
                or not isinstance(row.get("contractorCode"), str)
                or not row.get("contractorCode")
                or row.get("contractorCode") not in contractor_codes
                or not row.get("lotNo")
                or (
                    row.get("bidNo") is not None and row["bidNo"] != notice.get("bidNo")
                )
                or (
                    row.get("bidId") is not None and row["bidId"] != notice.get("bidId")
                )
                or row.get("passedYn") is not None
            ):
                return False

            if any(
                row.get(field) not in (None, 0)
                for field in ("lotPrice", "lotFinalPrice")
            ):
                return False

            if any(
                row.get(field) is not None
                for field in (
                    "evalPrice",
                    "ranking",
                    "succBidderPrice",
                    "succBidderYn",
                )
            ):
                return False

    return True


def _cancelled_after_opening(payload, context):
    """Validate a cancelled single-envelope LDT opening."""

    if context.get("processApply") not in (None, "LDT"):
        return False

    if context.get("bidMode") not in (None, "1_MTHS"):
        return False

    notice = lookup(
        payload,
        "notify.bidNoContractorResponse.bidNotification",
    )

    round_response = payload.get("roundmng")
    opening = payload.get("bid_open")
    lots = payload.get("lot_open_detail")
    detail = payload.get("cancellation_notice")

    if (
        not isinstance(notice, dict)
        or not isinstance(round_response, dict)
        or not isinstance(opening, dict)
        or not isinstance(detail, dict)
    ):
        return False

    # The missing round root must be explicit, not an absent
    # or silently repaired API response.
    if (
        "bidoBidroundMngViewDTO" not in round_response
        or round_response["bidoBidroundMngViewDTO"] is not None
    ):
        return False

    # Reject source error envelopes.
    for part in (round_response, opening, detail):
        if part.get("error") or part.get("success") is False:
            return False

    # The original notice must be a single-envelope LDT notice.
    if (
        notice.get("processApply") != "LDT"
        or notice.get("bidMode") != "1_MTHS"
        or notice.get("id") != context.get("id")
        or notice.get("notifyNo") != context.get("notifyNo")
        or notice.get("notifyVersion") != context.get("notifyVersion")
    ):
        return False

    cancellation = detail.get("bidCancelingResponse")

    if not isinstance(cancellation, dict):
        return False

    # Cancellation must belong to the same notice/package.
    if (
        cancellation.get("id") != notice.get("id")
        or cancellation.get("notifyNo") != notice.get("notifyNo")
        or cancellation.get("bidNo") != notice.get("bidNo")
        or not cancellation.get("cancelDecisionNo")
    ):
        return False

    # Verify cancellation event time.
    cancelled_at = _event_time(cancellation.get("cancelDate"))

    scheduled_open = _event_time(notice.get("bidOpenDate"))

    if cancelled_at is None or scheduled_open is None:
        return False

    # Opening submissions must exist.
    submissions = lookup(
        payload,
        "bid_open." "bidSubmissionByContractorViewResponse." "bidSubmissionDTOList",
    )

    if (
        not isinstance(submissions, list)
        or not submissions
        or any(not isinstance(row, dict) for row in submissions)
    ):
        return False

    # Opening dates must fall between scheduled opening
    # and the later cancellation.
    for submission in submissions:
        opened_at = _event_time(submission.get("createdDateBidOpen"))

        if opened_at is None or opened_at < scheduled_open or opened_at >= cancelled_at:
            return False

    # Preserve and validate the corresponding lot data.
    if (
        not isinstance(lots, list)
        or not lots
        or any(not isinstance(row, dict) for row in lots)
    ):
        return False

    if any(row.get("notifyId") != context.get("id") for row in lots):
        return False

    # If the opening response includes its own notice identity,
    # it must not contradict the requested notice.
    opening_notice = lookup(
        payload,
        "bid_open.bidNoContractorResponse.bidNotification",
    )

    if opening_notice is not None:
        if not isinstance(opening_notice, dict):
            return False

        if opening_notice.get("id") != context.get("id") or opening_notice.get(
            "notifyNo"
        ) != context.get("notifyNo"):
            return False

    return True


def validate_adb_assembly(payload, context, contract, *, thresholds=None):
    """Validate a verified ADB single-envelope bid opening."""

    roots = [
        "opening_adb_wb.bidNoContractorResponse.bidNotification",
        "opening_adb_wb.bidoBidroundMngViewDTO",
    ]

    raw_contract = contract.model_copy(
        update={
            "assembly": False,
            "roots": roots,
        }
    )

    result = validate_detail(
        payload,
        context,
        raw_contract,
        thresholds=thresholds,
    )

    problems = result["issues"]

    if not isinstance(payload, dict):
        return result

    expected_process = context.get("processApply")

    if expected_process not in ("ADB", "WB") or context.get("bidMode") != "1_MTHS":
        problems.append(issue("unsupported_adb_wb_opening_mode", "fail"))

    if set(payload) != {"opening_adb_wb"}:
        problems.append(issue("unexpected_opening_components", "fail"))

    part = payload.get("opening_adb_wb")

    if not isinstance(part, dict) or (
        part.get("error") or part.get("success") is False
    ):
        problems.append(issue("invalid_component", "fail", "opening_adb_wb"))
        return finish(result)

    # Validate both business roots independently.
    for root_path in roots:
        root = lookup(payload, root_path)

        if not isinstance(root, dict) or not root:
            problems.append(
                issue(
                    "missing_component_root",
                    "fail",
                    root_path,
                )
            )
            continue

        checked = validate_detail(
            payload,
            context,
            contract.model_copy(
                update={
                    "assembly": False,
                    "roots": [root_path],
                }
            ),
            thresholds=thresholds,
        )
        problems.extend(checked["issues"])

        for key in ("id", "notifyNo", "notifyVersion"):
            expected = context.get(key)
            actual = root.get(key)

            if expected is None or actual is None or str(actual) != str(expected):
                problems.append(
                    issue(
                        "opening_identity_mismatch",
                        "fail",
                        f"{root_path}.{key}",
                    )
                )

        if root.get("bidMode") != "1_MTHS":
            problems.append(
                issue(
                    "opening_mode_mismatch",
                    "fail",
                    f"{root_path}.bidMode",
                )
            )

        if root.get("processApply") != expected_process:
            problems.append(
                issue(
                    "process_apply_mismatch",
                    "fail",
                    f"{root_path}.processApply",
                )
            )

    notice = lookup(payload, roots[0])
    round_info = lookup(payload, roots[1])

    if isinstance(notice, dict) and isinstance(round_info, dict):
        if not notice.get("bidNo") or round_info.get("bidNo") != notice.get("bidNo"):
            problems.append(issue("opening_bid_no_mismatch", "fail"))

        opened_at = round_info.get("successBidOpenDate")

        try:
            if not isinstance(opened_at, str) or not opened_at:
                raise ValueError("Missing opening timestamp")
            datetime.fromisoformat(opened_at)
        except ValueError:
            problems.append(
                issue(
                    "invalid_opening_timestamp",
                    "fail",
                    roots[1] + ".successBidOpenDate",
                )
            )

    container = part.get("bidSubmissionByContractorViewResponse")

    if not isinstance(container, dict):
        problems.append(
            issue(
                "invalid_component",
                "fail",
                "opening_adb_wb." "bidSubmissionByContractorViewResponse",
            )
        )
        return finish(result)

    submissions = container.get("bidSubmissionDTOList")
    lot_details = container.get("bidoLotOpenDetailDTOS")

    if not isinstance(submissions, list) or any(
        not isinstance(x, dict) for x in submissions
    ):
        problems.append(
            issue(
                "invalid_component_list",
                "fail",
                "opening_adb_wb."
                "bidSubmissionByContractorViewResponse."
                "bidSubmissionDTOList",
            )
        )
    else:
        count = (
            round_info.get("countContractor") if isinstance(round_info, dict) else None
        )

        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            problems.append(issue("invalid_contractor_count", "fail"))
        elif count != len(submissions):
            problems.append(issue("contractor_count_mismatch", "fail"))

        for index, row in enumerate(submissions):
            if not row.get("id") or not row.get("contractorCode"):
                problems.append(
                    issue(
                        "invalid_submission_identity",
                        "fail",
                        str(index),
                    )
                )

            lots = row.get("lotNoValueDTOList")

            if not isinstance(lots, list) or any(not isinstance(x, dict) for x in lots):
                problems.append(
                    issue(
                        "invalid_lot_submission_list",
                        "fail",
                        str(index),
                    )
                )

    # An empty list is valid for the verified ADB response.
    if not isinstance(lot_details, list) or any(
        not isinstance(x, dict) for x in lot_details
    ):
        problems.append(
            issue(
                "invalid_component_list",
                "fail",
                "bidoLotOpenDetailDTOS",
            )
        )

    result = finish(result)

    if result["status"] in {"pass", "warn"}:
        result["collection_status"] = "complete"

    return result


def _no_qualified_technical_bidders(payload, context):
    """Verify a multi-lot LDT opening with no qualified technical bidders."""

    if context.get("processApply") != "LDT" or context.get("bidMode") != "1_HTHS":
        return False

    notice = lookup(
        payload,
        "notify.bidNoContractorResponse.bidNotification",
    )
    round_info = lookup(
        payload,
        "roundmng.bidoBidroundMngViewDTO",
    )
    technical = lookup(
        payload,
        "bid_open_technical."
        "bidSubmissionByContractorViewResponse."
        "bidSubmissionDTOList",
    )
    technical_lots = payload.get("lot_open_detail_technical")
    financial = payload.get("bid_open_financial")
    financial_lots = payload.get("lot_open_detail_financial")
    detail = payload.get("cancellation_notice")

    if (
        not isinstance(notice, dict)
        or not isinstance(round_info, dict)
        or not isinstance(financial, dict)
        or not isinstance(detail, dict)
    ):
        return False

    for part_name in (
        "notify",
        "roundmng",
        "bid_open_technical",
    ):
        part = payload.get(part_name)

        if (
            not isinstance(part, dict)
            or part.get("error")
            or part.get("success") is False
        ):
            return False

    if any(
        part.get("error") or part.get("success") is False
        for part in (financial, detail)
    ):
        return False

    # Verify the notice and round identities.
    for root in (notice, round_info):
        if any(
            root.get(key) != context.get(key)
            for key in ("id", "notifyNo", "notifyVersion")
        ):
            return False

        if root.get("processApply") != "LDT" or root.get("bidMode") != "1_HTHS":
            return False

    if (
        not notice.get("bidNo")
        or round_info.get("bidNo") != notice.get("bidNo")
        or round_info.get("isMultiLot") not in (True, 1)
    ):
        return False

    # Financial phase must have a valid publication marker.
    if _event_time(round_info.get("successBidOpenDateTc")) is None:
        return False

    # The financial API must explicitly return null roots.
    financial_roots = (
        "bidNoContractorResponse",
        "bidoBidroundMngViewDTO",
        "bidSubmissionByContractorViewResponse",
    )

    if any(
        key not in financial or financial[key] is not None for key in financial_roots
    ):
        return False

    if financial_lots != []:
        return False

    # Verify technical submissions and contractor count.
    if not isinstance(technical, list) or not technical:
        return False

    if any(not isinstance(row, dict) for row in technical):
        return False

    count = round_info.get("countContractor")

    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or count <= 0
        or count != len(technical)
    ):
        return False

    contractor_codes = [row.get("contractorCode") for row in technical]

    if (
        any(not isinstance(code, str) or not code for code in contractor_codes)
        or len(set(contractor_codes)) != len(contractor_codes)
        or any(not row.get("id") for row in technical)
    ):
        return False

    # Verify every returned contractor-lot evaluation.
    if not isinstance(technical_lots, list) or not technical_lots:
        return False

    pairs = set()
    evaluated_contractors = set()
    evaluated_lots = set()

    for row in technical_lots:
        if not isinstance(row, dict):
            return False

        code = row.get("contractorCode")
        lot_no = row.get("lotNo")

        if (
            row.get("notifyId") != context.get("id")
            or code not in contractor_codes
            or not isinstance(lot_no, str)
            or not lot_no
            or row.get("passedYn") != "N"
        ):
            return False

        pair = (code, lot_no)

        if pair in pairs:
            return False

        pairs.add(pair)
        evaluated_contractors.add(code)
        evaluated_lots.add(lot_no)

    # Every technical bidder must occur in the evaluations.
    if evaluated_contractors != set(contractor_codes):
        return False

    # Initially support only verified multi-lot structures.
    if len(evaluated_lots) < 2:
        return False

    # Cross-check declared bidder-lot participation when present.
    declared_pairs = set()
    declared_count = 0

    for bidder in technical:
        code = bidder["contractorCode"]
        declared = bidder.get("lotNoValueDTOList")

        if declared is None or declared == []:
            continue

        if not isinstance(declared, list):
            return False

        declared_count += 1

        for lot in declared:
            if not isinstance(lot, dict) or not lot.get("lotNo"):
                return False

            declared_pairs.add((code, lot["lotNo"]))

    # Reject partially declared participation.
    if declared_count not in (0, len(technical)):
        return False

    if declared_count and declared_pairs != pairs:
        return False

    # Check that this is not a verified cancellation.
    cancellation = detail.get("bidCancelingResponse")
    detail_notice = lookup(
        detail,
        "bidNoContractorResponse.bidNotification",
    )
    plan = detail.get("bidpPlanDetail")

    if (
        not isinstance(cancellation, dict)
        or not isinstance(detail_notice, dict)
        or not isinstance(plan, dict)
    ):
        return False

    if any(
        detail_notice.get(key) != context.get(key)
        for key in ("id", "notifyNo", "notifyVersion")
    ):
        return False

    if (
        cancellation.get("id") != context.get("id")
        or cancellation.get("notifyNo") != context.get("notifyNo")
        or cancellation.get("bidNo") != notice.get("bidNo")
    ):
        return False

    # Require explicit null cancellation fields.
    if (
        "cancelDate" not in cancellation
        or "cancelDecisionNo" not in cancellation
        or cancellation["cancelDate"] is not None
        or cancellation["cancelDecisionNo"] is not None
    ):
        return False

    # The plan must still point to the original notice.
    if not notice.get("bidId") or plan.get("id") != notice["bidId"]:
        return False

    try:
        linked = json.loads(plan["linkNotifyInfo"])
    except (KeyError, TypeError, ValueError):
        return False

    if not isinstance(linked, dict):
        return False

    return (
        linked.get("notifyId") == context.get("id")
        and linked.get("notifyNo") == context.get("notifyNo")
        and linked.get("bidId") == notice.get("bidId")
    )


def _ldt_financial_source_conflict(payload, context):
    """Verify an LDT opening with null financial data despite a publication marker."""

    required = {
        "notify",
        "roundmng",
        "bid_open_technical",
        "lot_open_detail_technical",
        "bid_open_financial",
        "lot_open_detail_financial",
        "cancellation_notice",
    }

    if (
        context.get("processApply") != "LDT"
        or context.get("bidMode") != "1_HTHS"
        or not isinstance(payload, dict)
        or set(payload) != required
    ):
        return False

    # 1. Read source business roots.
    notice = lookup(
        payload,
        "notify.bidNoContractorResponse.bidNotification",
    )
    round_info = lookup(
        payload,
        "roundmng.bidoBidroundMngViewDTO",
    )
    detail_notice = lookup(
        payload,
        "cancellation_notice.bidNoContractorResponse.bidNotification",
    )
    cancellation = lookup(
        payload,
        "cancellation_notice.bidCancelingResponse",
    )
    plan = lookup(
        payload,
        "cancellation_notice.bidpPlanDetail",
    )
    financial = payload.get("bid_open_financial")
    technical = lookup(
        payload,
        "bid_open_technical."
        "bidSubmissionByContractorViewResponse."
        "bidSubmissionDTOList",
    )
    technical_lots = payload.get("lot_open_detail_technical")

    # Explicit checks allow Pylance to narrow each variable.
    if not isinstance(notice, dict):
        return False
    if not isinstance(round_info, dict):
        return False
    if not isinstance(detail_notice, dict):
        return False
    if not isinstance(cancellation, dict):
        return False
    if not isinstance(plan, dict):
        return False
    if not isinstance(financial, dict):
        return False

    # 2. Reject API error envelopes.
    for name in (
        "notify",
        "roundmng",
        "bid_open_technical",
        "bid_open_financial",
        "cancellation_notice",
    ):
        part = payload[name]
        if (
            not isinstance(part, dict)
            or part.get("error")
            or part.get("success") is False
        ):
            return False

    # 3. Verify identities across all business roots.
    for root in (notice, round_info, detail_notice):
        if any(
            root.get(key) != context.get(key)
            for key in ("id", "notifyNo", "notifyVersion")
        ):
            return False

    if (
        notice.get("processApply") != "LDT"
        or notice.get("bidMode") != "1_HTHS"
        or round_info.get("processApply") != "LDT"
        or round_info.get("bidMode") != "1_HTHS"
        or not notice.get("bidNo")
        or round_info.get("bidNo") != notice["bidNo"]
        or detail_notice.get("bidNo") != notice["bidNo"]
    ):
        return False

    # 4. Require the observed single-lot, unevaluated shape.
    if (
        round_info.get("isMultiLot") is not False
        or type(round_info.get("countContractor")) is not int
        or round_info["countContractor"] != 1
        or round_info.get("bidStatus") != "OPEN_DXKT"
        or round_info.get("evalStatus") != "-1"
    ):
        return False

    # 5. Verify the source timestamp contradiction.
    technical_at = _event_time(round_info.get("successBidOpenDate"))
    financial_at = _event_time(round_info.get("successBidOpenDateTc"))

    if technical_at is None or financial_at is None or financial_at >= technical_at:
        return False

    # 6. Financial API must explicitly return null data.
    financial_roots = (
        "bidNoContractorResponse",
        "bidoBidroundMngViewDTO",
        "bidSubmissionByContractorViewResponse",
    )

    if any(
        key not in financial or financial[key] is not None for key in financial_roots
    ):
        return False

    if payload["lot_open_detail_financial"] != []:
        return False

    # 7. Confirm that cancellation did not occur.
    if (
        cancellation.get("id") != context.get("id")
        or cancellation.get("notifyNo") != context.get("notifyNo")
        or cancellation.get("bidNo") != notice["bidNo"]
        or "cancelDate" not in cancellation
        or "cancelDecisionNo" not in cancellation
        or cancellation["cancelDate"] is not None
        or cancellation["cancelDecisionNo"] is not None
        or not notice.get("bidId")
        or plan.get("id") != notice["bidId"]
    ):
        return False

    # 8. Verify that the linked notice refers to itself.
    try:
        linked = json.loads(plan["linkNotifyInfo"])
    except (KeyError, TypeError, ValueError):
        return False

    if not isinstance(linked, dict):
        return False

    if (
        linked.get("notifyId") != context.get("id")
        or linked.get("notifyNo") != context.get("notifyNo")
        or linked.get("bidId") != notice["bidId"]
        or linked.get("status") not in (None, "01")
    ):
        return False

    if "notifyVersion" in linked and linked["notifyVersion"] != context.get(
        "notifyVersion"
    ):
        return False

    # 9. Require exactly one technical bidder and one lot.
    if (
        not isinstance(technical, list)
        or len(technical) != 1
        or not isinstance(technical_lots, list)
        or len(technical_lots) != 1
    ):
        return False

    bidder = technical[0]
    lot = technical_lots[0]

    if not isinstance(bidder, dict):
        return False
    if not isinstance(lot, dict):
        return False

    if (
        not bidder.get("id")
        or not bidder.get("contractorCode")
        or lot.get("notifyId") != context.get("id")
        or lot.get("contractorCode") != bidder["contractorCode"]
        or not lot.get("lotNo")
        or lot.get("bidNo") not in (None, notice["bidNo"])
        or lot.get("bidId") not in (None, notice["bidId"])
    ):
        return False

    # 10. Validate the technical opening timestamp.
    opened_at = _event_time(bidder.get("createdDateBidOpen"))

    if opened_at is None or opened_at > technical_at:
        return False

    # 11. Ensure no actual evaluation or award is present.
    if any(
        bidder.get(key) is not None
        for key in (
            "techResult",
            "techScore",
        )
    ):
        return False

    if "passedYn" not in lot:
        return False

    return not any(
        lot.get(key) is not None
        for key in (
            "passedYn",
            "eligibilityYn",
            "experienceYn",
            "techScore",
            "evalPrice",
            "ranking",
            "succBidderYn",
            "succBidderPrice",
        )
    )


def validate_assembly(payload, context, contract, *, thresholds=None):

    if context.get("processApply") in ("ADB", "WB"):
        return validate_adb_assembly(
            payload,
            context,
            contract,
            thresholds=thresholds,
        )

    if context.get("processApply") == "KHAC":
        return validate_khac_assembly(
            payload,
            context,
            contract,
            thresholds=thresholds,
        )

    result = validate_detail(
        payload,
        context,
        contract.model_copy(update={"assembly": False}),
        thresholds=thresholds,
    )
    problems = result["issues"]
    if not isinstance(payload, dict):
        return result
    if result["identity"].get("notifyVersion") is None:
        problems.append(issue("missing_assembly_version", "fail", "notifyVersion"))

    cancelled_single = _cancelled_after_opening(
        payload,
        context,
    )

    for root in contract.roots:
        value = lookup(payload, root)

        if not isinstance(value, dict) or not value:

            # This round root is absent in a verified
            # cancelled single-envelope opening.
            if cancelled_single and root == "roundmng.bidoBidroundMngViewDTO":
                continue

            problems.append(issue("missing_component_root", "fail", root))

        else:
            checked = validate_detail(
                payload,
                context,
                contract.model_copy(
                    update={
                        "assembly": False,
                        "roots": [root],
                    }
                ),
                thresholds=thresholds,
            )

            problems.extend(checked["issues"])

    for name in ("notify", "roundmng"):
        part = payload.get(name)
        if (
            not isinstance(part, dict)
            or part.get("error")
            or part.get("success") is False
        ):
            problems.append(issue("invalid_component", "fail", name))
    try:
        parts, pending = opening_parts(payload, context)
    except (ValueError, TypeError) as exc:
        problems.append(
            issue("unresolved_opening_phase", "unresolved", reason=str(exc))
        )
        return finish(result)
    cancelled = _cancelled_after_technical(payload, context)

    no_qualified = _no_qualified_technical_bidders(
        payload,
        context,
    )

    financial_source_conflict = _ldt_financial_source_conflict(
        payload,
        context,
    )

    if cancelled_single:
        result["collection_status"] = "cancelled_after_opening"

    elif cancelled:
        result["collection_status"] = "cancelled_after_technical"

    elif no_qualified:
        result["collection_status"] = "no_qualified_technical_bidders"

    elif financial_source_conflict:
        result["collection_status"] = "financial_source_conflict"

    else:
        result["collection_status"] = "awaiting_financial" if pending else "complete"

    expected = {"notify", "roundmng"} | {name for pair in parts for name in pair}

    if cancelled or cancelled_single or no_qualified or financial_source_conflict:
        expected.add("cancellation_notice")

    if set(payload) != expected:
        problems.append(
            issue("unexpected_opening_components", "fail", expected=sorted(expected))
        )
    for bid, lot in parts:
        part = payload.get(bid)
        if (
            not isinstance(part, dict)
            or part.get("error")
            or part.get("success") is False
        ):
            problems.append(issue("invalid_component", "fail", bid))
        submissions = lookup(
            payload, f"{bid}.bidSubmissionByContractorViewResponse.bidSubmissionDTOList"
        )
        lots = payload.get(lot)
        for name, rows in ((bid, submissions), (lot, lots)):

            # Financial root is genuinely null for a verified
            # cancelled-after-technical notice.
            if (
                (cancelled or no_qualified or financial_source_conflict)
                and name == "bid_open_financial"
                and rows is None
            ):
                continue

            if not isinstance(rows, list) or any(
                not isinstance(row, dict) for row in rows
            ):
                problems.append(issue("invalid_component_list", "fail", name))
        if isinstance(lots, list):
            for row in lots:
                if isinstance(row, dict) and row.get("notifyId") != context.get("id"):
                    problems.append(
                        issue("lot_identity_mismatch", "fail", f"{lot}.notifyId")
                    )
        count = lookup(payload, "roundmng.bidoBidroundMngViewDTO.countContractor")
        # Financial participants can be a subset of technical participants.
        if (
            bid != "bid_open_financial"
            and isinstance(submissions, list)
            and isinstance(count, int)
            and count != len(submissions)
        ):
            problems.append(issue("contractor_count_mismatch", "warn", bid))
    if financial_source_conflict:
        problems.append(
            issue(
                "financial_null_with_publication_marker_conflict",
                "warn",
                "bid_open_financial.bidSubmissionByContractorViewResponse",
            )
        )
    return finish(result)
