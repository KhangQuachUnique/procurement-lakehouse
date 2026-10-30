"""Validate the assembly without rewriting any of its source responses."""

from procurement.quality.contracts import finish, issue, lookup, validate_detail
from procurement.quality.opening_phases import (
    financial_published,
    opening_parts,
)
import json
from datetime import datetime, timezone

from procurement.common.dates import VIETNAM_TZ

from collections import Counter

KHAC_ROOTS = [
    "opening_out.bidoNotifyContractorP",
    "opening_out.bidoBidroundOutMngViewDTO",
]

KHAC_SUBMISSIONS = (
    "opening_out.bidSubmissionByContractorViewResponse" ".bidSubmissionDTOList"
)


def _khac_cancelled_without_verified_evaluation(payload, context):
    """Verify a cancelled KHAC dual-envelope opening without evaluation."""

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

    if any(
        value is None
        for value in (
            scheduled,
            cancelled,
            decision,
            opened_technical,
            opened_financial,
        )
    ):
        return False

    # This particular source anomaly:
    # scheduled opening < cancellation < recorded opening timestamps.
    if not (
        scheduled < cancelled
        and decision <= cancelled
        and cancelled < opened_technical < opened_financial
    ):
        return False

    # Round timestamps must be consistent across both responses.
    for field in ("successBidOpenDate", "successBidOpenDateTc"):
        if technical_round.get(field) != financial_round.get(field):
            return False

    # Only the verified one-bidder shape is supported initially.
    if (
        technical_round.get("countContractor") != 1
        or financial_round.get("countContractor") != 1
    ):
        return False

    technical_container = technical.get("bidSubmissionByContractorViewResponse")
    financial_container = financial.get("bidSubmissionByContractorViewResponse")

    if not all(
        isinstance(value, dict) for value in (technical_container, financial_container)
    ):
        return False

    # Missing financial information must be explicitly null,
    # not absent and not silently converted into empty lists.
    for field in (
        "bidSubmissionDTOList",
        "bidoLotOpenDetailDTOS",
        "bidOpenAdditionList",
    ):
        if field not in financial_container or financial_container[field] is not None:
            return False

    # No separately evaluated technical lot data.
    for field in ("bidoLotOpenDetailDTOS", "bidOpenAdditionList"):
        if field not in technical_container or technical_container[field] is not None:
            return False

    submissions = technical_container.get("bidSubmissionDTOList")

    if not isinstance(submissions, list) or len(submissions) != 1:
        return False

    bidder = submissions[0]

    if (
        not isinstance(bidder, dict)
        or not bidder.get("id")
        or not bidder.get("contractorCode")
    ):
        return False

    if any(
        bidder.get(field) is not None
        for field in (
            "techResult",
            "techScore",
            "createdDateBidOpen",
        )
    ):
        return False

    if any(
        bidder.get(field) not in (None, 0) for field in ("bidPrice", "bidFinalPrice")
    ):
        return False

    lots = bidder.get("lotNoValueDTOList")

    if not isinstance(lots, list) or len(lots) != 1:
        return False

    lot = lots[0]

    if not isinstance(lot, dict):
        return False

    # In this verified KHAC response, lotNo identifies the package,
    # while lot.bidNo and lot.contractorCode are explicitly null.
    # Accept nullable duplicate identifiers only when they do not
    # contradict the notice and the enclosing technical submission.
    if (
        lot.get("lotNo") != original["bidNo"]
        or "bidNo" not in lot
        or "contractorCode" not in lot
        or lot["bidNo"] not in (None, original["bidNo"])
        or lot["contractorCode"] not in (None, bidder["contractorCode"])
    ):
        return False

    # No technical or financial evaluation result is available.
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

    if any(lot.get(field) not in (None, 0) for field in ("lotPrice", "lotFinalPrice")):
        return False

    return True


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
            cancelled_no_evaluation
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
    if mode == "1_HTHS" and not cancelled_no_evaluation:
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

    result = finish(result)

    if result["status"] in {"pass", "warn"}:
        if cancelled_no_evaluation:
            result["collection_status"] = "cancelled_without_verified_evaluation"
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

    return result.astimezone(timezone.utc)


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

    if not all(
        isinstance(root, dict)
        for root in (
            notice,
            cancellation,
            plan,
            financial_response,
            cancellation_response,
        )
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

    if not all(
        isinstance(part, dict) for part in (notice, round_response, opening, detail)
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
        "opening_adb_wb." "bidNoContractorResponse.bidNotification",
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

    if not all(
        isinstance(value, dict) for value in (notice, round_info, financial, detail)
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

    if not all(
        isinstance(value, dict) for value in (cancellation, detail_notice, plan)
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

    if (
        linked.get("notifyId") != context.get("id")
        or linked.get("notifyNo") != context.get("notifyNo")
        or linked.get("bidId") != notice.get("bidId")
    ):
        return False

    return True


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

    if cancelled_single:
        result["collection_status"] = "cancelled_after_opening"

    elif cancelled:
        result["collection_status"] = "cancelled_after_technical"

    elif no_qualified:
        result["collection_status"] = "no_qualified_technical_bidders"

    else:
        result["collection_status"] = "awaiting_financial" if pending else "complete"

    expected = {"notify", "roundmng"} | {name for pair in parts for name in pair}

    if cancelled or cancelled_single or no_qualified:
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
                (cancelled or no_qualified)
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
    return finish(result)
