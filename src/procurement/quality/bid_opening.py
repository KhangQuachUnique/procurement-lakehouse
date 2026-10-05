"""Validate the assembly without rewriting any of its source responses."""
from procurement.quality.contracts import finish, issue, lookup, validate_detail
from procurement.quality.opening_phases import opening_parts


def validate_assembly(payload, context, contract, *, thresholds=None):
    result = validate_detail(payload, context, contract.model_copy(update={"assembly": False}),
                             thresholds=thresholds)
    problems = result["issues"]
    if not isinstance(payload, dict):
        return result
    if result["identity"].get("notifyVersion") is None:
        problems.append(issue("missing_assembly_version", "fail", "notifyVersion"))
    for root in contract.roots:
        if not isinstance(lookup(payload, root), dict) or not lookup(payload, root):
            problems.append(issue("missing_component_root", "fail", root))
        else:
            checked = validate_detail(payload, context,
                contract.model_copy(update={"assembly": False, "roots": [root]}),
                thresholds=thresholds)
            problems.extend(checked["issues"])
    for name in ("notify", "roundmng"):
        part = payload.get(name)
        if not isinstance(part, dict) or part.get("error") or part.get("success") is False:
            problems.append(issue("invalid_component", "fail", name))
    try:
        parts, pending = opening_parts(payload, context)
    except (ValueError, TypeError) as exc:
        problems.append(issue("unresolved_opening_phase", "unresolved", reason=str(exc)))
        return finish(result)
    result["collection_status"] = "awaiting_financial" if pending else "complete"
    expected = {"notify", "roundmng"} | {name for pair in parts for name in pair}
    if set(payload) != expected:
        problems.append(issue("unexpected_opening_components", "fail", expected=sorted(expected)))
    for bid, lot in parts:
        part = payload.get(bid)
        if not isinstance(part, dict) or part.get("error") or part.get("success") is False:
            problems.append(issue("invalid_component", "fail", bid))
        submissions = lookup(payload, f"{bid}.bidSubmissionByContractorViewResponse.bidSubmissionDTOList")
        lots = payload.get(lot)
        for name, rows in ((bid, submissions), (lot, lots)):
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                problems.append(issue("invalid_component_list", "fail", name))
        if isinstance(lots, list):
            for row in lots:
                if isinstance(row, dict) and row.get("notifyId") != context.get("id"):
                    problems.append(issue("lot_identity_mismatch", "fail", f"{lot}.notifyId"))
        count = lookup(payload, "roundmng.bidoBidroundMngViewDTO.countContractor")
        # Financial participants can be a subset of technical participants.
        if bid != "bid_open_financial" and isinstance(submissions, list) and isinstance(count, int) and count != len(submissions):
            problems.append(issue("contractor_count_mismatch", "warn", bid))
    return finish(result)
