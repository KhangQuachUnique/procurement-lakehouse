"""Versioned source mappings. No inferred currency, eligibility, or fuzzy identity joins."""

from decimal import Decimal, InvalidOperation, localcontext

VERSION = {"mapping": "1", "schema": "1", "canonicalization": "1", "quality": "1"}

# Entity type, accepted roots, identity scheme/field, number, title, amount, currency.
MAPPINGS = {
    "project_detail": ("project", ("projectDTO",), "uuid", "id", "no", "name", "investTotal", "investUnit"),
    "khlcnt_plan_detail": ("plan", ("bidPoBidpPlanProjectDetailView",), "uuid", "id", "planNo", "planName", None, None),
    "khlcnt_bid_package_detail": ("package", ("",), "uuid", "id", "bidNo", "bidName", "bidPrice", "bidPriceUnit"),
    "notify_contractor_standard_detail": ("notice", ("bidoNotifyContractorM", "bidNoContractorResponse.bidNotification"), "notify_no", "notifyNo", "notifyNo", "bidName", "bidPrice", "bidPriceUnit"),
    "notify_contractor_reoffer_detail": ("notice", ("",), "notify_no", "notifyNo", "notifyNo", "bidName", None, None),
    "notify_contractor_vk_adb_detail": ("notice", ("bidoNotifyContractorP",), "notify_no", "notifyNo", "notifyNo", "bidName", "bidPrice", "bidPriceUnit"),
    "contractor_result_detail": ("result", ("bideContractorInputResultDTO",), "uuid", "id", "notifyNo", "bidName", "bidPrice", "bidPriceUnit"),
    "bid_opening_detail": ("opening", ("notify.bidNoContractorResponse.bidNotification", "roundmng.bidoBidroundMngViewDTO"), "notify_no", "notifyNo", "notifyNo", "bidName", None, None),
}

REFERENCES = {
    "plan": (("project", "uuid", "pid"),),
    "package": (("plan", "uuid", "planId"),),
    "notice": (("package", "uuid", "bidId"), ("plan", "number", "planNo")),
    "result": (("notice", "notify_no", "notifyNo"), ("package", "uuid", "bidId")),
    "opening": (("notice", "notify_no", "notifyNo"),),
}


def lookup(value, path):
    for part in path.split(".") if path else ():
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def text(value):
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    result = str(value).strip()
    return result or None


def money(value):
    if value is None or value == "":
        return None
    if isinstance(value, (bool, float)):
        raise ValueError("Money must be parsed from exact JSON decimals")
    try:
        with localcontext() as ctx:
            ctx.prec = 50
            result = Decimal(value)
            if not result.is_finite() or abs(result) >= Decimal(10) ** 32:
                raise ValueError("Money outside DECIMAL(38,6)")
            quantized = result.quantize(Decimal("0.000001"))
            if quantized != result:
                raise ValueError("Money has more than six fractional digits")
            return str(quantized)
    except InvalidOperation as exc:
        raise ValueError("Invalid money") from exc
