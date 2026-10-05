"""Reviewed modes and publication signals; a null API body never proves absence."""
from datetime import datetime

from procurement.quality.contracts import lookup

SINGLE = "1_MTHS"
DUAL = "1_HTHS"
ROUND_ROOT = "roundmng.bidoBidroundMngViewDTO"


def opening_mode(payload, context):
    values = {value for value in (
        context.get("bidMode"),
        lookup(payload, "notify.bidNoContractorResponse.bidNotification.bidMode"),
        lookup(payload, ROUND_ROOT + ".bidMode"),
    ) if value is not None}
    if len(values) != 1 or not values <= {SINGLE, DUAL}:
        raise ValueError("Unknown or conflicting bid opening mode")
    return values.pop()


def financial_published(root):
    if not isinstance(root, dict) or "successBidOpenDateTc" not in root:
        raise ValueError("Missing financial publication context")
    value = root["successBidOpenDateTc"]
    if value is None:
        return False
    if not isinstance(value, str) or not value:
        raise ValueError("Invalid financial publication date")
    datetime.fromisoformat(value)
    return True


def opening_parts(payload, context):
    """Required raw branches and whether another publication is pending."""
    if opening_mode(payload, context) == SINGLE:
        return [("bid_open", "lot_open_detail")], False
    published = financial_published(lookup(payload, ROUND_ROOT))
    parts = [("bid_open_technical", "lot_open_detail_technical")]
    if published:
        parts.append(("bid_open_financial", "lot_open_detail_financial"))
    return parts, not published
