"""Shared Silver→Gold projection contract, independent of any scheduler/Gold runtime."""

import re


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError("Invalid source column identifier")
    return value


INPUT_TABLES = (
    "package_revision",
    "notice_revision",
    "result_revision",
    "observation",
    "award_lot",
    "award_contractor",
    "plan_revision",
    "package_location",
)
PROJECTION_VERSION = "silver_gold_projection_v1"


def projection(columns, name):
    excluded = {"root_json", "raw_json", "payload_json", "field_sources_json", "envelope_json"}
    # The slim result contract stores the only consumed lot JSON attribute as
    # a typed column. Keep the existing Gold input shape during the transition.
    if name == "award_lot":
        excluded.add("source_lot_id")
    selected = [identifier(c) for c in columns if c not in excluded]
    if "root_json" in columns and name in {"package_revision", "plan_revision"}:
        selected.append("json_object('id',json_extract_string(root_json,'$.id')) AS root_json")
    elif name in {"package_revision", "plan_revision"} and "source_id" in columns:
        selected.append("json_object('id',source_id) AS root_json")
    elif name in {"package_revision", "plan_revision"} and "entity_id" in columns:
        selected.append("json_object('id',entity_id) AS root_json")
    elif "root_json" in columns and name == "notice_revision":
        selected.append(
            "json_object('bidGuaranteeUnit',json_extract_string(root_json,'$.bidGuaranteeUnit'),'bidPriceUnit',json_extract_string(root_json,'$.bidPriceUnit')) AS root_json"
        )
    elif name == "notice_revision" and "guarantee_currency" in columns and "currency" in columns:
        selected.append(
            "json_object('bidGuaranteeUnit',COALESCE(guarantee_currency,'VND'),'bidPriceUnit',COALESCE(currency,'VND')) AS root_json"
        )
    elif "source_lot_id" in columns and name == "award_lot":
        selected.append("json_object('id',source_lot_id) AS raw_json")
    elif "raw_json" in columns and name == "award_lot":
        selected.append("json_object('id',json_extract_string(raw_json,'$.id')) AS raw_json")
    return selected
