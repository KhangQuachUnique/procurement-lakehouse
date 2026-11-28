"""Authoritative 63 Vietnam provinces & economic regions dictionary for Spark Gold.

Registers two tiny broadcast-friendly Spark TempViews:
  - ``_gtmp_province_map`` (raw GSO/3-digit/short/alias codes -> canonical province)
  - ``_gtmp_canonical_geography`` (63 provinces + MULTI + UNKNOWN)
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType


@dataclass(frozen=True)
class ProvinceInfo:
    gso_code: str
    short_code: str
    name: str
    region: str


VIETNAM_63_PROVINCES: tuple[ProvinceInfo, ...] = (
    # 1. Đồng bằng sông Hồng (11)
    ProvinceInfo("01", "HNI", "Thành phố Hà Nội", "Đồng bằng sông Hồng"),
    ProvinceInfo("16", "VPH", "Tỉnh Vĩnh Phúc", "Đồng bằng sông Hồng"),
    ProvinceInfo("27", "BNH", "Tỉnh Bắc Ninh", "Đồng bằng sông Hồng"),
    ProvinceInfo("30", "HDG", "Tỉnh Hải Dương", "Đồng bằng sông Hồng"),
    ProvinceInfo("31", "HPG", "Thành phố Hải Phòng", "Đồng bằng sông Hồng"),
    ProvinceInfo("33", "HYN", "Tỉnh Hưng Yên", "Đồng bằng sông Hồng"),
    ProvinceInfo("34", "TBH", "Tỉnh Thái Bình", "Đồng bằng sông Hồng"),
    ProvinceInfo("35", "HNM", "Tỉnh Hà Nam", "Đồng bằng sông Hồng"),
    ProvinceInfo("36", "NDH", "Tỉnh Nam Định", "Đồng bằng sông Hồng"),
    ProvinceInfo("37", "NBH", "Tỉnh Ninh Bình", "Đồng bằng sông Hồng"),
    ProvinceInfo("22", "QNH", "Tỉnh Quảng Ninh", "Đồng bằng sông Hồng"),
    # 2. Trung du và miền núi phía Bắc (14)
    ProvinceInfo("02", "HGG", "Tỉnh Hà Giang", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("04", "CBG", "Tỉnh Cao Bằng", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("06", "BKN", "Tỉnh Bắc Kạn", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("08", "TQG", "Tỉnh Tuyên Quang", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("10", "LCI", "Tỉnh Lào Cai", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("11", "DBN", "Tỉnh Điện Biên", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("12", "LCH", "Tỉnh Lai Châu", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("14", "SLA", "Tỉnh Sơn La", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("15", "YBN", "Tỉnh Yên Bái", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("17", "HBH", "Tỉnh Hòa Bình", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("19", "TNN", "Tỉnh Thái Nguyên", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("20", "LSON", "Tỉnh Lạng Sơn", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("24", "BGG", "Tỉnh Bắc Giang", "Trung du và miền núi phía Bắc"),
    ProvinceInfo("25", "PTO", "Tỉnh Phú Thọ", "Trung du và miền núi phía Bắc"),
    # 3. Bắc Trung Bộ và Duyên hải miền Trung (14)
    ProvinceInfo("38", "THA", "Tỉnh Thanh Hóa", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("40", "NAN", "Tỉnh Nghệ An", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("42", "HTH", "Tỉnh Hà Tĩnh", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("44", "QBH", "Tỉnh Quảng Bình", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("45", "QTI", "Tỉnh Quảng Trị", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("46", "TTH", "Tỉnh Thừa Thiên Huế", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("48", "DNG", "Thành phố Đà Nẵng", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("49", "QNM", "Tỉnh Quảng Nam", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("51", "QNI", "Tỉnh Quảng Ngãi", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("52", "BDH", "Tỉnh Bình Định", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("54", "PYN", "Tỉnh Phú Yên", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("56", "KHA", "Tỉnh Khánh Hòa", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("58", "NTH", "Tỉnh Ninh Thuận", "Bắc Trung Bộ và Duyên hải miền Trung"),
    ProvinceInfo("60", "BTH", "Tỉnh Bình Thuận", "Bắc Trung Bộ và Duyên hải miền Trung"),
    # 4. Tây Nguyên (5)
    ProvinceInfo("62", "KTM", "Tỉnh Kon Tum", "Tây Nguyên"),
    ProvinceInfo("64", "GLI", "Tỉnh Gia Lai", "Tây Nguyên"),
    ProvinceInfo("66", "DLC", "Tỉnh Đắk Lắk", "Tây Nguyên"),
    ProvinceInfo("67", "DKN", "Tỉnh Đắk Nông", "Tây Nguyên"),
    ProvinceInfo("68", "LDG", "Tỉnh Lâm Đồng", "Tây Nguyên"),
    # 5. Đông Nam Bộ (6)
    ProvinceInfo("79", "HCM", "Thành phố Hồ Chí Minh", "Đông Nam Bộ"),
    ProvinceInfo("70", "BPC", "Tỉnh Bình Phước", "Đông Nam Bộ"),
    ProvinceInfo("72", "TNH", "Tỉnh Tây Ninh", "Đông Nam Bộ"),
    ProvinceInfo("74", "BDG", "Tỉnh Bình Dương", "Đông Nam Bộ"),
    ProvinceInfo("75", "DNI", "Tỉnh Đồng Nai", "Đông Nam Bộ"),
    ProvinceInfo("77", "VTU", "Tỉnh Bà Rịa - Vũng Tàu", "Đông Nam Bộ"),
    # 6. Đồng bằng sông Cửu Long (13)
    ProvinceInfo("80", "LAN", "Tỉnh Long An", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("82", "TGG", "Tỉnh Tiền Giang", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("83", "BTE", "Tỉnh Bến Tre", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("84", "TVH", "Tỉnh Trà Vinh", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("86", "VLG", "Tỉnh Vĩnh Long", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("87", "DTH", "Tỉnh Đồng Tháp", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("89", "AGG", "Tỉnh An Giang", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("91", "KGG", "Tỉnh Kiên Giang", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("92", "CTO", "Thành phố Cần Thơ", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("93", "HGG2", "Tỉnh Hậu Giang", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("94", "STG", "Tỉnh Sóc Trăng", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("95", "BLU", "Tỉnh Bạc Liêu", "Đồng bằng sông Cửu Long"),
    ProvinceInfo("96", "CMU", "Tỉnh Cà Mau", "Đồng bằng sông Cửu Long"),
)

_SPECIAL_ALIASES: dict[str, str] = {
    "HN": "01",
    "TPHN": "01",
    "HA NOI": "01",
    "TPHCM": "79",
    "TP HCM": "79",
    "SAI GON": "79",
    "HO CHI MINH": "79",
    "DN": "48",
    "DA NANG": "48",
    "HP": "31",
    "HAI PHONG": "31",
    "CT": "92",
    "CAN THO": "92",
    "BRVT": "77",
    "BA RIA VUNG TAU": "77",
    "THUA THIEN HUE": "46",
    "HUE": "46",
}

MUASAMCONG_3DIGIT_CODES: dict[str, str] = {
    "101": "HNI", "103": "HPG", "107": "HDG", "109": "HYN", "111": "HNM",
    "113": "NDH", "115": "TBH", "117": "NBH", "201": "HGG", "203": "CBG",
    "205": "LCI", "207": "BKN", "209": "LSON", "211": "TQG", "213": "YBN",
    "215": "TNN", "217": "PTO", "219": "VPH", "221": "BGG", "223": "BNH",
    "225": "QNH", "301": "DBN", "302": "LCH", "303": "SLA", "305": "HBH",
    "401": "THA", "403": "NAN", "405": "HTH", "407": "QBH", "409": "QTI",
    "411": "TTH", "501": "DNG", "503": "QNM", "505": "QNI", "507": "BDH",
    "509": "PYN", "511": "KHA", "601": "KTM", "603": "GLI", "605": "DLC",
    "606": "DKN", "701": "HCM", "703": "LDG", "705": "NTH", "707": "BPC",
    "709": "TNH", "711": "BDG", "713": "DNI", "715": "BTH", "717": "VTU",
    "801": "LAN", "803": "DTH", "805": "AGG", "807": "TGG", "809": "VLG",
    "811": "BTE", "813": "KGG", "815": "CTO", "816": "HGG2", "817": "TVH",
    "819": "STG", "821": "BLU", "823": "CMU",
}

_CODE_LOOKUP: dict[str, ProvinceInfo] = {}
for _p in VIETNAM_63_PROVINCES:
    _CODE_LOOKUP[_p.gso_code] = _p
    _CODE_LOOKUP[str(int(_p.gso_code))] = _p
    _CODE_LOOKUP[_p.short_code.upper()] = _p
for _alias, _gso in _SPECIAL_ALIASES.items():
    _CODE_LOOKUP[_alias.upper()] = _CODE_LOOKUP[_gso]
for _msc, _short in MUASAMCONG_3DIGIT_CODES.items():
    _CODE_LOOKUP[_msc] = _CODE_LOOKUP[_short]


def get_province_code_mapping_rows() -> list[tuple[str, str | None, str, str, str, bool, bool]]:
    """Return (raw_code, canonical_code, geography_key, province_name, region_name, is_multi, is_unknown)."""
    rows: dict[str, tuple[str, str | None, str, str, str, bool, bool]] = {}
    for raw_code, p in _CODE_LOOKUP.items():
        key = raw_code.upper().strip()
        rows[key] = (key, p.short_code, f"GEO_{p.short_code}", p.name, p.region, False, False)
    rows["824"] = (
        "824", None, "GEO_MULTI_UNALLOCATED",
        "Đa địa bàn chưa phân bổ", "Đa vùng", True, False,
    )
    for unk_code in ("825", "999", "UNKNOWN"):
        rows[unk_code] = (
            unk_code, None, "GEO_UNKNOWN",
            "Chưa xác định", "Chưa xác định", False, True,
        )
    return list(rows.values())


def get_canonical_geography_rows(
    geography_version: str = "v1",
) -> list[tuple[str, str, str | None, str, str, bool, bool]]:
    """Return 65 canonical rows (63 provinces + GEO_MULTI_UNALLOCATED + GEO_UNKNOWN)."""
    records: list[tuple[str, str, str | None, str, str, bool, bool]] = [
        (f"GEO_{p.short_code}", geography_version, p.short_code, p.name, p.region, False, False)
        for p in VIETNAM_63_PROVINCES
    ]
    records.append(
        ("GEO_MULTI_UNALLOCATED", geography_version, None, "Đa địa bàn chưa phân bổ", "Đa vùng", True, False)
    )
    records.append(
        ("GEO_UNKNOWN", geography_version, None, "Chưa xác định", "Chưa xác định", False, True)
    )
    return records


_PROVINCE_MAP_SCHEMA = StructType(
    [
        StructField("raw_code", StringType(), False),
        StructField("canonical_code", StringType(), True),
        StructField("geography_key", StringType(), False),
        StructField("province_name", StringType(), False),
        StructField("region_name", StringType(), False),
        StructField("is_multi", BooleanType(), False),
        StructField("is_unknown", BooleanType(), False),
    ]
)

_CANONICAL_GEO_SCHEMA = StructType(
    [
        StructField("geography_key", StringType(), False),
        StructField("geography_version", StringType(), False),
        StructField("province_code", StringType(), True),
        StructField("province_name", StringType(), False),
        StructField("region_name", StringType(), False),
        StructField("is_multi_geography", BooleanType(), False),
        StructField("is_unknown", BooleanType(), False),
    ]
)


def _sql_lit(val: str | bool | None) -> str:
    if val is None:
        return "CAST(NULL AS STRING)"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    escaped = val.replace("'", "''")
    return f"'{escaped}'"


def register_geography_views(spark: SparkSession, *, geography_version: str = "v1") -> None:
    """Create broadcast-friendly ``_gtmp_province_map`` and ``_gtmp_canonical_geography`` TempViews.

    Uses pure Spark SQL ``VALUES`` so zero PythonRDD/PythonWorker is ever spawned on remote K3s workers.
    """
    pm_values = ",\n        ".join(
        f"({', '.join(_sql_lit(v) for v in row)})" for row in get_province_code_mapping_rows()
    )
    spark.sql(
        f"""
        SELECT
            raw_code, canonical_code, geography_key, province_name, region_name, is_multi, is_unknown
        FROM VALUES
            {pm_values}
        AS t(raw_code, canonical_code, geography_key, province_name, region_name, is_multi, is_unknown)
        """
    ).createOrReplaceTempView("_gtmp_province_map")

    cg_values = ",\n        ".join(
        f"({', '.join(_sql_lit(v) for v in row)})"
        for row in get_canonical_geography_rows(geography_version)
    )
    spark.sql(
        f"""
        SELECT
            geography_key, geography_version, province_code, province_name, region_name, is_multi_geography, is_unknown
        FROM VALUES
            {cg_values}
        AS t(geography_key, geography_version, province_code, province_name, region_name, is_multi_geography, is_unknown)
        """
    ).createOrReplaceTempView("_gtmp_canonical_geography")
