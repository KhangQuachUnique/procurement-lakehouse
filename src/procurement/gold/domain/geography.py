"""Authoritative 63 Provinces of Vietnam and Economic Regions Dictionary.

Normalizes both GSO 2-digit numeric codes ('01', '79', '48'...) and 3-letter alphanumeric
telecom/postal codes ('HNI', 'HCM', 'DNG'...) into canonical 63 provinces with official
economic regions, eliminating the 108 fragmented pairs found in raw procurement data.
"""

import re
import unicodedata
from dataclasses import dataclass


@dataclass(frozen=True)
class ProvinceInfo:
    gso_code: str       # Mã 2 chữ số Tổng cục Thống kê (01 - 96)
    short_code: str     # Mã 3 ký tự viết tắt chuẩn (HNI, HCM, DNG...)
    name: str           # Tên tỉnh/thành phố chuẩn
    region: str         # Vùng kinh tế - xã hội


VIETNAM_63_PROVINCES: list[ProvinceInfo] = [
    # 1. Đồng bằng sông Hồng (11 tỉnh/thành)
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

    # 2. Trung du và miền núi phía Bắc (14 tỉnh)
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

    # 3. Bắc Trung Bộ và Duyên hải miền Trung (14 tỉnh/thành)
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

    # 4. Tây Nguyên (5 tỉnh)
    ProvinceInfo("62", "KTM", "Tỉnh Kon Tum", "Tây Nguyên"),
    ProvinceInfo("64", "GLI", "Tỉnh Gia Lai", "Tây Nguyên"),
    ProvinceInfo("66", "DLC", "Tỉnh Đắk Lắk", "Tây Nguyên"),
    ProvinceInfo("67", "DKN", "Tỉnh Đắk Nông", "Tây Nguyên"),
    ProvinceInfo("68", "LDG", "Tỉnh Lâm Đồng", "Tây Nguyên"),

    # 5. Đông Nam Bộ (6 tỉnh/thành)
    ProvinceInfo("79", "HCM", "Thành phố Hồ Chí Minh", "Đông Nam Bộ"),
    ProvinceInfo("70", "BPC", "Tỉnh Bình Phước", "Đông Nam Bộ"),
    ProvinceInfo("72", "TNH", "Tỉnh Tây Ninh", "Đông Nam Bộ"),
    ProvinceInfo("74", "BDG", "Tỉnh Bình Dương", "Đông Nam Bộ"),
    ProvinceInfo("75", "DNI", "Tỉnh Đồng Nai", "Đông Nam Bộ"),
    ProvinceInfo("77", "VTU", "Tỉnh Bà Rịa - Vũng Tàu", "Đông Nam Bộ"),

    # 6. Đồng bằng sông Cửu Long (13 tỉnh/thành)
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
]

# Lookup caches for O(1) resolution
_CODE_LOOKUP: dict[str, ProvinceInfo] = {}
_NAME_LOOKUP: dict[str, ProvinceInfo] = {}


def _strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.replace("đ", "d").replace("Đ", "D").lower().strip()


def _clean_key(text: str) -> str:
    cleaned = _strip_accents(text)
    cleaned = re.sub(r"^(tinh|thanh pho|tp\.?|t\.)\s+", "", cleaned)
    return re.sub(r"[^a-z0-9]", "", cleaned)


for p in VIETNAM_63_PROVINCES:
    # 2-digit code: "01", "79", "1" -> "01"
    _CODE_LOOKUP[p.gso_code] = p
    _CODE_LOOKUP[str(int(p.gso_code))] = p
    # Short 3-letter code: "HNI", "HCM", "DNG"
    _CODE_LOOKUP[p.short_code.upper()] = p
    # Cleaned name variants
    _NAME_LOOKUP[_clean_key(p.name)] = p

# Custom common abbreviations found in Muasamcong
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
for alias, target_gso in _SPECIAL_ALIASES.items():
    p_info = _CODE_LOOKUP[target_gso]
    _CODE_LOOKUP[alias.upper()] = p_info
    _NAME_LOOKUP[_clean_key(alias)] = p_info


def normalize_province(code_or_name: str | None) -> ProvinceInfo | None:
    """Normalize any province code, short abbreviation, or name into canonical ProvinceInfo."""
    if not code_or_name:
        return None
    raw = str(code_or_name).strip()
    if not raw:
        return None

    # 1. Exact match on code (GSO or 3-letter)
    upper_raw = raw.upper()
    if upper_raw in _CODE_LOOKUP:
        return _CODE_LOOKUP[upper_raw]

    # 2. Try normalized name key
    key = _clean_key(raw)
    if key in _NAME_LOOKUP:
        return _NAME_LOOKUP[key]

    # 3. Numeric string with leading zeros
    if raw.isdigit():
        padded = raw.zfill(2)
        if padded in _CODE_LOOKUP:
            return _CODE_LOOKUP[padded]

    return None


def get_geography_records() -> list[dict[str, any]]:
    """Return rows ready to insert into dim_geography table."""
    records = []
    # 63 canonical provinces
    for p in VIETNAM_63_PROVINCES:
        records.append({
            "geography_key": f"GEO_{p.short_code}",
            "geography_version": "v1",
            "province_code": p.short_code,
            "province_name": p.name,
            "region_name": p.region,
            "is_multi_geography": False,
            "is_unknown": False,
        })
    # Multi-geography synthetic key
    records.append({
        "geography_key": "GEO_MULTI_UNALLOCATED",
        "geography_version": "v1",
        "province_code": None,
        "province_name": "Đa địa bàn chưa phân bổ",
        "region_name": "Đa vùng",
        "is_multi_geography": True,
        "is_unknown": False,
    })
    # Unknown synthetic key
    records.append({
        "geography_key": "GEO_UNKNOWN",
        "geography_version": "v1",
        "province_code": None,
        "province_name": "Chưa xác định",
        "region_name": "Chưa xác định",
        "is_multi_geography": False,
        "is_unknown": True,
    })
    return records
