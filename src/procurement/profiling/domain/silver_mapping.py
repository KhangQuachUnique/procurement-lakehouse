"""Structured Bronze-to-Silver schema mapping definitions for profiling reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ColumnMappingSpec:
    """Mapping specification from Bronze JSON path(s) to a Silver table column."""

    silver_table: str
    silver_column: str
    silver_type: str
    bronze_table: str
    bronze_paths: tuple[str, ...]
    transform_rule: str
    business_note: str


@dataclass(frozen=True)
class SilverTableMappingGroup:
    """Group of column mappings for a specific Bronze resource / table."""

    group_id: str
    title: str
    bronze_tables: tuple[str, ...]
    silver_tables: tuple[str, ...]
    description: str
    mappings: tuple[ColumnMappingSpec, ...]


SILVER_MAPPING_GROUPS: tuple[SilverTableMappingGroup, ...] = (
    # =========================================================================
    # 1. PROJECT (project_detail -> project_revision, project_location)
    # =========================================================================
    SilverTableMappingGroup(
        group_id="project",
        title="Dự Án Đầu Tư (`project_detail` → `silver.project_revision`, `silver.project_location`)",
        bronze_tables=("project_detail",),
        silver_tables=("silver.project_revision", "silver.project_location"),
        description=(
            "Chuẩn hóa hồ sơ Dự án đầu tư từ khối `projectDTO` kết hợp với các trường địa lý cấp root "
            "(`provCode`, `districtCode`, `plocation`). Tự động bóc tách chuỗi JSON mảng địa điểm trong `plocation` "
            "thành bảng con `silver.project_location` (quan hệ 1-N) và liên kết sang KHLCNT qua `bidpProjectProjectPlanList`."
        ),
        mappings=(
            ColumnMappingSpec(
                "silver.project_revision", "revision_id", "VARCHAR (PK)",
                "project_detail", ("$.source_id", "$.source_version", "$.content_hash"),
                "SHA-256(['project', entity_id, source_version, root_hash])",
                "Khóa chính định danh phiên bản dự án (bất biến).",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "entity_id", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.id", "$.source_id"),
                "SHA-256(['project', 'uuid', clean_text(projectDTO.id)])",
                "Khóa thực thể xuyên suốt các phiên bản điều chỉnh dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "entity_type", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.id",),
                "Hằng số 'project'",
                "Phân loại thực thể trong kiến trúc Silver Lakehouse.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "observation_id", "VARCHAR (FK)",
                "project_detail", ("$._dlt_id", "$.content_hash"),
                "SHA-256([file_sha256, row_ordinal, content_hash])",
                "Khóa ngoại truy vết dòng vật lý trong bảng `silver.observation`.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "is_current", "BOOLEAN",
                "project_detail", ("$.source_version", "$.payload.projectDTO.version"),
                "Release Resolver (SCD Type 2 flag)",
                "Đánh dấu `true` cho bản ghi hiệu lực mới nhất của dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "project_no", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.no", "$.payload.no"),
                "clean_text(projectDTO.no)",
                "Mã số dự án hiển thị trên cổng e-GP (VD: PR2400000017).",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "business_number", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.no", "$.payload.no"),
                "clean_text(projectDTO.no)",
                "Mã nghiệp vụ chuẩn hóa dùng chung cho tra cứu liên thực thể.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "title", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.name", "$.payload.name"),
                "clean_text(projectDTO.name)",
                "Tên dự án đầu tư xây dựng / mua sắm công.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "investor_code", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.investorCode",),
                "clean_text(projectDTO.investorCode)",
                "Mã định danh Chủ đầu tư trên hệ thống e-GP.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "investor_name", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.investorName",),
                "clean_text(projectDTO.investorName)",
                "Tên pháp nhân đầy đủ của Chủ đầu tư.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "buyer_id", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.investorCode",),
                "clean_text(projectDTO.investorCode)",
                "Alias chuẩn hóa của bên mua / chủ đầu tư.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "buyer_name", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.investorName",),
                "clean_text(projectDTO.investorName)",
                "Alias tên bên mua / chủ đầu tư.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "total_investment", "DECIMAL(38,6)",
                "project_detail", ("$.payload.projectDTO.investTotal", "$.payload.investTotal"),
                "parse_money(projectDTO.investTotal) -> warn 'invalid_money' nếu lỗi",
                "Tổng mức đầu tư được phê duyệt của dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "amount", "DECIMAL(38,6)",
                "project_detail", ("$.payload.projectDTO.investTotal", "$.payload.investTotal"),
                "parse_money(projectDTO.investTotal)",
                "Trường số tiền chuẩn hóa đồng nhất giữa các bảng revision.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "currency", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.investTotalUnit", "$.payload.investTotalUnit"),
                "clean_text(investTotalUnit) ?: clean_text(investUnit)",
                "Đơn vị tiền tệ của tổng mức đầu tư (chủ yếu là VND).",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "decision_no", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.decisionNo",),
                "clean_text(projectDTO.decisionNo)",
                "Số quyết định phê duyệt dự án đầu tư.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "decision_date", "DATE",
                "project_detail", ("$.payload.projectDTO.decisionDate",),
                "parse_date(projectDTO.decisionDate); kiểm tra decisionDate <= publicDate",
                "Ngày ký quyết định phê duyệt dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "prov_code", "VARCHAR",
                "project_detail", ("$.payload.provCode", "$.payload.projectDTO.provCode"),
                "clean_pipe_code(provCode) (bóc '|115|' -> '115', lọc '||' -> NULL)",
                "Mã tỉnh/thành phố chính của dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "district_code", "VARCHAR",
                "project_detail", ("$.payload.districtCode", "$.payload.projectDTO.districtCode"),
                "clean_pipe_code(districtCode) (bóc '|11509|' -> '11509', lọc '||' -> NULL)",
                "Mã quận/huyện chính của dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "public_date", "DATE",
                "project_detail", ("$.payload.projectDTO.publicDate", "$.payload.publicDate"),
                "parse_date(projectDTO.publicDate)",
                "Ngày công khai dự án (Khóa phân vùng chính trên Iceberg).",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "public_date_raw", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.publicDate", "$.payload.publicDate"),
                "clean_text(projectDTO.publicDate)",
                "Chuỗi thời gian công khai gốc từ nguồn.",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "status_raw", "VARCHAR",
                "project_detail", ("$.payload.projectDTO.status", "$.payload.status"),
                "clean_text(projectDTO.status)",
                "Mã trạng thái gốc của dự án (VD: '01' = Đã đăng tải).",
            ),
            ColumnMappingSpec(
                "silver.project_revision", "root_json", "TEXT",
                "project_detail", ("$.payload.projectDTO",),
                "canonical_json(merged_root)",
                "Chuỗi JSON chuẩn hóa toàn bộ thuộc tính gốc để bảo toàn 100% thông tin.",
            ),
            ColumnMappingSpec(
                "silver.project_location", "location_id", "VARCHAR (PK)",
                "project_detail", ("$.payload.plocation", "$.payload.provCode"),
                "SHA-256([revision_id, 'location', ordinal, loc_data])",
                "Khóa chính dòng địa điểm thực hiện dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_location", "revision_id", "VARCHAR (FK)",
                "project_detail", ("$.payload.projectDTO.id",),
                "FK -> silver.project_revision.revision_id",
                "Liên kết về phiên bản dự án cha.",
            ),
            ColumnMappingSpec(
                "silver.project_location", "province_code", "VARCHAR",
                "project_detail", ("$.payload.plocation", "$.payload.provCode"),
                "json.loads(plocation)[i].provCode -> clean_pipe_code (fallback $.payload.provCode)",
                "Mã tỉnh/thành phố sau khi giải mã mảng JSON `plocation`.",
            ),
            ColumnMappingSpec(
                "silver.project_location", "province_name", "VARCHAR",
                "project_detail", ("$.payload.plocation", "$.payload.provName"),
                "json.loads(plocation)[i].provName -> clean_text (fallback $.payload.provName)",
                "Tên tỉnh/thành phố thực hiện dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_location", "district_code", "VARCHAR",
                "project_detail", ("$.payload.plocation", "$.payload.districtCode"),
                "json.loads(plocation)[i].districtCode -> clean_pipe_code (fallback $.payload.districtCode)",
                "Mã quận/huyện thực hiện dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_location", "district_name", "VARCHAR",
                "project_detail", ("$.payload.plocation", "$.payload.districtName"),
                "json.loads(plocation)[i].districtName -> clean_text (fallback $.payload.districtName)",
                "Tên quận/huyện thực hiện dự án.",
            ),
            ColumnMappingSpec(
                "silver.project_location", "ward_code", "VARCHAR",
                "project_detail", ("$.payload.plocation",),
                "json.loads(plocation)[i].wardCode -> clean_pipe_code",
                "Mã phường/xã (nếu có trong phần tử `plocation`).",
            ),
            ColumnMappingSpec(
                "silver.project_location", "ward_name", "VARCHAR",
                "project_detail", ("$.payload.plocation",),
                "json.loads(plocation)[i].wardName -> clean_text",
                "Tên phường/xã (nếu có trong phần tử `plocation`).",
            ),
            ColumnMappingSpec(
                "silver.project_location", "location_name", "VARCHAR",
                "project_detail", ("$.payload.plocation",),
                "Ghép chuỗi sạch '{wardName}, {districtName}, {provName}'",
                "Địa danh hiển thị đầy đủ đã loại bỏ cú pháp mảng JSON thô.",
            ),
        ),
    ),

    # =========================================================================
    # 2. KHLCNT PLAN (khlcnt_plan_detail -> plan_revision)
    # =========================================================================
    SilverTableMappingGroup(
        group_id="khlcnt_plan",
        title="Kế Hoạch Lựa Chọn Nhà Thầu (`khlcnt_plan_detail` → `silver.plan_revision`)",
        bronze_tables=("khlcnt_plan_detail",),
        silver_tables=("silver.plan_revision",),
        description=(
            "Chuẩn hóa thực thể Kế hoạch lựa chọn nhà thầu (KHLCNT) từ khối `bidPoBidpPlanProjectDetailView`. "
            "Giải quyết tham chiếu ngược về Dự án đầu tư (`project_id`) thông qua `pid` (UUID) hoặc `pno` (Mã dự án)."
        ),
        mappings=(
            ColumnMappingSpec(
                "silver.plan_revision", "revision_id", "VARCHAR (PK)",
                "khlcnt_plan_detail", ("$.source_id", "$.source_version", "$.content_hash"),
                "SHA-256(['plan', entity_id, source_version, root_hash])",
                "Khóa chính định danh phiên bản KHLCNT.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "entity_id", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.id",),
                "SHA-256(['plan', 'uuid', clean_text(root.id)])",
                "Khóa định danh thực thể KHLCNT xuyên suốt các lần sửa đổi.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "entity_type", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.id",),
                "Hằng số 'plan'",
                "Phân loại thực thể Kế hoạch.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "observation_id", "VARCHAR (FK)",
                "khlcnt_plan_detail", ("$._dlt_id", "$.content_hash"),
                "SHA-256([file_sha256, row_ordinal, content_hash])",
                "Khóa ngoại truy vết bản ghi Bronze gốc.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "is_current", "BOOLEAN",
                "khlcnt_plan_detail", ("$.source_version",),
                "Release Resolver (SCD Type 2 flag)",
                "Cờ đánh dấu phiên bản KHLCNT mới nhất.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "plan_no", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.planNo",),
                "clean_text(root.planNo)",
                "Số hiệu KHLCNT công bố trên e-GP (VD: PL2400001234).",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "title", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.name",),
                "clean_text(root.name)",
                "Tên kế hoạch lựa chọn nhà thầu.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "investor_name", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.investorName",),
                "clean_text(root.investorName)",
                "Tên Chủ đầu tư / Cơ quan lập kế hoạch.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "project_id", "VARCHAR (FK)",
                "khlcnt_plan_detail", (
                    "$.payload.bidPoBidpPlanProjectDetailView.pid",
                    "$.payload.bidPoBidpPlanProjectDetailView.pno",
                ),
                "Reference Resolver phân giải từ `pid` (scheme='uuid') hoặc `pno` (scheme='number')",
                "Khóa ngoại trỏ sang `silver.project_revision.entity_id`.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "project_no", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.pno",),
                "clean_project_reference(root.pno) (lọc sentinel 'N/A', 'NULL', 'NONE')",
                "Mã số dự án đầu tư cha.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "plan_type", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.planType",),
                "clean_text(root.planType)",
                "Phân loại kế hoạch (DTPT: Đầu tư phát triển, TX: Thường xuyên, KHAC).",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "total_investment", "DECIMAL(38,6)",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.investTotal",),
                "parse_money(root.investTotal) -> warn 'invalid_money' nếu lỗi",
                "Tổng mức đầu tư / dự toán của kế hoạch.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "currency", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.investTotalUnit",),
                "clean_text(root.investTotalUnit)",
                "Đơn vị tiền tệ (VND).",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "decision_date", "DATE",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.decisionDate",),
                "parse_date(root.decisionDate)",
                "Ngày ban hành quyết định phê duyệt KHLCNT.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "decision_no", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.decisionNo",),
                "clean_text(root.decisionNo)",
                "Số quyết định phê duyệt KHLCNT.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "decision_agency", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.decisionAgency",),
                "clean_text(root.decisionAgency)",
                "Cơ quan có thẩm quyền phê duyệt KHLCNT.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "bid_pack", "BIGINT",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.bidPack",),
                "_parse_bid_pack(root.bidPack) (số nguyên không âm)",
                "Tổng số lượng gói thầu thuộc kế hoạch.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "location", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.location",),
                "clean_text(root.location)",
                "Địa điểm thực hiện ghi ở cấp kế hoạch (thường thưa thớt, chi tiết nằm ở gói thầu).",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "public_date", "DATE",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.publicDate",),
                "parse_date(root.publicDate)",
                "Ngày đăng tải KHLCNT (Khóa phân vùng Iceberg).",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "status_raw", "VARCHAR",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView.status",),
                "clean_text(root.status)",
                "Trạng thái gốc của KHLCNT.",
            ),
            ColumnMappingSpec(
                "silver.plan_revision", "root_json", "TEXT",
                "khlcnt_plan_detail", ("$.payload.bidPoBidpPlanProjectDetailView",),
                "canonical_json(root)",
                "Bảo toàn nguyên vẹn JSON của `bidPoBidpPlanProjectDetailView`.",
            ),
        ),
    ),

    # =========================================================================
    # 3. KHLCNT PACKAGE (khlcnt_bid_package_detail -> package_revision, package_location, package_lot)
    # =========================================================================
    SilverTableMappingGroup(
        group_id="khlcnt_package",
        title="Gói Thầu Trong KHLCNT (`khlcnt_bid_package_detail` → `silver.package_revision`, `silver.package_location`, `silver.package_lot`)",
        bronze_tables=("khlcnt_bid_package_detail",),
        silver_tables=("silver.package_revision", "silver.package_location", "silver.package_lot"),
        description=(
            "Chuẩn hóa thông tin Gói thầu thuộc KHLCNT từ payload phẳng cấp root và phân rã 2 mảng con: "
            "`bidLocation[]` thành bảng `silver.package_location` và `bidpBidLotList[]` thành bảng `silver.package_lot`."
        ),
        mappings=(
            ColumnMappingSpec(
                "silver.package_revision", "revision_id", "VARCHAR (PK)",
                "khlcnt_bid_package_detail", ("$.source_id", "$.source_version", "$.content_hash"),
                "SHA-256(['package', entity_id, source_version, root_hash])",
                "Khóa chính định danh phiên bản gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "entity_id", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.id", "$.source_id"),
                "SHA-256(['package', 'uuid', _valid_reference(payload.id)])",
                "Khóa định danh thực thể gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "entity_type", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.id",),
                "Hằng số 'package'",
                "Phân loại thực thể Gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "observation_id", "VARCHAR (FK)",
                "khlcnt_bid_package_detail", ("$._dlt_id", "$.content_hash"),
                "SHA-256([file_sha256, row_ordinal, content_hash])",
                "Khóa ngoại truy vết bản ghi Bronze gốc.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "is_current", "BOOLEAN",
                "khlcnt_bid_package_detail", ("$.source_version",),
                "Release Resolver (SCD Type 2 flag)",
                "Đánh dấu phiên bản hiện hành mới nhất của gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "package_no", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidNo",),
                "clean_text(payload.bidNo)",
                "Mã gói thầu (VD: BP2400012345).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "title", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidName",),
                "clean_text(payload.bidName)",
                "Tên gói thầu trong kế hoạch.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "plan_no", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.planNo",),
                "_valid_reference(payload.planNo) (lọc 'N/A', 'NULL')",
                "Mã số KHLCNT cha.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "plan_id", "VARCHAR (FK)",
                "khlcnt_bid_package_detail", ("$.payload.planId", "$.payload.planNo"),
                "Reference Resolver phân giải từ `planId` (uuid) hoặc `planNo` (number)",
                "Khóa ngoại trỏ về `silver.plan_revision.entity_id`.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "bid_price", "DECIMAL(38,6)",
                "khlcnt_bid_package_detail", ("$.payload.bidPrice",),
                "parse_money(payload.bidPrice) -> warn 'invalid_money' nếu lỗi",
                "Giá gói thầu được phê duyệt trong KHLCNT.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "currency", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidPriceUnit",),
                "clean_text(payload.bidPriceUnit)",
                "Đơn vị tiền tệ của giá gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "process_apply", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.processApply",),
                "clean_text(payload.processApply)",
                "Quy trình áp dụng (LDT: Luật Đấu thầu, KHAC: Vốn khác/ADB/WB).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "bid_field", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidField",),
                "clean_text(payload.bidField)",
                "Lĩnh vực đấu thầu (HH, XL, TV, PTV, HON_HOP).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "bid_form", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidForm",),
                "clean_text(payload.bidForm)",
                "Hình thức lựa chọn nhà thầu (DTRR, CHCT, CDT, ...).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "bid_mode", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidMode",),
                "clean_text(payload.bidMode)",
                "Phương thức LCNT (1_MTHS: 1 giai đoạn 1 túi, 1_HTHS: 1 giai đoạn 2 túi, ...).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "capital_detail", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.capitalDetail",),
                "clean_text(payload.capitalDetail)",
                "Chi tiết nguồn vốn thực hiện gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "contract_type", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.ctype",),
                "clean_text(payload.ctype)",
                "Loại hợp đồng (TG: Trọn gói, DGCD: Đơn giá cố định, DGDC: Đơn giá điều chỉnh).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "execution_period", "BIGINT",
                "khlcnt_bid_package_detail", ("$.payload.cperiod",),
                "_nonnegative_int(payload.cperiod)",
                "Thời gian thực hiện hợp đồng / gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "execution_period_unit", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.cperiodUnit",),
                "clean_text(payload.cperiodUnit)",
                "Đơn vị thời gian thực hiện (D: Ngày, M: Tháng).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "is_domestic", "BOOLEAN",
                "khlcnt_bid_package_detail", ("$.payload.isDomestic",),
                "parse_boolean(payload.isDomestic) (1/0 -> True/False)",
                "Cờ đấu thầu trong nước (`true`) hay quốc tế (`false`).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "is_internet", "BOOLEAN",
                "khlcnt_bid_package_detail", ("$.payload.isInternet",),
                "parse_boolean(payload.isInternet) (1/0 -> True/False)",
                "Cờ đấu thầu qua mạng (`true`) hay không qua mạng (`false`).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "bid_start_unit", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidStartUnit",),
                "clean_text(payload.bidStartUnit)",
                "Đơn vị thời gian bắt đầu tổ chức LCNT (Q: Quý, M: Tháng).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "bid_start_year", "BIGINT",
                "khlcnt_bid_package_detail", ("$.payload.bidStartYear",),
                "_positive_year(payload.bidStartYear) (lọc sentinel 0 -> NULL)",
                "Năm dự kiến bắt đầu tổ chức lựa chọn nhà thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "bid_start_quarter", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidStartQuarter",),
                "_quarter(payload.bidStartQuarter) (lọc sentinel '0' -> NULL)",
                "Quý dự kiến bắt đầu tổ chức LCNT (I, II, III, IV).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "bid_start_month", "BIGINT",
                "khlcnt_bid_package_detail", ("$.payload.bidStartMonth",),
                "_month(payload.bidStartMonth) (lọc sentinel 0 -> NULL; kiểm tra 1..12)",
                "Tháng dự kiến bắt đầu tổ chức LCNT (1..12).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "public_date", "DATE",
                "khlcnt_bid_package_detail", (
                    "$.payload.createdDate",
                    "$.payload.originalCreateDate",
                    "$.payload.exchangeDate",
                ),
                "parse_date(createdDate ?: originalCreateDate ?: exchangeDate)",
                "Ngày tạo/công bố gói thầu (Khóa phân vùng Iceberg).",
            ),
            ColumnMappingSpec(
                "silver.package_revision", "root_json", "TEXT",
                "khlcnt_bid_package_detail", ("$.payload",),
                "canonical_json(payload)",
                "Bảo toàn nguyên vẹn JSON gói thầu.",
            ),
            # Child 1: package_location
            ColumnMappingSpec(
                "silver.package_location", "location_id", "VARCHAR (PK)",
                "khlcnt_bid_package_detail", ("$.payload.bidLocation[].id",),
                "SHA-256([revision_id, 'location', ordinal])",
                "Khóa chính địa điểm thực hiện gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_location", "revision_id", "VARCHAR (FK)",
                "khlcnt_bid_package_detail", ("$.payload.id",),
                "FK -> silver.package_revision.revision_id",
                "Khóa ngoại trỏ về phiên bản gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_location", "location_ordinal", "BIGINT",
                "khlcnt_bid_package_detail", ("$.payload.bidLocation",),
                "Chỉ số phần tử (0, 1, ...) trong mảng `bidLocation[]`",
                "Thứ tự địa điểm trong danh sách.",
            ),
            ColumnMappingSpec(
                "silver.package_location", "province_code", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidLocation[].provCode",),
                "clean_pipe_code(location.provCode)",
                "Mã tỉnh/thành phố thực hiện gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_location", "province_name", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidLocation[].provName",),
                "clean_text(location.provName)",
                "Tên tỉnh/thành phố thực hiện gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_location", "district_code", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidLocation[].districtCode",),
                "clean_pipe_code(location.districtCode)",
                "Mã quận/huyện thực hiện gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_location", "district_name", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidLocation[].districtName",),
                "clean_text(location.districtName)",
                "Tên quận/huyện thực hiện gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_location", "ward_code", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidLocation[].wardCode",),
                "clean_pipe_code(location.wardCode)",
                "Mã phường/xã thực hiện gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_location", "ward_name", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidLocation[].wardName",),
                "clean_text(location.wardName)",
                "Tên phường/xã thực hiện gói thầu.",
            ),
            # Child 2: package_lot
            ColumnMappingSpec(
                "silver.package_lot", "lot_id", "VARCHAR (PK)",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].id",),
                "SHA-256([revision_id, 'lot', ordinal, source_lot_id])",
                "Khóa chính phần/lô thuộc gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "revision_id", "VARCHAR (FK)",
                "khlcnt_bid_package_detail", ("$.payload.id",),
                "FK -> silver.package_revision.revision_id",
                "Khóa ngoại trỏ về phiên bản gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "source_lot_id", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].id",),
                "_valid_reference(lot.id)",
                "ID gốc của phần/lô trên hệ thống e-GP.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "lot_ordinal", "BIGINT",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList",),
                "Chỉ số phần tử (0, 1, ...) trong mảng `bidpBidLotList[]`",
                "Thứ tự phần/lô trong gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "lot_no", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].lotNo", "$.payload.bidpBidLotList[].bidNo"),
                "clean_text(lot.lotNo)",
                "Mã số phần/lô.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "lot_name", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].lotName", "$.payload.bidpBidLotList[].bidName"),
                "clean_text(lot.lotName)",
                "Tên phần/lô hoặc danh mục hàng hóa/thuốc.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "lot_price", "DECIMAL(38,6)",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].lotPrice",),
                "parse_money(lot.lotPrice)",
                "Giá dự toán của phần/lô.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "currency", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].lotPriceUnit", "$.payload.bidPriceUnit"),
                "clean_text(lot.lotPriceUnit) ?: clean_text(payload.bidPriceUnit)",
                "Đơn vị tiền tệ của phần/lô.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "medicine_code", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].medicineCode",),
                "clean_text(lot.medicineCode)",
                "Mã hoạt chất / mã thuốc (đối với gói thầu dược phẩm/y tế).",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "quantity", "DECIMAL(38,6)",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].quantity",),
                "parse_money(lot.quantity)",
                "Số lượng hàng hóa / định lượng mời thầu.",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "uom", "VARCHAR",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].uom",),
                "clean_text(lot.uom)",
                "Đơn vị tính (viên, lọ, hộp, cái, ...).",
            ),
            ColumnMappingSpec(
                "silver.package_lot", "planned_unit_price", "DECIMAL(38,6)",
                "khlcnt_bid_package_detail", ("$.payload.bidpBidLotList[].pricePlan",),
                "parse_money(lot.pricePlan)",
                "Đơn giá kế hoạch của mặt hàng trong lô.",
            ),
        ),
    ),

    # =========================================================================
    # 4. NOTIFY CONTRACTOR (notify_contractor_* -> notice_revision, notice_lot, notice_location, notice_reoffer_submission)
    # =========================================================================
    SilverTableMappingGroup(
        group_id="notify_contractor",
        title="Thông Báo Mời Thầu (`notify_contractor_*` → `silver.notice_revision`, `silver.notice_lot`, `silver.notice_location`, `silver.notice_reoffer_submission`)",
        bronze_tables=(
            "notify_contractor_standard_detail",
            "notify_contractor_vk_adb_detail",
            "notify_contractor_reoffer_detail",
        ),
        silver_tables=(
            "silver.notice_revision",
            "silver.notice_lot",
            "silver.notice_location",
            "silver.notice_reoffer_submission",
        ),
        description=(
            "Hợp nhất 3 dòng dữ liệu Thông báo mời thầu: Standard (`bidoNotifyContractorM` / `bidNoContractorResponse.bidNotification`), "
            "VK/ADB (`bidoNotifyContractorP`), và Reoffer (`$` top-level) về mô hình đích chung `silver.notice_revision` (contract `notice-2`). "
            "Kế thừa có kiểm chứng các thuộc tính khuyết từ `bidpPlanDetail` / `bidDetail` và `bidoBidStatus`."
        ),
        mappings=(
            ColumnMappingSpec(
                "silver.notice_revision", "revision_id", "VARCHAR (PK)",
                "notify_contractor_standard_detail", ("$.source_id", "$.source_version", "$.content_hash"),
                "SHA-256(['notice', entity_id, source_version, root_hash])",
                "Khóa chính định danh phiên bản TBMT.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "entity_id", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.notifyNo",
                    "$.payload.bidoNotifyContractorP.notifyNo",
                    "$.payload.reofferNo",
                ),
                "SHA-256(['notice', id_scheme, notifyNo | reofferNo])",
                "Khóa thực thể TBMT xuyên suốt các lần sửa đổi / làm rõ (`00`, `01`, ...).",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "source_table / source_family", "VARCHAR",
                "notify_contractor_standard_detail", ("$.source_id",),
                "Ghi nhận bảng nguồn & dòng dữ liệu ('standard', 'vk_adb', 'reoffer')",
                "Phân biệt nguồn gốc biểu mẫu TBMT.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "source_document_id", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.id",
                    "$.payload.bidoNotifyContractorP.id",
                    "$.payload.id",
                ),
                "clean_text(<root>.id)",
                "UUID bản ghi gốc của thông báo trên hệ thống e-GP.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "notify_no / business_number", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.notifyNo",
                    "$.payload.bidoNotifyContractorP.notifyNo",
                    "$.payload.notifyNo",
                ),
                "clean_text(<root>.notifyNo) (kiểm tra đối chiếu với $.source_id)",
                "Mã TBMT định danh quốc gia (VD: IB2400012345).",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "notify_version", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.notifyVersion",
                    "$.payload.bidoNotifyContractorP.notifyVersion",
                    "$.source_version",
                ),
                "clean_text(<root>.notifyVersion) (kiểm tra đối chiếu với $.source_version)",
                "Phiên bản TBMT (`00`: gốc, `01`+: sửa đổi).",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "reoffer_no / reoffer_version", "VARCHAR",
                "notify_contractor_reoffer_detail", ("$.payload.reofferNo", "$.payload.reofferVersion"),
                "clean_text(payload.reofferNo), clean_text(payload.reofferVersion)",
                "Mã và phiên bản thông báo chào giá lại (chỉ có ở bảng Reoffer).",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "package_id", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.bidId",
                    "$.payload.bidoNotifyContractorP.bidId",
                    "$.payload.bidId",
                ),
                "clean_text(<root>.bidId)",
                "UUID gói thầu liên kết trực tiếp với KHLCNT.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "package_no", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.bidNo",
                    "$.payload.bidoNotifyContractorP.bidNo",
                    "$.payload.bidNo",
                ),
                "clean_text(<root>.bidNo)",
                "Mã số gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "title", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.bidName",
                    "$.payload.bidoNotifyContractorP.bidName",
                    "$.payload.bidName",
                ),
                "clean_text(<root>.bidName)",
                "Tên gói thầu mời thầu.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "plan_id / plan_version", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidpPlanDetail.planId",
                    "$.payload.bidpPlanDetail.planVersion",
                    "$.payload.bidDetail.planId",
                ),
                "Chỉ gán khi `bidpPlanDetail.id == package_id` (ngược lại phát cảnh báo `unverified_embedded_package`)",
                "ID và phiên bản KHLCNT trích xuất từ khối kế hoạch nhúng đã xác thực.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "plan_no / plan_name / plan_type_raw", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.planNo",
                    "$.payload.bidoNotifyContractorM.planName",
                    "$.payload.bidoNotifyContractorM.planType",
                ),
                "clean_text(<root>.planNo / planName / planType)",
                "Mã, tên và phân loại kế hoạch lựa chọn nhà thầu.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "project_name", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.projectName",
                    "$.payload.bidoNotifyContractorP.projectName",
                ),
                "clean_text(<root>.projectName)",
                "Tên dự án đầu tư ghi trên TBMT.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "procuring_entity_code / buyer_id", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.procuringEntityCode",
                    "$.payload.bidoNotifyContractorP.procuringEntityCode",
                    "$.payload.procuringEntityCode",
                ),
                "clean_text(<root>.procuringEntityCode)",
                "Mã định danh Bên mời thầu.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "procuring_entity_name / buyer_name", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.procuringEntityName",
                    "$.payload.bidoNotifyContractorP.procuringEntityName",
                    "$.payload.procuringEntityName",
                ),
                "clean_text(<root>.procuringEntityName)",
                "Tên pháp nhân Bên mời thầu.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "investor_code / investor_name", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.investorCode",
                    "$.payload.bidoNotifyContractorM.investorName",
                    "$.payload.bidoNotifyContractorP.investorName",
                ),
                "clean_text(<root>.investorCode / investorName)",
                "Mã và tên Chủ đầu tư.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "bid_field", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.investField",
                    "$.payload.bidpPlanDetail.bidField",
                    "$.payload.bidoNotifyContractorP.investField",
                ),
                "clean_text(<root>.investField) ?: clean_text(bidpPlanDetail.bidField)",
                "Lĩnh vực đấu thầu (có cơ chế fallback sang `bidpPlanDetail.bidField`).",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "bid_form / bid_mode / process_apply_raw", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.bidForm",
                    "$.payload.bidoNotifyContractorM.bidMode",
                    "$.payload.bidoNotifyContractorM.processApply",
                ),
                "clean_text(<root>.bidForm / bidMode / processApply)",
                "Hình thức, phương thức và quy trình áp dụng đấu thầu.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "bid_price / amount", "DECIMAL(38,6)",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.bidPrice",
                    "$.payload.bidoNotifyContractorP.bidPrice",
                ),
                "parse_money(<root>.bidPrice)",
                "Giá gói thầu công bố trong TBMT.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "estimate_price", "DECIMAL(38,6)",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.bidEstimatePrice",
                    "$.payload.bidoNotifyContractorP.bidEstimatePrice",
                ),
                "parse_money(<root>.bidEstimatePrice)",
                "Dự toán gói thầu được duyệt (nếu cập nhật sau KHLCNT).",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "guarantee_amount / guarantee_form", "DECIMAL(38,6) / VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.guaranteeValue",
                    "$.payload.bidoNotifyContractorM.guaranteeForm",
                    "$.payload.bidoNotifyContractorP.bidGuaranteeValue",
                    "$.payload.bidoNotifyContractorP.bidGuaranteeForm",
                ),
                "Standard dùng `guaranteeValue`/`guaranteeForm`; VK-ADB dùng `bidGuaranteeValue`/`bidGuaranteeForm`",
                "Số tiền và hình thức bảo đảm dự thầu.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "initial_price / price_step", "DECIMAL(38,6)",
                "notify_contractor_reoffer_detail", ("$.payload.priceInit", "$.payload.priceStep"),
                "parse_money(payload.priceInit), parse_money(payload.priceStep)",
                "Giá khởi điểm và bước giá trong chào giá trực tuyến (Reoffer).",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "contract_type / contract_duration_raw / unit", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.contractType",
                    "$.payload.bidoNotifyContractorM.contractPeriod",
                    "$.payload.bidoNotifyContractorP.cType",
                    "$.payload.bidoNotifyContractorP.cPeriod",
                    "$.payload.bidpPlanDetail.cperiod",
                ),
                "Standard: `contractType`/`contractPeriod`; VK-ADB: `cType`/`cPeriod`; fallback `bidpPlanDetail.cperiod`",
                "Loại hợp đồng và thời gian thực hiện hợp đồng.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "published_at / public_date", "TIMESTAMPTZ / DATE",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.publicDate",
                    "$.payload.bidoNotifyContractorP.publicDate",
                    "$.payload.publicDate",
                ),
                "parse_iso_datetime(<root>.publicDate) & parse_date(<root>.publicDate)",
                "Thời điểm đăng tải TBMT và ngày phân vùng Iceberg.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "bid_open_date / bid_close_date", "TIMESTAMPTZ",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.bidOpenDate",
                    "$.payload.bidoNotifyContractorM.bidCloseDate",
                    "$.payload.bidoNotifyContractorP.bidOpenDate",
                    "$.payload.bidoNotifyContractorP.bidCloseDate",
                ),
                "parse_iso_datetime(bidOpenDate / bidCloseDate); kiểm tra published_at <= bid_close_date <= bid_open_date",
                "Thời điểm đóng thầu và mở thầu.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "is_internet / is_domestic / is_prequalification", "BOOLEAN",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.isInternet",
                    "$.payload.bidoNotifyContractorM.isDomestic",
                    "$.payload.bidoNotifyContractorM.isPrequalification",
                ),
                "parse_boolean(<root>.isInternet / isDomestic / isPrequalification)",
                "Các cờ thuộc tính đấu thầu qua mạng, trong nước và sơ tuyển.",
            ),
            ColumnMappingSpec(
                "silver.notice_revision", "is_multi_lot", "BOOLEAN",
                "notify_contractor_standard_detail", (
                    "$.payload.bidoNotifyContractorM.isMultiLot",
                    "$.payload.bidpPlanDetail.isMultiLot",
                    "$.payload.bidoBidStatus.isMultiLot",
                ),
                "parse_boolean(<root>.isMultiLot ?: bidpPlanDetail.isMultiLot ?: bidoBidStatus.isMultiLot)",
                "Cờ gói thầu chia nhiều phần/lô (được cứu 100% nhờ chuỗi fallback 3 tầng).",
            ),
            # Child 1: notice_lot
            ColumnMappingSpec(
                "silver.notice_lot", "lot_no / lot_name / lot_price / estimate_price / guarantee_amount", "VARCHAR / DECIMAL(38,6)",
                "notify_contractor_standard_detail", (
                    "$.payload.bidNoContractorResponse.bidNotification.lotDTOList[]",
                    "$.payload.bidoNotifyContractorM.lotDTOList[]",
                    "$.payload.bidoNotifyContractorP.lotDTOList[]",
                ),
                "Unnest `lotDTOList[]`; parse_money cho `lotPrice`, `lotEstimatePrice`, `lotGuaranteeValue`",
                "Bảng con chi tiết từng phần/lô trong TBMT.",
            ),
            # Child 2: notice_location
            ColumnMappingSpec(
                "silver.notice_location", "province_code / province_name / district_code / district_name / ward_code / ward_name", "VARCHAR",
                "notify_contractor_standard_detail", (
                    "$.payload.bidpBidLocationList[]",
                    "$.payload.lsBidpBidLocationDTO[]",
                    "$.payload.bidDetail.bidLocation[]",
                ),
                "Unnest mảng địa điểm; áp dụng `clean_pipe_code` cho các cột mã và `clean_text` cho các cột tên",
                "Bảng con địa điểm thực hiện gói thầu trong TBMT.",
            ),
            # Child 3: notice_reoffer_submission
            ColumnMappingSpec(
                "silver.notice_reoffer_submission", "submission_id / contractor_code / price / final_price / offered_at", "VARCHAR / DECIMAL(38,6) / TIMESTAMPTZ",
                "notify_contractor_reoffer_detail", (
                    "$.payload.contractorsResult[]",
                    "$.payload.contractorsResultAll[]",
                ),
                "Unnest lịch sử chào giá lại; parse_money cho `reofferPrice`, `reofferPriceFinal`; parse_iso_datetime cho `reofferDate`",
                "Bảng con lưu toàn bộ các lượt chào giá trực tuyến trong gói Reoffer.",
            ),
        ),
    ),

    # =========================================================================
    # 5. CONTRACTOR RESULT (contractor_result_detail -> result_revision, award_lot, award_contractor)
    # =========================================================================
    SilverTableMappingGroup(
        group_id="contractor_result",
        title="Kết Quả Lựa Chọn Nhà Thầu (`contractor_result_detail` → `silver.result_revision`, `silver.award_lot`, `silver.award_contractor`)",
        bronze_tables=("contractor_result_detail",),
        silver_tables=(
            "silver.result_revision",
            "silver.award_lot",
            "silver.award_contractor",
        ),
        description=(
            "Chuẩn hóa Kết quả lựa chọn nhà thầu (KQLCNT) theo cấu trúc phân cấp 3 tầng: "
            "Quyết định KQLCNT (`bideContractorInputResultDTO`) → Kết quả theo Lô (`lotResultDTO[]` → `silver.award_lot`) "
            "→ Danh sách Nhà thầu dự thầu & trúng thầu (`contractorList[]` → `silver.award_contractor`)."
        ),
        mappings=(
            ColumnMappingSpec(
                "silver.result_revision", "revision_id", "VARCHAR (PK)",
                "contractor_result_detail", ("$.source_id", "$.source_version", "$.content_hash"),
                "SHA-256(['result', entity_id, source_version, root_hash])",
                "Khóa chính định danh phiên bản KQLCNT.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "entity_id", "VARCHAR",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.id", "$.source_id"),
                "SHA-256(['result', 'uuid', clean_text(root.id)])",
                "Khóa định danh thực thể KQLCNT.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "result_id / result_version", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.id",
                    "$.payload.bideContractorInputResultDTO.resultVersion",
                ),
                "clean_text(root.id), clean_text(root.resultVersion)",
                "UUID và phiên bản của hồ sơ KQLCNT.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "notify_no / notify_version", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.notifyNo",
                    "$.payload.bideContractorInputResultDTO.notifyVersion",
                ),
                "clean_text(root.notifyNo), clean_text(root.notifyVersion)",
                "Mã TBMT và phiên bản TBMT tương ứng (Khóa liên kết sang `notice`).",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "bid_id / package_no", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.bidId",
                    "$.payload.bideContractorInputResultDTO.bidNo",
                ),
                "clean_text(root.bidId), clean_text(root.bidNo)",
                "UUID và mã gói thầu (Khóa liên kết sang `package`).",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "business_number", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.notifyNo",
                    "$.payload.bideContractorInputResultDTO.bidNo",
                ),
                "clean_text(root.notifyNo) ?: clean_text(root.bidNo)",
                "Mã nghiệp vụ chính của KQLCNT.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "title", "VARCHAR",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.bidName",),
                "clean_text(root.bidName)",
                "Tên gói thầu được phê duyệt kết quả.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "procuring_entity_code / buyer_id", "VARCHAR",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.procuringEntityCode",),
                "clean_text(root.procuringEntityCode)",
                "Mã Bên mời thầu.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "procuring_entity_name / buyer_name", "VARCHAR",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.procuringEntityName",),
                "clean_text(root.procuringEntityName)",
                "Tên Bên mời thầu.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "investor_code / investor_name", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.investorCode",
                    "$.payload.bideContractorInputResultDTO.investorName",
                ),
                "clean_text(root.investorCode), clean_text(root.investorName)",
                "Mã và tên Chủ đầu tư.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "decision_no / decision_agency", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.decisionNo",
                    "$.payload.bideContractorInputResultDTO.decisionAgency",
                ),
                "clean_text(root.decisionNo), clean_text(root.decisionAgency)",
                "Số quyết định và cơ quan phê duyệt KQLCNT.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "decision_date", "DATE",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.decisionDate",),
                "parse_date(root.decisionDate); kiểm tra decisionDate <= publicDate",
                "Ngày ký quyết định phê duyệt KQLCNT.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "total_winning_price / amount", "DECIMAL(38,6)",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotFinalPrice",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotPrice",
                ),
                "SUM(bidWiningPrice ?: lotFinalPrice ?: lotPrice) của các nhà thầu có `bidResult == 1`",
                "Tổng giá trúng thầu của toàn bộ gói thầu.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "currency", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.bidPriceUnit",
                    "$.payload.bideContractorInputResultDTO.bidEstimatePriceUnit",
                ),
                "clean_text(bidPriceUnit ?: bidEstimatePriceUnit) ?: 'VND'",
                "Đơn vị tiền tệ của kết quả lựa chọn nhà thầu.",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "public_date / public_date_raw", "DATE / VARCHAR",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.publicDate",),
                "parse_date(root.publicDate) & clean_text(root.publicDate)",
                "Ngày công khai KQLCNT (Khóa phân vùng Iceberg).",
            ),
            ColumnMappingSpec(
                "silver.result_revision", "status_raw", "VARCHAR",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.status",),
                "clean_text(root.status)",
                "Trạng thái gốc của hồ sơ KQLCNT.",
            ),
            # Child 1: award_lot
            ColumnMappingSpec(
                "silver.award_lot", "award_lot_id", "VARCHAR (PK)",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.lotResultDTO[].id",),
                "SHA-256([revision_id, 'award_lot', ordinal, lot_id ?: lot_no])",
                "Khóa chính của phần/lô trúng thầu.",
            ),
            ColumnMappingSpec(
                "silver.award_lot", "lot_no / lot_name", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].lotNo",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].lotName",
                ),
                "clean_text(lotNo ?: bidNo ?: ordinal+1), clean_text(lotName ?: bidName)",
                "Mã lô và tên lô được phê duyệt kết quả.",
            ),
            ColumnMappingSpec(
                "silver.award_lot", "lot_price", "DECIMAL(38,6)",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.lotResultDTO[].lotPrice",),
                "parse_money(lotPrice); chặn giá <= 0 (`invalid_zero_price`) và giá giả <= 10 VND (`dummy_placeholder_price`)",
                "Giá gói thầu / dự toán của lô (đã lọc nhiễu giá 1đ, 10đ).",
            ),
            ColumnMappingSpec(
                "silver.award_lot", "winning_price", "DECIMAL(38,6)",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice",
                ),
                "Tổng giá trúng thầu của các nhà thầu thắng cuộc (`bidResult == 1`) trong lô; cảnh báo `price_scale_anomaly` nếu lệch > 100x",
                "Giá trúng thầu thực tế của lô.",
            ),
            ColumnMappingSpec(
                "silver.award_lot", "winning_code / status", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].winningCode",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].status",
                ),
                "clean_text(lot.winningCode), clean_text(str(lot.status))",
                "Mã trạng thái trao thầu và trạng thái lô.",
            ),
            # Child 2: award_contractor
            ColumnMappingSpec(
                "silver.award_contractor", "award_contractor_id / award_lot_id", "VARCHAR (PK / FK)",
                "contractor_result_detail", ("$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].id",),
                "SHA-256([revision_id, 'award_contractor', lot_ordinal, c_idx, id ?: orgCode])",
                "Khóa chính dòng nhà thầu và khóa ngoại trỏ về `silver.award_lot`.",
            ),
            ColumnMappingSpec(
                "silver.award_contractor", "contractor_code", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgCode",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode",
                ),
                "clean_text(orgCode ?: taxCode) ?: 'UNKNOWN'",
                "Mã định danh nhà thầu (DKKD / e-GP code).",
            ),
            ColumnMappingSpec(
                "silver.award_contractor", "contractor_name", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgFullname",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].newContractorName",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].contractorName",
                ),
                "clean_text(orgFullname ?: newContractorName ?: contractorName) ?: 'UNKNOWN'",
                "Tên pháp nhân đầy đủ của nhà thầu.",
            ),
            ColumnMappingSpec(
                "silver.award_contractor", "tax_code / country", "VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxNation",
                ),
                "clean_text(taxCode), clean_text(taxNation) ?: 'VN'",
                "Mã số thuế và quốc gia đăng ký của nhà thầu.",
            ),
            ColumnMappingSpec(
                "silver.award_contractor", "is_consortium / venture_code / venture_name / role", "BOOLEAN / VARCHAR",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureCode",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureName",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].role",
                ),
                "is_consortium = bool(ventureCode or ventureName); clean_text cho mã, tên và vai trò liên danh",
                "Thông tin nhà thầu độc lập hay thành viên liên danh.",
            ),
            ColumnMappingSpec(
                "silver.award_contractor", "awarded_price", "DECIMAL(38,6)",
                "contractor_result_detail", (
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotFinalPrice",
                    "$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotPrice",
                ),
                "parse_money(bidWiningPrice ?: lotFinalPrice ?: lotPrice)",
                "Giá trúng thầu / giá dự thầu sau đánh giá của nhà thầu.",
            ),
        ),
    ),
)


def lookup_empirical_coverage(
    profiles: dict[str, Any],
    bronze_table: str,
    paths: tuple[str, ...],
) -> str:
    """Resolve empirical Populated % and Null % from profiled Bronze tables."""
    profile = profiles.get(bronze_table)
    if not profile or not getattr(profile, "fields", None):
        return "*(Chưa có mẫu)*"

    matched_stats: list[str] = []
    for p in paths:
        f = profile.fields.get(p)
        if f is not None:
            short_p = p.replace("$.payload.", "")
            matched_stats.append(f"`{short_p}`: **{f.populated_ratio * 100:.1f}%** (Null {f.null_ratio * 100:.1f}%)")

    if not matched_stats:
        return "Derived / Mảng động"
    return "<br>".join(matched_stats)
