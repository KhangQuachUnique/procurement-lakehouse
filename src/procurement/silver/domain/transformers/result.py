"""Transformer for Contractor Selection Result (table: contractor_result_detail)."""

from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Any

from procurement.silver.domain.parsers import (
    MAX_MONEY,
    clean_text,
    lookup_path,
    parse_date,
    parse_money,
)
from procurement.silver.domain.protocols import (
    ExtractedChild,
    ExtractedReference,
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


@dataclass(frozen=True)
class _ParsedContractor:
    ordinal: int
    c_idx: int
    c_id: str | None
    contractor_code: str
    contractor_name: str
    tax_code: str | None
    country: str
    is_consortium: bool
    venture_code: str | None
    venture_name: str | None
    role: str | None
    is_winner: bool
    raw_bid_result: Any
    price_dec: Decimal | None
    price_str: str | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class _ParsedLot:
    ordinal: int
    lot_id: str | None
    lot_no: str
    lot_name: str | None
    lot_price_str: str | None
    lot_currency: str | None
    winning_code: str | None
    lot_status: str | None
    contractors: list[_ParsedContractor]
    winning_price_dec: Decimal | None
    winning_price_str: str | None
    has_incomplete_winner_price: bool
    has_consortium_ambiguity: bool
    has_currency_conflict: bool
    raw_payload: dict[str, Any]
    is_unresolved: bool = False
    has_unresolved_outcome: bool = False
    has_malformed_entry: bool = False

    @property
    def unresolved(self) -> bool:
        return self.is_unresolved


def _sum_decimals_exact(values: Any) -> tuple[Decimal | None, bool]:
    """Sum decimals using localcontext(prec=50) and detect overflow for DECIMAL(38,6).

    Returns:
        (total_decimal, has_overflow)
    """
    valid_values = [v for v in values if v is not None]
    if not valid_values:
        return Decimal(0), False
    with localcontext() as ctx:
        ctx.prec = 50
        total = Decimal(0)
        for v in valid_values:
            total += v
        if abs(total) >= MAX_MONEY:
            return None, True
        return total, False


def _add_issue(issues: list[QualityIssue] | None, issue: QualityIssue) -> None:
    if issues is None:
        return
    if any(i.issue_id == issue.issue_id for i in issues):
        return
    issues.append(issue)


def _normalize_outcome_code(value: Any) -> str | None:
    """Strictly normalize outcome code: int (excluding bool) or stripped string."""
    if value is None or isinstance(value, (bool, float)):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else None
    return None


def _clean_currency_value(value: Any) -> tuple[str | None, bool]:
    """Clean currency value, flagging boolean/float/integer as invalid types."""
    if value is None:
        return None, False
    if isinstance(value, (bool, int, float)):
        return None, True
    txt = clean_text(str(value))
    return (txt if txt else None), False


@register_transformer("contractor_result_detail")
class ContractorResultTransformer(BaseResourceTransformer):
    """Transformer for procurement contractor award results (Kết quả lựa chọn nhà thầu)."""

    @property
    def table_name(self) -> str:
        return "contractor_result_detail"

    @property
    def entity_type(self) -> str:
        return "result"

    @property
    def id_scheme(self) -> str:
        return "uuid"

    def extract_root(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Extract root dictionary from payload (bideContractorInputResultDTO)."""
        root = lookup_path(payload, "bideContractorInputResultDTO")
        if isinstance(root, dict):
            return root
        if isinstance(payload, dict) and "id" in payload:
            return payload
        return None

    def extract_source_identity(self, root: dict[str, Any]) -> str | None:
        """Primary key of result is 'id' inside bideContractorInputResultDTO."""
        return clean_text(root.get("id"))

    def _parse_result_structure(
        self,
        root: dict[str, Any],
        obs_id: str | None = None,
        issues: list[QualityIssue] | None = None,
    ) -> tuple[str | None, list[_ParsedLot], Decimal | None, str | None]:
        # 1. Resolve Root Currency (check all candidate fields, never silently conflict)
        root_units: dict[str, str] = {}
        for fld in ("bidPriceUnit", "bidEstimatePriceUnit"):
            val, is_inv = _clean_currency_value(root.get(fld))
            if is_inv and obs_id:
                _add_issue(
                    issues,
                    QualityIssue(
                        issue_id=digest([obs_id, "unknown_currency", fld]),
                        observation_id=obs_id,
                        code="unknown_currency",
                        path=f"bideContractorInputResultDTO.{fld}",
                        severity="warn",
                    ),
                )
            if val is not None:
                root_units[fld] = val

        distinct_root = set(root_units.values())
        root_currency: str | None = None
        root_currency_conflict = False
        if len(distinct_root) > 1:
            root_currency = None
            root_currency_conflict = True
            if obs_id:
                _add_issue(
                    issues,
                    QualityIssue(
                        issue_id=digest([obs_id, "conflicting_currency", "bideContractorInputResultDTO"]),
                        observation_id=obs_id,
                        code="conflicting_currency",
                        path="bideContractorInputResultDTO",
                        severity="warn",
                    ),
                )
        elif len(distinct_root) == 1:
            root_currency = next(iter(distinct_root))

        lots = root.get("lotResultDTO")
        root_has_malformed_entry = False
        if lots is not None and not isinstance(lots, list):
            root_has_malformed_entry = True
            if obs_id:
                _add_issue(
                    issues,
                    QualityIssue(
                        issue_id=digest([obs_id, "malformed_entry", "lotResultDTO"]),
                        observation_id=obs_id,
                        code="malformed_entry",
                        path="bideContractorInputResultDTO.lotResultDTO",
                        severity="warn",
                    ),
                )
            return root_currency, [], None, None
        if not isinstance(lots, list):
            return root_currency, [], None, None

        parsed_lots: list[_ParsedLot] = []
        for ordinal, lot in enumerate(lots):
            if not isinstance(lot, dict):
                root_has_malformed_entry = True
                if obs_id:
                    _add_issue(
                        issues,
                        QualityIssue(
                            issue_id=digest([obs_id, "malformed_entry", f"lotResultDTO[{ordinal}]"]),
                            observation_id=obs_id,
                            code="malformed_entry",
                            path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}]",
                            severity="warn",
                        ),
                    )
                continue

            lot_id = clean_text(lot.get("id"))
            lot_no = clean_text(lot.get("lotNo") or lot.get("bidNo")) or str(ordinal + 1)
            lot_name = clean_text(lot.get("lotName") or lot.get("bidName"))

            # 2. Resolve ALL explicit lot currency fields, never or-chain hiding conflicts
            lot_units: dict[str, str] = {}
            for lot_fld in ("lotPriceUnit", "currency", "bidEstimatePriceUnit"):
                val, is_inv = _clean_currency_value(lot.get(lot_fld))
                if is_inv and obs_id:
                    _add_issue(
                        issues,
                        QualityIssue(
                            issue_id=digest([obs_id, "unknown_currency", f"lot[{ordinal}].{lot_fld}"]),
                            observation_id=obs_id,
                            code="unknown_currency",
                            path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].{lot_fld}",
                            severity="warn",
                        ),
                    )
                if val is not None:
                    lot_units[lot_fld] = val

            distinct_lot = set(lot_units.values())
            lot_has_currency_conflict = False
            lot_currency: str | None = None

            if len(distinct_lot) > 1:
                # Conflicting currencies declared within the lot itself
                lot_has_currency_conflict = True
                lot_currency = None
                if obs_id:
                    _add_issue(
                        issues,
                        QualityIssue(
                            issue_id=digest([obs_id, "conflicting_currency", f"lot[{ordinal}].explicit"]),
                            observation_id=obs_id,
                            code="conflicting_currency",
                            path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}]",
                            severity="warn",
                        ),
                    )
            elif len(distinct_lot) == 1:
                explicit_lot_curr = next(iter(distinct_lot))
                if root_currency is not None and explicit_lot_curr != root_currency:
                    # Explicit lot currency conflicts with root currency
                    lot_has_currency_conflict = True
                    lot_currency = None
                    if obs_id:
                        _add_issue(
                            issues,
                            QualityIssue(
                                issue_id=digest([obs_id, "conflicting_currency", f"lot[{ordinal}].vs_root"]),
                                observation_id=obs_id,
                                code="conflicting_currency",
                                path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}]",
                                severity="warn",
                            ),
                        )
                else:
                    # Valid explicit lot currency (can salvage lot despite absent root)
                    lot_currency = explicit_lot_curr
            else:
                # No explicit lot currency -> inherit root currency
                lot_currency = root_currency

            # Validate lot budget price
            lot_price_str = None
            raw_lot_price = lot.get("lotPrice")
            if raw_lot_price is not None and str(raw_lot_price).strip() != "":
                try:
                    parsed_lot_p = parse_money(raw_lot_price)
                    if parsed_lot_p is not None:
                        dec_lot_p = Decimal(parsed_lot_p)
                        if dec_lot_p <= Decimal(0) and obs_id:
                            _add_issue(
                                issues,
                                QualityIssue(
                                    issue_id=digest([obs_id, "invalid_zero_price", f"lot[{ordinal}].lotPrice"]),
                                    observation_id=obs_id,
                                    code="invalid_zero_price",
                                    path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].lotPrice",
                                    severity="warn",
                                ),
                            )
                        elif dec_lot_p <= Decimal(10) and lot_currency in ("VND", None) and obs_id:
                            _add_issue(
                                issues,
                                QualityIssue(
                                    issue_id=digest([obs_id, "dummy_placeholder_price", f"lot[{ordinal}].lotPrice"]),
                                    observation_id=obs_id,
                                    code="dummy_placeholder_price",
                                    path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].lotPrice",
                                    severity="warn",
                                ),
                            )
                        else:
                            lot_price_str = parsed_lot_p
                except (ValueError, ArithmeticError):
                    if obs_id:
                        _add_issue(
                            issues,
                            QualityIssue(
                                issue_id=digest([obs_id, "invalid_money", f"lot[{ordinal}].lotPrice"]),
                                observation_id=obs_id,
                                code="invalid_money",
                                path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].lotPrice",
                                severity="warn",
                            ),
                        )

            # 3. Process Contractors in Lot
            contractors = lot.get("contractorList")
            parsed_contractors: list[_ParsedContractor] = []
            lot_has_unresolved_outcome = False
            lot_has_malformed_entry = False

            if contractors is not None and not isinstance(contractors, list):
                lot_has_malformed_entry = True
                if obs_id:
                    _add_issue(
                        issues,
                        QualityIssue(
                            issue_id=digest([obs_id, "malformed_entry", f"lot[{ordinal}].contractorList"]),
                            observation_id=obs_id,
                            code="malformed_entry",
                            path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].contractorList",
                            severity="warn",
                        ),
                    )

            if isinstance(contractors, list):
                for c_idx, c in enumerate(contractors):
                    if not isinstance(c, dict):
                        lot_has_malformed_entry = True
                        if obs_id:
                            _add_issue(
                                issues,
                                QualityIssue(
                                    issue_id=digest([obs_id, "malformed_entry", f"lot[{ordinal}].contractorList[{c_idx}]"]),
                                    observation_id=obs_id,
                                    code="malformed_entry",
                                    path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].contractorList[{c_idx}]",
                                    severity="warn",
                                ),
                            )
                        continue

                    c_id = clean_text(c.get("id"))
                    contractor_code = clean_text(c.get("orgCode") or c.get("taxCode")) or "UNKNOWN"
                    contractor_name = (
                        clean_text(
                            c.get("orgFullname")
                            or c.get("newContractorName")
                            or c.get("contractorName")
                        )
                        or "UNKNOWN"
                    )
                    tax_code = clean_text(c.get("taxCode"))
                    country = clean_text(c.get("taxNation")) or "VN"
                    venture_code = clean_text(c.get("ventureCode"))
                    venture_name = clean_text(c.get("ventureName"))
                    is_consortium = bool(
                        venture_code
                        or venture_name
                        or c.get("isConsortium")
                        or c.get("is_consortium")
                    )
                    role = clean_text(c.get("role"))

                    raw_bid_result = c.get("bidResult")
                    norm_outcome = _normalize_outcome_code(raw_bid_result)

                    is_missing_outcome = (raw_bid_result is None) or (
                        isinstance(raw_bid_result, str) and not raw_bid_result.strip()
                    )
                    is_unsupported_outcome = (
                        isinstance(raw_bid_result, (bool, float))
                        or (norm_outcome is not None and norm_outcome not in ("0", "1", "2", "3"))
                        or (not is_missing_outcome and norm_outcome is None)
                    )

                    if is_missing_outcome:
                        lot_has_unresolved_outcome = True
                        if obs_id:
                            _add_issue(
                                issues,
                                QualityIssue(
                                    issue_id=digest([obs_id, "missing_outcome", f"lot[{ordinal}].contractorList[{c_idx}]"]),
                                    observation_id=obs_id,
                                    code="missing_outcome",
                                    path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].contractorList[{c_idx}].bidResult",
                                    severity="warn",
                                ),
                            )
                    elif is_unsupported_outcome:
                        lot_has_unresolved_outcome = True
                        if obs_id:
                            _add_issue(
                                issues,
                                QualityIssue(
                                    issue_id=digest([obs_id, "unsupported_outcome", f"lot[{ordinal}].contractorList[{c_idx}]"]),
                                    observation_id=obs_id,
                                    code="unsupported_outcome",
                                    path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].contractorList[{c_idx}].bidResult",
                                    severity="warn",
                                ),
                            )

                    # Only confirmed winners: exact string "1" from strict normalization
                    is_winner = (norm_outcome == "1") and not isinstance(raw_bid_result, (bool, float))

                    # Winning price: ONLY bidWiningPrice
                    raw_win_price = c.get("bidWiningPrice")
                    c_price_dec: Decimal | None = None
                    c_price_str: str | None = None
                    if raw_win_price is not None and str(raw_win_price).strip() != "":
                        try:
                            parsed_win = parse_money(raw_win_price)
                            if parsed_win is not None:
                                dec_val = Decimal(parsed_win)
                                if dec_val < Decimal(0) and obs_id:
                                    _add_issue(
                                        issues,
                                        QualityIssue(
                                            issue_id=digest([obs_id, "invalid_money", f"lot[{ordinal}].contractor[{c_idx}].bidWiningPrice"]),
                                            observation_id=obs_id,
                                            code="invalid_money",
                                            path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].contractorList[{c_idx}].bidWiningPrice",
                                            severity="warn",
                                        ),
                                    )
                                else:
                                    c_price_dec = dec_val
                                    c_price_str = parsed_win
                        except (ValueError, ArithmeticError):
                            if obs_id:
                                _add_issue(
                                    issues,
                                    QualityIssue(
                                        issue_id=digest([obs_id, "invalid_money", f"lot[{ordinal}].contractor[{c_idx}].bidWiningPrice"]),
                                        observation_id=obs_id,
                                        code="invalid_money",
                                        path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].contractorList[{c_idx}].bidWiningPrice",
                                        severity="warn",
                                    ),
                                )

                    if (
                        not is_winner
                        and c_price_dec is not None
                        and c_price_dec > Decimal(0)
                        and obs_id
                    ):
                        _add_issue(
                            issues,
                            QualityIssue(
                                issue_id=digest([obs_id, "non_winner_with_winning_price", f"lot[{ordinal}].contractorList[{c_idx}]"]),
                                observation_id=obs_id,
                                code="non_winner_with_winning_price",
                                path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].contractorList[{c_idx}].bidWiningPrice",
                                severity="warn",
                            ),
                        )

                    parsed_contractors.append(
                        _ParsedContractor(
                            ordinal=ordinal,
                            c_idx=c_idx,
                            c_id=c_id,
                            contractor_code=contractor_code,
                            contractor_name=contractor_name,
                            tax_code=tax_code,
                            country=country,
                            is_consortium=is_consortium,
                            venture_code=venture_code,
                            venture_name=venture_name,
                            role=role,
                            is_winner=is_winner,
                            raw_bid_result=raw_bid_result,
                            price_dec=c_price_dec,
                            price_str=c_price_str,
                            raw_payload=c,
                        )
                    )

            confirmed_winners = [c for c in parsed_contractors if c.is_winner]
            has_incomplete_winner_price = False
            has_consortium_ambiguity = False
            has_unverified_zero_price = False
            lot_overflow = False
            lot_winning_price_dec: Decimal | None = None
            lot_winning_price_str: str | None = None

            if len(confirmed_winners) > 0:
                # 4. Check for incomplete or unverified zero winner prices
                for w in confirmed_winners:
                    if w.price_dec is None:
                        has_incomplete_winner_price = True
                        if obs_id:
                            _add_issue(
                                issues,
                                QualityIssue(
                                    issue_id=digest([obs_id, "incomplete_winner_price", f"lot[{ordinal}]"]),
                                    observation_id=obs_id,
                                    code="incomplete_winner_price",
                                    path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}]",
                                    severity="warn",
                                ),
                            )
                    elif w.price_dec == Decimal(0) or (
                        w.price_dec <= Decimal(10) and lot_currency in ("VND", None)
                    ):
                        has_unverified_zero_price = True
                        if obs_id:
                            _add_issue(
                                issues,
                                QualityIssue(
                                    issue_id=digest([obs_id, "unverified_zero_winning_price", f"lot[{ordinal}].c[{w.c_idx}]"]),
                                    observation_id=obs_id,
                                    code="unverified_zero_winning_price",
                                    path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].contractorList[{w.c_idx}].bidWiningPrice",
                                    severity="warn",
                                ),
                            )

                consortium_members = [w for w in confirmed_winners if w.is_consortium]
                if len(consortium_members) > 1:
                    has_consortium_ambiguity = True
                    if obs_id:
                        _add_issue(
                            issues,
                            QualityIssue(
                                issue_id=digest([obs_id, "unverified_consortium_allocation", f"lot[{ordinal}]"]),
                                observation_id=obs_id,
                                code="unverified_consortium_allocation",
                                path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}]",
                                severity="warn",
                            ),
                        )

                lot_overflow = False
                # Derived lot total requires completeness, verified prices, resolved outcome, and known currency
                if (
                    not has_incomplete_winner_price
                    and not has_unverified_zero_price
                    and not has_consortium_ambiguity
                    and not lot_has_currency_conflict
                    and not lot_has_unresolved_outcome
                    and not lot_has_malformed_entry
                    and lot_currency is not None
                ):
                    lot_sum, lot_overflow = _sum_decimals_exact(w.price_dec for w in confirmed_winners)
                    if lot_overflow:
                        lot_winning_price_dec = None
                        lot_winning_price_str = None
                        if obs_id:
                            _add_issue(
                                issues,
                                QualityIssue(
                                    issue_id=digest([obs_id, "invalid_money", f"lot[{ordinal}].overflow"]),
                                    observation_id=obs_id,
                                    code="invalid_money",
                                    path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}]",
                                    severity="warn",
                                ),
                            )
                    else:
                        lot_winning_price_dec = lot_sum
                        lot_winning_price_str = f"{lot_winning_price_dec:.6f}"

            lot_is_unresolved = (
                lot_has_unresolved_outcome
                or lot_has_malformed_entry
                or has_incomplete_winner_price
                or has_unverified_zero_price
                or has_consortium_ambiguity
                or lot_has_currency_conflict
                or lot_overflow
                or (len(confirmed_winners) > 0 and lot_currency is None)
            )

            if lot_currency is None and (lot_price_str is not None or lot_winning_price_str is not None or len(confirmed_winners) > 0) and obs_id:
                _add_issue(
                    issues,
                    QualityIssue(
                        issue_id=digest([obs_id, "unknown_currency", f"lot[{ordinal}]"]),
                        observation_id=obs_id,
                        code="unknown_currency",
                        path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}]",
                        severity="warn",
                    ),
                )

            if (
                lot_price_str is not None
                and lot_winning_price_dec is not None
                and Decimal(lot_price_str) * Decimal(100) < lot_winning_price_dec
                and obs_id
            ):
                _add_issue(
                    issues,
                    QualityIssue(
                        issue_id=digest([obs_id, "price_scale_anomaly", f"lot[{ordinal}].lotPrice"]),
                        observation_id=obs_id,
                        code="price_scale_anomaly",
                        path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].lotPrice",
                        severity="warn",
                    ),
                )

            winning_code = clean_text(lot.get("winningCode"))
            lot_status = clean_text(str(lot.get("status"))) if lot.get("status") is not None else None

            parsed_lots.append(
                _ParsedLot(
                    ordinal=ordinal,
                    lot_id=lot_id,
                    lot_no=lot_no,
                    lot_name=lot_name,
                    lot_price_str=lot_price_str,
                    lot_currency=lot_currency,
                    winning_code=winning_code,
                    lot_status=lot_status,
                    contractors=parsed_contractors,
                    winning_price_dec=lot_winning_price_dec,
                    winning_price_str=lot_winning_price_str,
                    has_incomplete_winner_price=has_incomplete_winner_price,
                    has_consortium_ambiguity=has_consortium_ambiguity,
                    has_currency_conflict=lot_has_currency_conflict,
                    raw_payload=lot,
                    is_unresolved=lot_is_unresolved,
                    has_unresolved_outcome=lot_has_unresolved_outcome,
                    has_malformed_entry=lot_has_malformed_entry,
                )
            )

        lots_with_winners = [l for l in parsed_lots if any(c.is_winner for c in l.contractors)]
        root_winning_price_dec: Decimal | None = None
        root_winning_price_str: str | None = None

        if (
            root_has_malformed_entry
            or any(l.is_unresolved for l in parsed_lots)
            or not lots_with_winners
            or any(l.winning_price_dec is None for l in lots_with_winners)
        ):
            root_winning_price_dec = None
            root_winning_price_str = None
        else:
            winner_currencies = {l.lot_currency for l in lots_with_winners}
            if len(winner_currencies) > 1 or root_currency_conflict or None in winner_currencies:
                root_winning_price_dec = None
                root_winning_price_str = None
            else:
                root_sum, root_overflow = _sum_decimals_exact(l.winning_price_dec for l in lots_with_winners)
                if root_overflow:
                    root_winning_price_dec = None
                    root_winning_price_str = None
                    if obs_id:
                        _add_issue(
                            issues,
                            QualityIssue(
                                issue_id=digest([obs_id, "invalid_money", "bideContractorInputResultDTO.root_overflow"]),
                                observation_id=obs_id,
                                code="invalid_money",
                                path="bideContractorInputResultDTO",
                                severity="warn",
                            ),
                        )
                else:
                    root_winning_price_dec = root_sum
                    root_winning_price_str = f"{root_winning_price_dec:.6f}"
                    if root_currency is None and winner_currencies:
                        root_currency = next(iter(winner_currencies))

        if root_currency is None and (root_winning_price_str is not None or lots_with_winners) and obs_id:
            _add_issue(
                issues,
                QualityIssue(
                    issue_id=digest([obs_id, "unknown_currency", "bideContractorInputResultDTO"]),
                    observation_id=obs_id,
                    code="unknown_currency",
                    path="bideContractorInputResultDTO",
                    severity="warn",
                ),
            )

        return root_currency, parsed_lots, root_winning_price_dec, root_winning_price_str

    def extract_attributes(
        self,
        root: dict[str, Any],
        revision_id: str,
        entity_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> dict[str, Any]:
        """Normalize domain attributes for contractor selection result."""
        notify_no = clean_text(root.get("notifyNo"))
        package_no = clean_text(root.get("bidNo"))
        business_number = notify_no or package_no

        root_curr, _parsed_lots, _root_win_dec, root_win_str = self._parse_result_structure(
            root, obs_id=obs_id, issues=issues
        )

        dec_date = parse_date(root.get("decisionDate"))
        pub_date = parse_date(root.get("publicDate"))
        if dec_date and pub_date and dec_date > pub_date:
            _add_issue(
                issues,
                QualityIssue(
                    issue_id=digest([obs_id, "invalid_date_order", "decisionDate"]),
                    observation_id=obs_id,
                    code="invalid_date_order",
                    path="bideContractorInputResultDTO.decisionDate",
                    severity="warn",
                ),
            )

        return {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            "result_id": clean_text(root.get("id")),
            "result_version": clean_text(root.get("resultVersion")),
            "notify_no": notify_no,
            "notify_version": clean_text(root.get("notifyVersion")),
            "business_number": business_number,
            "bid_id": clean_text(root.get("bidId")),
            "package_no": package_no,
            "title": clean_text(root.get("bidName")),
            "procuring_entity_code": clean_text(root.get("procuringEntityCode")),
            "procuring_entity_name": clean_text(root.get("procuringEntityName")),
            "buyer_id": clean_text(root.get("procuringEntityCode")),
            "buyer_name": clean_text(root.get("procuringEntityName")),
            "investor_code": clean_text(root.get("investorCode")),
            "investor_name": clean_text(root.get("investorName")),
            "decision_no": clean_text(root.get("decisionNo")),
            "decision_date": dec_date,
            "decision_agency": clean_text(root.get("decisionAgency")),
            "total_winning_price": root_win_str,
            "amount": root_win_str,
            "currency": root_curr,
            "public_date": pub_date,
            "public_date_raw": clean_text(root.get("publicDate")),
            "status_raw": clean_text(root.get("status")),
            "root_json": canonical_json(root),
        }

    def transform(self, envelope: RawRecordEnvelope) -> TransformedRecord:
        """Override transform to provide explicit source_version fallback from resultVersion."""
        from dataclasses import replace

        root = self.extract_root(envelope.payload)
        version = None
        if isinstance(root, dict):
            version = clean_text(root.get("resultVersion"))

        target_envelope = envelope
        if envelope.source_version is None and version:
            target_envelope = replace(envelope, source_version=version)

        result = super().transform(target_envelope)
        result.observation = self.build_observation(envelope)[1]
        return result

    def extract_references(
        self,
        root: dict[str, Any],
        revision_id: str,
    ) -> list[ExtractedReference]:
        """Extract links to notice (via notifyNo), package (via bidId), and plan (via planNo)."""
        refs = []
        notify_no = clean_text(root.get("notifyNo"))
        if notify_no:
            refs.append(
                ExtractedReference(
                    digest([revision_id, "notice", notify_no]),
                    revision_id,
                    "notice",
                    "notify_no",
                    notify_no,
                    "notifyNo",
                )
            )
        bid_id = clean_text(root.get("bidId"))
        if bid_id:
            refs.append(
                ExtractedReference(
                    digest([revision_id, "package", bid_id]),
                    revision_id,
                    "package",
                    "uuid",
                    bid_id,
                    "bidId",
                )
            )
        plan_no = clean_text(root.get("planNo"))
        if plan_no:
            refs.append(
                ExtractedReference(
                    digest([revision_id, "plan", plan_no]),
                    revision_id,
                    "plan",
                    "number",
                    plan_no,
                    "planNo",
                )
            )
        return refs

    def extract_children(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> list[ExtractedChild]:
        """Extract award_lot and award_contractor sub-entities."""
        children: list[ExtractedChild] = []
        root = payload.get("bideContractorInputResultDTO")
        if not isinstance(root, dict):
            root = payload if isinstance(payload, dict) and "id" in payload else {}

        _root_curr, parsed_lots, _root_win_dec, _root_win_str = self._parse_result_structure(
            root, obs_id=obs_id, issues=issues
        )

        for lot in parsed_lots:
            award_lot_child_id = digest([revision_id, "award_lot", lot.ordinal, lot.lot_id or lot.lot_no])

            for c in lot.contractors:
                c_child_id = digest(
                    [
                        revision_id,
                        "award_contractor",
                        lot.ordinal,
                        c.c_idx,
                        c.c_id or c.contractor_code,
                    ]
                )

                if c.is_winner:
                    contractor_attrs = {
                        "award_contractor_id": c_child_id,
                        "award_lot_id": award_lot_child_id,
                        "revision_id": revision_id,
                        "contractor_code": c.contractor_code,
                        "contractor_name": c.contractor_name,
                        "tax_code": c.tax_code,
                        "country": c.country,
                        "is_consortium": c.is_consortium,
                        "venture_code": c.venture_code,
                        "venture_name": c.venture_name,
                        "role": c.role,
                        "awarded_price": c.price_str,
                        "evaluation_rank": None,
                        "bid_result": 1,
                        "price_source": "bidWiningPrice" if c.price_str is not None else None,
                        "currency": lot.lot_currency,
                    }
                    children.append(
                        ExtractedChild(
                            child_id=c_child_id,
                            revision_id=revision_id,
                            kind="award_contractor",
                            role="contractorList[]",
                            path=f"lotResultDTO[{lot.ordinal}].contractorList[{c.c_idx}]",
                            ordinal=c.c_idx,
                            source_id=c.c_id or c.contractor_code,
                            payload_json=canonical_json(c.raw_payload),
                            typed_table="award_contractor",
                            typed_attributes=contractor_attrs,
                        )
                    )
                else:
                    # Non-winner retained in payload_json without typed award_contractor row
                    children.append(
                        ExtractedChild(
                            child_id=c_child_id,
                            revision_id=revision_id,
                            kind="award_contractor",
                            role="contractorList[]",
                            path=f"lotResultDTO[{lot.ordinal}].contractorList[{c.c_idx}]",
                            ordinal=c.c_idx,
                            source_id=c.c_id or c.contractor_code,
                            payload_json=canonical_json(c.raw_payload),
                            typed_table=None,
                            typed_attributes=None,
                        )
                    )

            lot_attrs = {
                "award_lot_id": award_lot_child_id,
                "revision_id": revision_id,
                "lot_no": lot.lot_no,
                "lot_name": lot.lot_name,
                "lot_price": lot.lot_price_str,
                "winning_price": lot.winning_price_str,
                "currency": lot.lot_currency,
                "winning_code": lot.winning_code,
                "status": lot.lot_status,
                "raw_json": canonical_json(lot.raw_payload),
            }
            children.append(
                ExtractedChild(
                    child_id=award_lot_child_id,
                    revision_id=revision_id,
                    kind="award_lot",
                    role="lotResultDTO[]",
                    path=f"lotResultDTO[{lot.ordinal}]",
                    ordinal=lot.ordinal,
                    source_id=lot.lot_id or lot.lot_no,
                    payload_json=canonical_json(lot.raw_payload),
                    typed_table="award_lot",
                    typed_attributes=lot_attrs,
                )
            )

        return children
