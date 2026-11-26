"""Prepare and verify DAX-vs-SQL checks for new presentation measures, dynamic insights, matrix, and rank."""
from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

from profile_sources import PROJECT, WORKSPACE, connection, discover, save


def build_queries() -> tuple[list[dict], list[dict]]:
    con = connection(discover(WORKSPACE / 'PowerBI/export/parquet'))
    queries: list[dict] = []
    expected: list[dict] = []

    def add_num(name: str, dax: str, sql: str | None = None, expected_val=None, tol: str = '1e-6') -> None:
        val = con.execute(sql).fetchone()[0] if sql is not None else expected_val
        queries.append({'name': name, 'dax': 'EVALUATE ROW("value", ' + dax + ')'})
        expected.append({'name': name, 'kind': 'numeric', 'value': None if val is None else str(val), 'tolerance': tol})

    def add_str(name: str, dax: str, contains: list[str] | None = None, exact: str | None = None) -> None:
        queries.append({'name': name, 'dax': 'EVALUATE ROW("value", ' + dax + ')'})
        expected.append({'name': name, 'kind': 'string', 'exact': exact, 'contains': contains or []})

    # 1. Scale measures (nghìn tỷ VND)
    scopes = [
        ('all', 'TRUE', []),
        ('year2025', 'year(report_date)=2025', ['dim_date[year]=2025']),
        ('jan2025_hh', "report_date between date '2025-01-01' and date '2025-01-31' AND sector_key='SEC_HH'", ['dim_date[year_month]="2025-01"', 'dim_sector[sector_key]="SEC_HH"']),
        ('empty_month', "report_date between date '2022-01-01' and date '2022-01-31'", ['dim_date[year_month]="2022-01"']),
    ]
    for sc, where, filters in scopes:
        fstr = (', ' + ', '.join(filters)) if filters else ''
        add_num(
            f'{sc}/planned_nt',
            f'CALCULATE([Kế hoạch · nghìn tỷ VND]{fstr})' if filters else '[Kế hoạch · nghìn tỷ VND]',
            f"SELECT sum(planned_amount_sum)/1e12 FROM fact_demand_daily WHERE currency_key='VND' AND ({where})",
            tol='1e-6',
        )
        add_num(
            f'{sc}/award_nt',
            f'CALCULATE([Trúng thầu · nghìn tỷ VND]{fstr})' if filters else '[Trúng thầu · nghìn tỷ VND]',
            f"SELECT sum(award_amount_sum)/1e12 FROM fact_award_daily WHERE currency_key='VND' AND ({where})",
            tol='1e-6',
        )
        add_num(
            f'{sc}/diff_nt',
            f'CALCULATE([Chênh lệch mô tả · nghìn tỷ VND]{fstr})' if filters else '[Chênh lệch mô tả · nghìn tỷ VND]',
            f"SELECT CASE WHEN sum(descriptive_sample_count)>0 THEN sum(descriptive_difference_sum)/1e12 END FROM fact_savings_daily WHERE currency_key='VND' AND ({where})",
            tol='1e-6',
        )

    # 2. Compact KPI & Data window strings
    add_str('scope/all', '[Phạm vi dữ liệu thực tế]', exact='16/09/2022 – 31/12/2025')
    add_str('scope/2025', 'CALCULATE([Phạm vi dữ liệu thực tế], dim_date[year]=2025)', exact='01/01/2025 – 31/12/2025')
    add_str('scope/empty', 'CALCULATE([Phạm vi dữ liệu thực tế], dim_date[year_month]="2022-01")', exact='Ngoài phạm vi có dữ liệu')

    add_str('compact/packages_all', '[Rút gọn · Số gói kế hoạch]', exact='2,28 triệu')
    add_str('compact/planned_all', '[Rút gọn · Kế hoạch · tỷ VND]', exact='5.305,4 nghìn tỷ')
    add_str('compact/award_all', '[Rút gọn · Trúng thầu · tỷ VND]', exact='2.162,5 nghìn tỷ')
    add_str('compact/empty_planned', 'CALCULATE([Rút gọn · Kế hoạch · tỷ VND], dim_date[year_month]="2022-01")', exact='—')

    # 3. Dynamic Insights across filter contexts
    add_str('insight/overview_all', '[Insight · Tổng quan · Tiêu đề]', contains=['0,27%', '46,44%'])
    add_str('insight/overview_body_all', '[Insight · Tổng quan · Nội dung]', contains=['6.257', '54,83%', '0,81%', 'Hàng hóa'])
    add_str('insight/overview_hh', 'CALCULATE([Insight · Tổng quan · Tiêu đề], dim_sector[sector_name]="Hàng hóa")', contains=['≥100 tỷ'])
    add_str('insight/overview_empty', 'CALCULATE([Insight · Tổng quan · Tiêu đề], dim_date[year_month]="2022-01")', contains=['Không có dữ liệu'])

    add_str('insight/comp_all', '[Insight · Cạnh tranh · Tiêu đề]', contains=['61,61%', '427.841'])
    add_str('insight/comp_body_all', '[Insight · Cạnh tranh · Nội dung]', contains=['Tư vấn', 'Hàng hóa', 'không kết luận sai phạm'])
    add_str('insight/comp_khac', 'CALCULATE([Insight · Cạnh tranh · Nội dung], dim_sector[sector_name]="KHAC")', contains=['dưới 30 lượt mở'])
    add_str('insight/comp_empty', 'CALCULATE([Insight · Cạnh tranh · Tiêu đề], dim_date[year_month]="2022-01")', contains=['Không có lượt mở thầu'])

    add_str('insight/sav_all', '[Insight · So giá · Tiêu đề]', contains=['6,45%'])
    add_str('insight/sav_body_all', '[Insight · So giá · Nội dung]', contains=['87,66%', 'chưa phải tiết kiệm xác minh'])
    add_str('insight/sav_empty', 'CALCULATE([Insight · So giá · Tiêu đề], dim_date[year_month]="2022-01")', contains=['Không có mẫu'])

    add_str('insight/dur_days', 'CALCULATE([Insight · Thời gian · Tiêu đề], dim_duration_metric[metric_key]="plan_to_notice_days")', contains=['16,4 ngày'])
    add_str('insight/dur_hours', 'CALCULATE([Insight · Thời gian · Tiêu đề], dim_duration_metric[metric_key]="notice_to_close_hours")', contains=['238,0 giờ'])
    add_str('insight/dur_multi', '[Insight · Thời gian · Tiêu đề]', contains=['không có mẫu hợp lệ'])

    add_str('insight/sup_all', '[Insight · Người nhận · Tiêu đề]', contains=['5,90%', '160.481'])
    add_str('insight/sup_hh_body', 'CALCULATE([Insight · Người nhận · Tiêu đề], dim_sector[sector_name]="Hỗn hợp")', contains=['46,64%'])

    add_str('insight/qual_all', '[Insight · Chất lượng · Tiêu đề]', contains=['8/8 nguồn', '97,29%'])
    add_str('insight/qual_empty', 'CALCULATE([Insight · Chất lượng · Tiêu đề], dim_date[year_month]="2022-01")', contains=['Không có quan sát'])

    # 4. Page 4 dynamic titles
    add_str('title/dur_hours', 'CALCULATE([Tiêu đề · Xu hướng thời gian], dim_duration_metric[metric_key]="notice_to_close_hours")', contains=['giờ'])
    add_str('title/dur_days', 'CALCULATE([Tiêu đề · Xu hướng thời gian], dim_duration_metric[metric_key]="plan_to_notice_days")', contains=['ngày'])

    # 5. Page 6 Quality matrix missing observation vs unobserved month
    add_num('matrix/demand_2023_05', 'CALCULATE([Ma trận · demand], dim_date[year_month]="2023-05")', "SELECT count(*) FROM fact_demand_daily WHERE report_date BETWEEN DATE '2023-05-01' AND DATE '2023-05-31'", tol='0')
    add_num('matrix/comp_honhop_2023_05', 'CALCULATE([Ma trận · bidding_competition], dim_date[year_month]="2023-05", dim_sector[sector_name]="Hỗn hợp")', expected_val=0, tol='0')
    add_num('matrix/empty_2022_01', 'CALCULATE([Ma trận · demand], dim_date[year_month]="2022-01")', expected_val=None, tol='0')
    add_str('matrix/color_missing', 'CALCULATE([Màu nền ma trận · bidding_competition], dim_date[year_month]="2023-05", dim_sector[sector_name]="Hỗn hợp")', exact='#F1F5F9')

    # 6. Page 5 Top 10 Rank query
    queries.append({
        'name': 'rank/top10_suppliers',
        'dax': (
            'EVALUATE TOPN(10, FILTER(SUMMARIZECOLUMNS(dim_party[party_name_clean], dim_party[party_id_compact], '
            '"rank", [Hạng người nhận], "val", [Người nhận · tỷ VND]), NOT ISBLANK([val])), '
            '[val], DESC, dim_party[party_id_compact], ASC) ORDER BY [val] DESC'
        ),
    })
    expected.append({'name': 'rank/top10_suppliers', 'kind': 'rank10'})

    save(PROJECT / 'validation/presentation_queries.json', queries)
    save(PROJECT / 'validation/presentation_expected.json', expected)
    print(f'Prepared {len(queries)} presentation & dynamic insight queries')
    return queries, expected


def compare_results(results_path: Path) -> None:
    expected = json.loads((PROJECT / 'validation/presentation_expected.json').read_text(encoding='utf-8'))
    actual = json.loads(results_path.read_text(encoding='utf-8-sig'))
    indexed = {r['name']: r for r in actual}
    checks = []
    for exp in expected:
        name = exp['name']
        res = indexed.get(name)
        if res is None or res.get('status') != 'passed':
            checks.append({'name': name, 'status': 'failed', 'reason': 'missing or engine error', 'actual': res})
            continue
        rows = res['rows']
        if exp['kind'] == 'numeric':
            a = next(iter(rows[0].values()))
            v = exp['value']
            diff = None if a is None or v is None else abs(Decimal(str(a)) - Decimal(str(v)))
            ok = (a is None and v is None) or (diff is not None and diff <= Decimal(exp['tolerance']))
            checks.append({'name': name, 'status': 'passed' if ok else 'failed', 'expected': v, 'actual': a})
        elif exp['kind'] == 'string':
            a = next(iter(rows[0].values()))
            ok = isinstance(a, str)
            if exp['exact'] is not None:
                ok = (a == exp['exact'])
            for sub in exp['contains']:
                if not a or sub not in a:
                    ok = False
            checks.append({'name': name, 'status': 'passed' if ok else 'failed', 'expected': exp['exact'] or exp['contains'], 'actual': a})
        elif exp['kind'] == 'rank10':
            ranks = [r['[rank]'] for r in rows]
            ids = [r['dim_party[party_id_compact]'] for r in rows]
            ok = ranks == list(range(1, 11)) and len(set(ids)) == 10 and 'MST 0103053042' in ids
            checks.append({'name': name, 'status': 'passed' if ok else 'failed', 'ranks': ranks, 'ids': ids})

    status = 'passed' if all(c['status'] == 'passed' for c in checks) else 'failed'
    out = {'status': status, 'total': len(checks), 'failed': [c for c in checks if c['status'] != 'passed'], 'checks': checks}
    save(PROJECT / 'validation/presentation_validation.json', out)
    print(json.dumps({'status': status, 'total': len(checks), 'failed': out['failed']}, ensure_ascii=False, indent=2))
    if status != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['prepare', 'compare'])
    parser.add_argument('--results', type=Path, default=PROJECT / 'validation/presentation_dax_results.json')
    args = parser.parse_args()
    if args.action == 'prepare':
        build_queries()
    else:
        compare_results(args.results)
