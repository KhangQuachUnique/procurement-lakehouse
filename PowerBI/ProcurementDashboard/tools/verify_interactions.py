"""Exercise real slicers and compare accessible rendered cards with independent SQL."""
import argparse
import json
import re
import time
from decimal import Decimal

from capture_pages import grab
from pywinauto import Desktop, mouse
from pywinauto.keyboard import send_keys
import win32gui
import win32process

from desktop_control import activate
from profile_sources import PROJECT, WORKSPACE, connection, discover, save


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pid', type=int, required=True)
    parser.add_argument('--restore-only', action='store_true')
    args = parser.parse_args()
    window = next(w for w in Desktop(backend='uia').windows(process=args.pid) if w.window_text() == 'ProcurementDashboard')
    con = connection(discover(WORKSPACE/'PowerBI/export/parquet'))
    evidence = []
    if args.restore_only:
        previous=json.loads((PROJECT/'validation/interaction_validation.json').read_text(encoding='utf-8'))
        evidence=[e for e in previous['checks'] if e['name'] != 'reset_all']
        if not evidence or any(e['status']!='passed' for e in evidence):
            raise RuntimeError('Restore-only requires five existing successful interaction checks')
    shots = PROJECT/'validation/screenshots'

    def foreground():
        if win32process.GetWindowThreadProcessId(win32gui.GetForegroundWindow())[1] != args.pid:
            raise RuntimeError('Another app became foreground; refusing mouse or keyboard input')

    def click(rect, arrow=False):
        foreground()
        mouse.click(coords=(rect.right-12 if arrow else rect.left+20, (rect.top+rect.bottom)//2))
        time.sleep(.15)

    def navigate(title):
        nonlocal window
        window = activate(window)
        tab = next(t for t in window.descendants(control_type='TabItem') if t.window_text() == title)
        foreground()
        if not tab.is_selected():
            nav=next(e for e in window.descendants(control_type='Button') if e.window_text()==title)
            send_keys('{VK_CONTROL down}')
            try:
                nav.click_input()
            finally:
                send_keys('{VK_CONTROL up}')
            time.sleep(2)
        if not tab.is_selected():
            raise RuntimeError('Sidebar navigation failed: '+title)

    def option(label, combo_rect):
        for attempt in range(20):
            lists=[i.rectangle() for i in window.descendants(control_type='List') if abs(i.rectangle().left-combo_rect.left)<30 and abs(i.rectangle().top-combo_rect.bottom)<8]
            bottom=min((r.bottom for r in lists),default=combo_rect.bottom+245)
            items = window.descendants(control_type='ListItem')
            matches = [i for i in items if i.window_text() == label and abs(i.rectangle().left-combo_rect.left)<30 and combo_rect.bottom-5 <= i.rectangle().top < bottom]
            matches=[i for i in matches if i.rectangle().bottom <= bottom]
            if matches:
                return matches[0]
            # Date values are virtualized; scroll only inside the observed dropdown.
            dates = [i.window_text() for i in items if re.fullmatch(r'\d{4}-\d{2}', i.window_text()) and abs(i.rectangle().left-combo_rect.left)<30 and combo_rect.bottom-5 <= i.rectangle().top < bottom]
            if not dates:
                raise RuntimeError('Option not available: '+label)
            foreground()
            mouse.scroll(coords=(combo_rect.left+100, combo_rect.bottom+100),
                         wheel_dist=(12 if label=='Select all' else 1) * (1 if label=='Select all' or label < min(dates) else -1))
            time.sleep(.2)
        raise RuntimeError('Could not reach option '+label)

    def select(label, values, clear=False):
        nonlocal window
        window = activate(window)
        send_keys('{ESC}{ESC}')
        combo = next(e for e in window.descendants(control_type='ComboBox') if e.window_text() == label)
        rect = combo.rectangle()
        for attempt in range(3):
            click(rect, arrow=True)
            popup=[i for i in window.descendants(control_type='ListItem') if abs(i.rectangle().left-rect.left)<30 and rect.bottom-5 <= i.rectangle().top < rect.bottom+245]
            if popup: break
            time.sleep(.3)
        if clear:
            # All -> none is unfiltered in Power BI; selecting target values then creates the desired set.
            for _ in range(2):
                click(option('Select all', rect).rectangle())
        for value in values:
            click(option(value, rect).rectangle())
        foreground()
        send_keys('{ESC}')
        time.sleep(.6)

    def vi(value, places):
        return format(Decimal(str(value)), f',.{places}f').replace(',', '_').replace('.', ',').replace('_', '.')

    def check(name, measure, expected, places=2, suffix=''):
        wanted = '—' if expected is None else vi(expected, places)+suffix
        actual = None
        for _ in range(10):
            images = [e.window_text() for e in window.descendants(control_type='Image')]
            prefix = 'Hiển thị · '+measure+' '
            candidates = [s[len(prefix):-1] for s in images if s.startswith(prefix) and s.endswith('.')]
            if candidates and len(set(candidates)) == 1:
                actual = candidates[0]
                if actual == wanted:
                    break
            time.sleep(.3)
        result = {'name': name, 'measure': measure, 'expected_rendered': wanted, 'actual_rendered': actual,
                  'status': 'passed' if actual == wanted else 'failed'}
        evidence.append(result)
        print(name,result['status'],flush=True)
        if actual != wanted:
            raise RuntimeError(str(result))

    def screenshot(name):
        foreground()
        grab(window).save(shots/(name+'.png'))

    def sql(query):
        return con.execute(query).fetchone()[0]

    try:
        if not args.restore_only:
            navigate('Tổng quan thị trường')
            select('Tháng', [], clear=True)
            select('Lĩnh vực', [], clear=True)
            check('baseline_all', 'Kế hoạch · tỷ VND', sql('SELECT sum(planned_amount_sum)/1000000000 FROM fact_demand_daily WHERE currency_key=\'VND\''))
            select('Lĩnh vực', ['Hàng hóa'])
            check('sector_filter', 'Kế hoạch · tỷ VND', sql("SELECT sum(planned_amount_sum)/1000000000 FROM fact_demand_daily WHERE currency_key='VND' AND sector_key='SEC_HH'"))
            share=sql("SELECT 100*sum(CASE WHEN d.band_label='Từ 100 tỷ VND trở lên' THEN f.planned_amount_sum END)/sum(f.planned_amount_sum) FROM fact_demand_daily f JOIN dim_value_band d ON f.value_band_key=d.band_key WHERE f.currency_key='VND' AND f.sector_key='SEC_HH'")
            check('filtered_insight','Giá trị nhóm từ 100 tỷ',share,suffix='%')
            screenshot('interaction-sector-hh')
            navigate('Mức độ cạnh tranh')
            check('sector_sync', 'Lượt mở thầu', sql("SELECT sum(opening_count) FROM fact_bidding_competition_daily WHERE sector_key='SEC_HH'"), places=0)
            screenshot('interaction-sector-sync')
            select('Lĩnh vực', [], clear=True)
            navigate('Thời gian quy trình')
            select('metric_label', ['Thông báo → đóng thầu'])
            check('hours_metric', 'Thời gian bình quân', sql("SELECT sum(duration_sum)/sum(sample_count) FROM fact_duration_daily WHERE metric_key='notice_to_close_hours'"), places=1, suffix=' giờ')
            screenshot('interaction-hours')
            select('metric_label', ['Kế hoạch → thông báo'])
            navigate('Mức độ cạnh tranh')
            select('Tháng', ['2022-01'], clear=True)
            check('empty_month', 'Nhà thầu bình quân', None)
            check('empty_insight','Biên độ cạnh tranh',None)
            screenshot('interaction-empty-month')
        select('Tháng', [], clear=True)
        navigate('Tổng quan thị trường')
        check('reset_all', 'Kế hoạch · tỷ VND', sql('SELECT sum(planned_amount_sum)/1000000000 FROM fact_demand_daily WHERE currency_key=\'VND\''))
        screenshot('interaction-reset')
        save(PROJECT/'validation/interaction_validation.json', {'status': 'passed', 'checks': evidence,
             'method': 'Sidebar Ctrl+click and actual slicer input at UIA rectangles; rendered card accessible names compared with SQL; default state restored.'})
        foreground()
        # Leave the regenerated PBIR as the source of truth.
        print(json.dumps({'status': 'passed', 'checks': len(evidence)}, ensure_ascii=False), flush=True)
    except Exception as exc:
        save(PROJECT/'validation/interaction_validation.json', {'status': 'failed', 'checks': evidence, 'error': str(exc)})
        raise


if __name__ == '__main__':
    main()
