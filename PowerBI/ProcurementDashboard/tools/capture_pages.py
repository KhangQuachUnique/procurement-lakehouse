"""Capture eight pages using verified navigator geometry in the project window."""
import argparse
import json
import time

import win32gui
import win32process
import win32api
from PIL import ImageGrab
from pywinauto import Desktop, mouse
from desktop_control import ROOT, activate


def grab(window):
    if win32process.GetWindowThreadProcessId(win32gui.GetForegroundWindow())[1] != window.process_id():
        raise RuntimeError('Power BI project must be foreground')
    return ImageGrab.grab(bbox=win32gui.GetWindowRect(window.handle))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pid', required=True, type=int)
    args = parser.parse_args()
    window = next(w for w in Desktop(backend='uia').windows(process=args.pid) if w.window_text() == 'ProcurementDashboard')
    order = json.loads((ROOT/'ProcurementDashboard.Report/definition/pages/pages.json').read_text())['pageOrder']
    out = ROOT/'validation/screenshots'
    out.mkdir(parents=True, exist_ok=True)
    evidence = []
    inventory=json.loads((ROOT/'validation/report_inventory.json').read_text(encoding='utf-8'))
    titles={p['name']:p['displayName'] for p in inventory['pages']}
    for page in order+[order[0]]:
        window = activate(window)
        tabs=[t for t in window.descendants(control_type='TabItem') if t.window_text()==titles[page]]
        if len(tabs)!=1:
            raise RuntimeError('Ambiguous page tab '+page)
        rect=tabs[0].rectangle()
        mouse.click(coords=((rect.left+rect.right)//2, (rect.top+rect.bottom)//2))
        time.sleep(2)
        if not tabs[0].is_selected():
            raise RuntimeError('Page selection not confirmed '+page)
        window=activate(window)
        bounds=win32gui.GetWindowRect(window.handle)
        win32api.SetCursorPos((bounds[2]-50,bounds[1]+100))
        shot=grab(window)
        path=out/('final-'+page+'.png')
        shot.save(path)
        evidence.append({'page':page,'file':str(path.relative_to(ROOT)),'selected':True})
        print(page,'captured',flush=True)
    (ROOT/'validation/page_capture_validation.json').write_text(json.dumps({
        'status': 'passed', 'method': 'UIA tab selection confirmed, foreground checks; visual review required.',
        'pages': evidence}, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
