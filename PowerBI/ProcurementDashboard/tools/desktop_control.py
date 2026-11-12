"""Inspect only the Power BI window opened for this project; capture evidence inside validation/."""
import argparse
import json
from pathlib import Path
from pywinauto import Desktop
from pywinauto.keyboard import send_keys
import win32gui
import win32process
import win32api
import time
from PIL import ImageGrab

ROOT=Path(__file__).resolve().parents[1]

def activate(window):
    handle=window.handle
    if win32gui.IsIconic(handle):win32gui.ShowWindow(handle,9)
    if win32gui.GetWindowPlacement(handle)[1]!=3:win32gui.ShowWindow(handle,3)
    try:win32gui.SetForegroundWindow(handle)
    except Exception:window.set_focus()
    time.sleep(0.15)
    return Desktop(backend='uia').window(handle=handle).wrapper_object()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--action',choices=['inspect','click','keys','capture','close','choose','navigate'],default='inspect')
    parser.add_argument('--title',default='.*ProcurementDashboard.*')
    parser.add_argument('--name')
    parser.add_argument('--keys')
    parser.add_argument('--filename',default='desktop-current')
    parser.add_argument('--pid',type=int)
    parser.add_argument('--control-type')
    parser.add_argument('--index',type=int,default=0)
    parser.add_argument('--value')
    parser.add_argument('--page')
    args=parser.parse_args()
    desktop=Desktop(backend='uia')
    if args.pid:
        windows=desktop.windows(process=args.pid,visible_only=False)
    else:
        windows=[w for w in desktop.windows() if 'Power BI' in w.window_text() or 'ProcurementDashboard' in w.window_text()]
    print('windows',[(w.handle,w.window_text()) for w in windows])
    if not windows:
        raise RuntimeError('No Power BI window')
    window=next((w for w in windows if 'ProcurementDashboard' in w.window_text()),windows[-1])
    if args.action=='close':
        if 'ProcurementDashboard' not in window.window_text():
            raise RuntimeError('Refusing to close an unrelated window')
        win32gui.PostMessage(window.handle,0x0112,0xF060,0)
        deadline=time.monotonic()+25
        while win32gui.IsWindow(window.handle) and time.monotonic()<deadline:
            time.sleep(0.25)
        if win32gui.IsWindow(window.handle):
            raise RuntimeError('Window did not close; inspect dialogs before starting another instance')
        print('Confirmed project window closed')
    if args.action in ['choose','navigate']:
        window=activate(window)
        if args.page:
            button=next(c for c in window.descendants() if c.element_info.control_type=='Button' and c.window_text()==args.page)
            button.click_input(pressed='control')
            time.sleep(0.7)
    if args.action=='choose':
        combos=[]
        for _ in range(10):
            window=Desktop(backend='uia').window(handle=window.handle).wrapper_object()
            combos=[c for c in window.descendants() if c.element_info.control_type=='ComboBox' and c.window_text()==args.name]
            if combos:break
            time.sleep(0.3)
        if not combos:raise RuntimeError('No slicer: '+str(args.name))
        combo=combos[0]
        options=[c for c in window.descendants() if c.element_info.control_type=='ListItem' and c.window_text()==args.value]
        if not options:combo.click_input()
        for _ in range(10):
            options=[c for c in window.descendants() if c.element_info.control_type=='ListItem' and c.window_text()==args.value]
            if options:break
            time.sleep(0.15)
        if not options:raise RuntimeError('No dropdown option: '+str(args.value))
        if win32process.GetWindowThreadProcessId(win32gui.GetForegroundWindow())[1]!=window.process_id():
            raise RuntimeError('Refusing input: another app is foreground')
        options[0].click_input()
        time.sleep(0.5)
        if win32process.GetWindowThreadProcessId(win32gui.GetForegroundWindow())[1]==window.process_id():
            send_keys('{ESC}{ESC}')
    if args.action=='click':
        window=activate(window)
        targets=[]
        for _ in range(10):
            targets=[c for c in window.descendants() if c.window_text()==args.name and (not args.control_type or c.element_info.control_type==args.control_type)]
            if targets:break
            time.sleep(0.3)
        if not targets:
            raise RuntimeError(f'No element: {args.name}')
        target=targets[args.index]
        print('target',target.element_info.control_type,target.window_text(),str(target.rectangle()))
        if target.element_info.control_type=='TabItem':
            try: target.select()
            except Exception: target.click_input()
        elif target.element_info.control_type=='ComboBox':
            target.click_input()
        elif target.element_info.control_type=='ListItem':
            target.click_input()
        else:
            try: target.invoke()
            except Exception: target.click_input()
    if args.action=='keys':
        window=activate(window)
        send_keys(args.keys,with_spaces=True,pause=0.05)
    if args.action in ['inspect','capture','choose','navigate']:
        folder=ROOT/'validation/screenshots';folder.mkdir(parents=True,exist_ok=True)
        window=activate(window)
        bounds=win32gui.GetWindowRect(window.handle)
        win32api.SetCursorPos((bounds[0]+650,bounds[1]+300))
        time.sleep(0.2)
        foreground=win32gui.GetForegroundWindow()
        if win32process.GetWindowThreadProcessId(foreground)[1] != window.process_id():
            raise RuntimeError('Refusing screenshot: Power BI is not foreground')
        if win32gui.IsIconic(window.handle):
            raise RuntimeError('Refusing screenshot: Power BI is minimized')
        ImageGrab.grab(bbox=win32gui.GetWindowRect(window.handle)).save(folder/(args.filename+'.png'))
        entries=[]
        for c in window.descendants():
            try:
                name=c.window_text()
                if name:
                    entries.append({'name':name,'type':c.element_info.control_type,'automation_id':c.element_info.automation_id,'rect':str(c.rectangle())})
            except Exception: pass
        (folder/(args.filename+'.uia.json')).write_text(json.dumps(entries,ensure_ascii=False,indent=2),encoding='utf-8')
        relevant=[e for e in entries if e['type'] in ['Window','Dialog'] or any(s in e['name'].lower() for s in ['error','problem','issue','failed'])]
        print(json.dumps(relevant[:18],ensure_ascii=False,indent=2))

if __name__=='__main__': main()
