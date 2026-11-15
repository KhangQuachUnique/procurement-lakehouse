"""Control only the current ProcurementDashboard window; capture evidence on request."""
import argparse
import json
import time
from pathlib import Path

import win32gui
import win32process
from PIL import ImageGrab
from pywinauto import Desktop
from pywinauto.keyboard import send_keys

ROOT = Path(__file__).resolve().parents[1]


def activate(window):
    handle = window.handle
    if win32gui.IsIconic(handle):
        win32gui.ShowWindow(handle, 9)
    if win32gui.GetWindowPlacement(handle)[1] != 3:
        win32gui.ShowWindow(handle, 3)
    try:
        win32gui.SetForegroundWindow(handle)
    except Exception:
        window.set_focus()
    time.sleep(.15)
    return Desktop(backend='uia').window(handle=handle).wrapper_object()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', choices=['inspect', 'capture', 'save', 'close'], default='inspect')
    parser.add_argument('--pid', type=int)
    parser.add_argument('--filename', default='desktop-current')
    args = parser.parse_args()
    windows = Desktop(backend='uia').windows(**({'process': args.pid} if args.pid else {}))
    candidates = [w for w in windows if w.window_text() == 'ProcurementDashboard']
    if len(candidates) != 1:
        raise RuntimeError('Expected exactly one matching ProcurementDashboard window')
    window = activate(candidates[0])
    if win32process.GetWindowThreadProcessId(win32gui.GetForegroundWindow())[1] != window.process_id():
        raise RuntimeError('Project must be foreground before input or capture')
    if args.action == 'save':
        send_keys('^s')
    elif args.action == 'close':
        send_keys('%{F4}')
    elif args.action == 'capture':
        if Path(args.filename).name != args.filename:
            raise ValueError('Filename must be a basename')
        folder = ROOT/'validation/screenshots'
        folder.mkdir(parents=True, exist_ok=True)
        ImageGrab.grab(bbox=win32gui.GetWindowRect(window.handle)).save(folder/(args.filename+'.png'))
    else:
        print(json.dumps({'pid': window.process_id(), 'title': window.window_text(),
                          'tabs': [t.window_text() for t in window.descendants(control_type='TabItem')]},
                         ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
