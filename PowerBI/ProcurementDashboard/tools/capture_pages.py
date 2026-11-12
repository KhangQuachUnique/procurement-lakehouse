"""Capture all native page-navigation states when WebView UIA rectangles are stale.

Only clicks four detected navy navigation rectangles inside the verified project window.
Never captures another foreground application or guesses a click if detection is ambiguous.
"""
import argparse
import json
import time
from collections import deque
from pathlib import Path
import win32gui
import win32process
from PIL import ImageGrab, Image
from pywinauto import Desktop, mouse
from pywinauto.keyboard import send_keys
from desktop_control import activate

ROOT=Path(__file__).resolve().parents[1]

def grab(window):
    if win32process.GetWindowThreadProcessId(win32gui.GetForegroundWindow())[1]!=window.process_id():
        raise RuntimeError('Another application is foreground; refusing capture/input')
    if win32gui.IsIconic(window.handle):raise RuntimeError('Window is minimized')
    return ImageGrab.grab(bbox=win32gui.GetWindowRect(window.handle))

def navigation(image,strict=True):
    w,h=image.size
    x0,x1=int(w*.33),int(w*.83)
    y0,y1=200,min(520,h)
    pixels=image.convert('RGB').load()
    points=set()
    for y in range(y0,y1):
        for x in range(x0,x1):
            r,g,b=pixels[x,y]
            if r<55 and g<85 and b>g+12 and b>r+25:
                points.add((x,y))
    candidates=[]
    while points:
        p=points.pop();queue=[p];left=right=p[0];top=bottom=p[1];count=0
        while queue:
            x,y=queue.pop();count+=1
            left=min(left,x);right=max(right,x);top=min(top,y);bottom=max(bottom,y)
            for dx,dy in [(1,0),(-1,0),(0,1),(0,-1),(1,1),(-1,-1),(1,-1),(-1,1)]:
                q=(x+dx,y+dy)
                if q in points:points.remove(q);queue.append(q)
        bw,bh=right-left+1,bottom-top+1
        if 100<=bw<=350 and 25<=bh<=90:
            candidates.append({'box':[left,top,right,bottom],'density':count/(bw*bh)})
    candidates.sort(key=lambda r:r['box'][0])
    if strict and (len(candidates)!=4 or max(c['box'][1] for c in candidates)-min(c['box'][1] for c in candidates)>5):
        raise RuntimeError('Expected exactly four native page navigation rectangles: '+str(candidates))
    return candidates

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--pid',type=int,required=True);args=parser.parse_args()
    windows=Desktop(backend='uia').windows(process=args.pid,visible_only=False)
    window=next(w for w in windows if w.window_text()=='ProcurementDashboard')
    page_names=['overview','savings','competition','duration']
    evidence=[]
    geometry=None
    original_size=None
    reference=ROOT/'validation/screenshots/final-overview.png'
    if reference.exists():
        reference_image=Image.open(reference)
        try:
            geometry=navigation(reference_image)
            original_size=reference_image.size
        except RuntimeError:pass
    for ix,name in enumerate(page_names+[page_names[0]]):
        index=ix%4
        window=activate(window)
        before=grab(window)
        rects=navigation(before,strict=False)
        if len(rects)==4:
            geometry=rects
            original_size=before.size
        if geometry is None or before.size!=original_size:
            raise RuntimeError('Navigation geometry has not been verified for this window size')
        rects=geometry
        left,top,right,bottom=rects[index]['box']
        bounds=win32gui.GetWindowRect(window.handle)
        native=Desktop(backend='win32').window(handle=window.handle).wrapper_object()
        native.click_input(coords=(bounds[0]+(left+right)//2,bounds[1]+(top+bottom)//2),pressed='control',absolute=True)
        time.sleep(1.5)
        after=grab(window)
        actual=navigation(after,strict=False)
        selected=max(actual,key=lambda c:c['density'])
        if selected['density']<0.5:raise RuntimeError('No active navigation fill detected')
        selected_center=(selected['box'][0]+selected['box'][2])/2
        active=min(range(4),key=lambda j:abs((geometry[j]['box'][0]+geometry[j]['box'][2])/2-selected_center))
        if active!=index:raise RuntimeError(f'Navigation did not activate {name}: {actual}')
        out=ROOT/'validation/screenshots'/('final-'+name+'.png')
        after.save(out)
        evidence.append({'page':name,'status':'captured','active_navigation_index':active,'file':str(out.relative_to(ROOT))})
        print(name,'captured and active state verified',flush=True)
    (ROOT/'validation/page_capture_validation.json').write_text(json.dumps({'status':'passed','pages':evidence,'method':'Native Page Navigator Ctrl+click; window-specific foreground check; reviewer must inspect screenshots.'},indent=2),encoding='utf-8')

if __name__=='__main__':main()
