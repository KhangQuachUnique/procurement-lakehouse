"""Compare live DAX values with exact-source SQL baselines; no result is silently skipped."""
import json
from decimal import Decimal
from profile_sources import PROJECT, save

def main():
    expected=json.loads((PROJECT/'validation/numeric_expected.json').read_text(encoding='utf-8'))
    actual=json.loads((PROJECT/'validation/dax_results.json').read_text(encoding='utf-8-sig'))
    indexed={r['name']:r for r in actual}
    results=[]
    for e in expected:
        r=indexed.get(e['name'])
        if r is None or r['status']!='passed':
            results.append({'name':e['name'],'status':'failed','reason':'missing or failed DAX query','actual':r})
            continue
        rows=r['rows']
        if len(rows)!=1 or len(rows[0])!=1:
            results.append({'name':e['name'],'status':'failed','reason':'expected scalar row','actual':rows})
            continue
        a=next(iter(rows[0].values()))
        value=e['value']
        diff=None if a is None or value is None else abs(Decimal(str(a))-Decimal(str(value)))
        passed=(a is None and value is None) or (diff is not None and diff<=Decimal(e['absolute_tolerance']))
        results.append({'name':e['name'],'status':'passed' if passed else 'failed','expected':value,'actual':a,'difference':diff,'tolerance':e['absolute_tolerance']})
    failures=[r for r in actual if r['status']!='passed']
    status='passed' if all(r['status']=='passed' for r in results) and not failures else 'failed'
    report={'status':status,'queries_executed':len(actual),'independently_compared':len(results),'engine_failures':failures,'comparisons':results}
    save(PROJECT/'validation/numeric_validation.json',report)
    print(json.dumps({'status':status,'queries':len(actual),'comparisons':len(results),'failures':failures+[r for r in results if r['status']=='failed']},ensure_ascii=False,indent=2,default=str))
    if status!='passed':raise SystemExit(1)

if __name__=='__main__':main()
