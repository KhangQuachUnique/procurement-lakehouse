import json
from decimal import Decimal
from profile_sources import PROJECT, save

cases=json.loads((PROJECT/'validation/group_expected.json').read_text(encoding='utf-8'))
results={r['name']:r for r in json.loads((PROJECT/'validation/dax_results.json').read_text(encoding='utf-8-sig'))}
checks=[]
for case in cases:
    result=results.get(case['name'],{})
    actual=[]
    if result.get('status')=='passed':
        for row in result['rows']:
            label=next(v for k,v in row.items() if k.endswith('['+case['group_column']+']'))
            actual.append({'label':label,'amount':row['[amount]']})
    actual.sort(key=lambda r:(-Decimal(str(r['amount'])),r['label']))
    expected=case['expected']
    passed=len(actual)==len(expected)==10 and all(a['label']==e['label'] and abs(Decimal(str(a['amount']))-Decimal(str(e['amount'])))<=Decimal(case['absolute_tolerance']) for a,e in zip(actual,expected))
    checks.append({'name':case['name'],'status':'passed' if passed else 'failed','actual':actual,'expected':expected})
save(PROJECT/'validation/group_validation.json',{'status':'passed' if all(c['status']=='passed' for c in checks) else 'failed','comparisons':checks})
print([(c['name'],c['status']) for c in checks])
if any(c['status']!='passed' for c in checks):raise SystemExit(1)
