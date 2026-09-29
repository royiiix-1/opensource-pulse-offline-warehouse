"""Small evidence replay for an interview; explicitly not a live database query."""
import json,pathlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
r=json.loads((ROOT/'evidence/public/portfolio.json').read_text())
print('OpenSource Pulse — saved evidence replay (not a live query)')
print('Dates:',r['dates'][0],'..',r['dates'][-1])
print('Distinct events:',format(r['distinct_events'],','))
print('Repository observation versions:',format(r['tables']['dim_repository_scd2'],','))
for x in r['performance']:
    print(x['experiment'],x['variant'],'median_seconds=',round(x['median_seconds'],3),'n=',x['n'],'files=',x['scan_files'])
print('Failure recovery:',r['failure_recovery']['recovered_complete'])
print('Remote CI executed:',r['remote_ci_executed'])
print('For live validation use scripts/verify_portfolio.py --mode full in the configured WSL environment.')
