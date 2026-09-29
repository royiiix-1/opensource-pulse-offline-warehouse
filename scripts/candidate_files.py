"""Explicit candidate scope. Raw evidence, personal handoff and runtimes stay local."""
import hashlib,pathlib,re
ROOT=pathlib.Path(__file__).resolve().parents[1]
EXCLUDED_SCRIPTS={'migrate_m2_control_budget.py','migrate_m2_metadata_check.py','resume_m2_downstream.py','resume_m2_downstream.sh','inspect_m4_event_links.py','diagnose_partition_location.py','prefetch_m2.py','verify_ods_memory_repair.py'}
def candidates(root=ROOT):
    roots=['README.md','STATUS.md','LICENSE','THIRD_PARTY_NOTICES.md','.gitignore','pyproject.toml','requirements.txt','requirements.airflow.lock.txt']
    files=[root/p for p in roots if (root/p).is_file()]
    for name in ['acquisition','warehouse','m3','spark','hive','tests','docs','config','scripts','.github','airflow/dags','evidence/public']:
        base=root/name
        if not base.exists():continue
        for p in base.rglob('*'):
            rel=p.relative_to(root)
            if '__pycache__' in p.parts or p.suffix=='.pyc' or str(rel).replace('\\','/').startswith('config/runtime/'):continue
            if name=='scripts' and p.name in EXCLUDED_SCRIPTS:continue
            if p.is_file():files.append(p)
    for n in range(1,6):
        p=root/'evidence'/f'M{n}_SUMMARY.json'
        if p.is_file():files.append(p)
    return sorted(set(files))
def scan(root=ROOT):
    issues=[];inventory=[]
    patterns={'private_key':r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
      'github_token':r'gh[pousr]_[A-Za-z0-9]{30,}', 'aws_access_key':r'AKIA[0-9A-Z]{16}',
      'bearer_literal':r'(?i)Bearer\s+[A-Za-z0-9_.-]{24,}'}
    for p in candidates(root):
        rel=p.relative_to(root).as_posix()
        cursor=root;linked=False
        for component in p.relative_to(root).parts:
            cursor=cursor/component
            linked=linked or cursor.is_symlink()
        if linked or not p.resolve().is_relative_to(root.resolve()):
            issues.append({'path':rel,'reason':'symlink_or_escape'});continue
        size=p.stat().st_size
        if size>5*1024**2:
            inventory.append({'path':rel,'bytes':size,'sha256':None})
            issues.append({'path':rel,'reason':'file_above_5_MiB'});continue
        with p.open('rb') as stream:data=stream.read(5*1024**2+1)
        if len(data)>5*1024**2:
            issues.append({'path':rel,'reason':'file_grew_above_limit'});continue
        inventory.append({'path':rel,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
        if p.suffix.lower() in {'.gz','.parquet','.sqlite','.db','.jar','.zip','.tgz'} or p.name.startswith('.env'):
            issues.append({'path':rel,'reason':'runtime_or_sensitive_file'})
        text=data.decode('utf-8',errors='replace')
        for label,pattern in patterns.items():
            if re.search(pattern,text):issues.append({'path':rel,'reason':label})
    return {'passed':not issues,'issues':issues,'files':inventory,'file_count':len(inventory),
      'total_bytes':sum(x['bytes'] for x in inventory),'scope':'candidate allowlist only; signature scan, not a guarantee that all secrets are detectable'}
