"""Audited model snapshot commit, independent of the existing daily release pointer."""
import datetime as dt,fcntl,hashlib,json,pathlib,subprocess,time,uuid
from warehouse.common import ROOT,read,atomic,dates
from warehouse.hdfs import HDFS
from warehouse.operations import disk_usage
CONTROL=ROOT/'build/control-m3'
def fingerprint():
    files=[ROOT/'spark/m3_snapshot.py',ROOT/'spark/m3_aggregates.py',ROOT/'spark/repository_scd2.py',ROOT/'m3/control.py']
    h=hashlib.sha256()
    for p in files:h.update(str(p.relative_to(ROOT)).encode());h.update(p.read_bytes())
    return h.hexdigest()
def run(start,end):
    selected=dates(start,end)
    if len(selected)!=7:raise ValueError('M3 publication requires seven complete consecutive dates')
    CONTROL.mkdir(parents=True,exist_ok=True)
    with (CONTROL/'model.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        inputs=[]
        for date in selected:
            p=read(ROOT/'build/control-v2/published'/(date+'.json'))
            if p['coverage']!='COMPLETE' or p['hours']!=list(range(24)):raise ValueError('incomplete date')
            inputs.append(p)
        fs=HDFS()
        for source in inputs:
            if fs.inventory(source['hdfs_path'])!=source['inventory']:
                raise RuntimeError('input release data mutated: '+source['date'])
        digest=fingerprint()
        identity=hashlib.sha256((digest+json.dumps([(p['date'],p['identity']) for p in inputs])).encode()).hexdigest()
        pointer=CONTROL/'published.json';fs=HDFS()
        evidence=ROOT/'evidence'/('m3-model-'+uuid.uuid4().hex);evidence.mkdir()
        if pointer.exists() and read(pointer)['identity']==identity:
            active=read(pointer)
            if fs.inventory(active['hdfs_path'])!=active['inventory']:raise RuntimeError('published model mutated')
            atomic(evidence/'publication.json',{'status':'NO_OP','identity':identity,'release':active['release']})
            return active
        if disk_usage()+(16<<30)>read(ROOT/'config/m2-budget.json')['disk_limit_bytes']:raise RuntimeError('M3 storage headroom insufficient')
        release='m'+identity[:16]+uuid.uuid4().hex[:8]
        ctx={'inputs':inputs,'identity':identity,'model_digest':digest,'release':release,'database':'osp3_'+release,
             'hdfs_path':'/osp-offline/model-releases/'+release,'staging_path':'/osp-offline/model-staging/'+release,'evidence':str(evidence)}
        context=evidence/'context.json';atomic(context,ctx)
        # Unique candidate location is invisible to consumers until the atomic pointer commit.
        with (evidence/'spark.log').open('wb') as log:
            process=subprocess.Popen(['spark-submit','--master','yarn',str(ROOT/'spark/m3_snapshot.py'),str(context)],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
            samples=[]
            while process.poll() is None:
                used=disk_usage();samples.append({'at':time.time(),'conservative_bytes':used})
                if used>read(ROOT/'config/m2-budget.json')['disk_limit_bytes']-(2<<30):
                    process.terminate();process.wait(timeout=60)
                    atomic(evidence/'budget.json',{'exceeded':True,'samples':samples});raise RuntimeError('M3 disk watchdog')
                time.sleep(15)
        atomic(evidence/'budget.json',{'exceeded':False,'samples':samples})
        if process.returncode:raise RuntimeError('M3 Spark failed; preserved '+str(evidence))
        result=read(evidence/'spark-result.json')
        if result['status']!='VALIDATED' or not all(c['passed'] for c in result['checks'].values()):raise RuntimeError('M3 quality gate')
        if fingerprint()!=digest:raise RuntimeError('model changed before publication')
        for p in inputs:
            if read(ROOT/'build/control-v2/published'/(p['date']+'.json'))['identity']!=p['identity']:raise RuntimeError('input date changed before publication')
        inventory=fs.inventory(ctx['hdfs_path'])
        if not inventory:raise RuntimeError('empty model')
        active={'identity':identity,'model_digest':digest,'release':release,'database':ctx['database'],'hdfs_path':ctx['hdfs_path'],
                'input_identities':[(p['date'],p['identity']) for p in inputs],'tables':result['tables'],'inventory':inventory,'coverage':'COMPLETE_7_DATES'}
        atomic(pointer,active);atomic(evidence/'publication.json',{'status':'PUBLISHED','identity':identity,'release':release})
        return active
