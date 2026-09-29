"""Fetch only SHA-pinned artifacts from committed lock; install without network resolution."""
import hashlib,json,pathlib,subprocess,urllib.request,urllib.parse
R=pathlib.Path(__file__).resolve().parents[1]
dest=R/"build/airflow-install/packages";dest.mkdir(parents=True,exist_ok=True)
locked=json.loads((R/"config/airflow-artifacts.lock.json").read_text())
total=sum(item["bytes"] for item in locked)
if total>200*(1<<20):raise RuntimeError("artifact budget exceeded")
for item in locked:
    if urllib.parse.urlparse(item["url"]).hostname!="files.pythonhosted.org":raise RuntimeError("unexpected package origin")
    path=dest/urllib.parse.unquote(urllib.parse.urlparse(item["url"]).path.rsplit("/",1)[1])
    if not path.exists():
        part=path.with_suffix(path.suffix+".part")
        with urllib.request.urlopen(item["url"],timeout=60) as src,part.open("xb") as out:
            n=0
            for block in iter(lambda:src.read(1<<20),b""):
                n+=len(block)
                if n>item["bytes"]:raise RuntimeError("artifact size exceeded")
                out.write(block)
        if part.stat().st_size!=item["bytes"] or hashlib.sha256(part.read_bytes()).hexdigest()!=item["sha256"]:raise RuntimeError("checksum mismatch")
        part.rename(path)
    if hashlib.sha256(path.read_bytes()).hexdigest()!=item["sha256"]:raise RuntimeError("cache checksum mismatch")
python=R/".runtime/airflow-env/bin/python"
args=[str(python),"-m","pip","--isolated","--cache-dir",str(R/"build/airflow-install/pip-cache"),"install","--no-index","--no-build-isolation","--find-links",str(dest),"-c",str(R/"config/airflow-constraints-2.10.5-python3.11.txt"),"apache-airflow==2.10.5"]
with (R/"build/airflow-install/install.log").open("ab") as out:subprocess.run(args,stdout=out,stderr=subprocess.STDOUT,check=True)
freeze=subprocess.check_output([str(python),"-m","pip","freeze","--all"],text=True)
(R/"requirements.airflow.lock.txt").write_text(freeze)
print("Airflow lock verified",len(locked),"artifacts",total,"bytes")
