"""Pinned project-local artifacts with bounded transfers and append-only budget reservations."""
import hashlib,json,pathlib,shutil,subprocess,time,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1]
lock=json.loads((ROOT/"config/runtime-lock.json").read_text())
runtime=ROOT/".runtime"; cache=ROOT/"build/runtime-downloads"
runtime.mkdir(exist_ok=True); cache.mkdir(parents=True,exist_ok=True)
ledger=ROOT/"evidence/runtime-download-ledger.jsonl"
for a in lock["artifacts"]:
    final=cache/(a["name"]+".tar.gz")
    def digest(p):
        h=hashlib.new(a["algorithm"])
        with p.open("rb") as f:
            for b in iter(lambda:f.read(1<<20),b""): h.update(b)
        return h.hexdigest()
    if not final.exists():
        used=sum(json.loads(s).get("reserved_bytes",0) for s in ledger.read_text().splitlines()) if ledger.exists() else 0
        if used+a["max_bytes"]>lock["budget_bytes"]: raise RuntimeError("network budget exhausted")
        if shutil.disk_usage(ROOT).free<40*(1<<30): raise RuntimeError("disk reserve insufficient")
        attempt=uuid.uuid4().hex
        part=cache/(a["name"]+"-"+attempt+".part")
        with ledger.open("a") as f: f.write(json.dumps(dict(artifact=a["name"],reserved_bytes=a["max_bytes"],at=time.time()))+"\n")
        print("Downloading",a["name"],flush=True)
        details={"artifact":a["name"],"attempt":attempt,"started_at":time.time(),"segments":[]}
        evidence=ROOT/"evidence"/("download-"+a["name"]+"-"+attempt+".json")
        try:
            if a.get("range_bytes"):
                size=a["range_bytes"]
                with part.open("xb") as output:
                    for start in range(0,size,8<<20):
                        end=min(start+(8<<20),size)-1
                        chunk=cache/(a["name"]+"-"+attempt+"-"+str(start)+".chunk")
                        headers=chunk.with_suffix(".headers")
                        args=["curl","--fail","--location","--proto","=https","--range",f"{start}-{end}","--max-filesize",str(end-start+1),"--max-time","90","--connect-timeout","20","--dump-header",str(headers),"--output",str(chunk),a["url"]]
                        p=subprocess.run(args,capture_output=True,text=True)
                        details["segments"].append({"start":start,"end":end,"exit_code":p.returncode})
                        if p.returncode: raise RuntimeError(p.stderr[-600:])
                        if chunk.stat().st_size!=end-start+1: raise RuntimeError("range length mismatch")
                        if f"content-range: bytes {start}-{end}/{size}" not in headers.read_text().lower(): raise RuntimeError("Content-Range mismatch")
                        with chunk.open("rb") as f: shutil.copyfileobj(f,output,1<<20)
                        print(a["name"],end+1,"/",size,flush=True)
            else:
                p=subprocess.run(["curl","--fail","--location","--proto","=https","--max-filesize",str(a["max_bytes"]),"--max-time","300","--connect-timeout","30","--output",str(part),a["url"]],capture_output=True,text=True)
                details["exit_code"]=p.returncode
                if p.returncode: raise RuntimeError(p.stderr[-1000:])
            if digest(part)!=a["digest"]: raise RuntimeError("checksum mismatch")
            part.rename(final)
            details.update(status="VERIFIED",bytes=final.stat().st_size,digest=a["digest"])
        except Exception as error:
            details.update(status="FAILED",error=str(error))
            raise
        finally:
            details["ended_at"]=time.time()
            evidence.write_text(json.dumps(details,indent=2))
    if digest(final)!=a["digest"]: raise RuntimeError("cache checksum mismatch")
    target=runtime/a["folder"]
    if not target.exists():
        stage=runtime/("extract-"+uuid.uuid4().hex); stage.mkdir()
        subprocess.run(["tar","-xzf",str(final),"-C",str(stage)],check=True)
        (stage/a["folder"]).rename(target)
    print("Verified and ready:",a["name"],flush=True)
(ROOT/"evidence/runtime-artifacts-verified.json").write_text(json.dumps(lock,indent=2))
