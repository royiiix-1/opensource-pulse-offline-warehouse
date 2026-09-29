import json,pathlib,socket,time,urllib.request
R=pathlib.Path(__file__).resolve().parents[1]
checks={}
for attempt in range(120):
    checks={}
    for port in [19000,19870,18088,18041,19083]:
        try:
            with socket.create_connection(("127.0.0.1",port),timeout=1): checks[str(port)]=True
        except OSError: checks[str(port)]=False
    if all(checks.values()):
        try:
            nodes=json.load(urllib.request.urlopen("http://127.0.0.1:18088/ws/v1/cluster/nodes",timeout=3))
            checks["yarn_node_running"]=any(n["state"]=="RUNNING" for n in nodes["nodes"]["node"])
        except Exception: checks["yarn_node_running"]=False
        if all(checks.values()): break
    time.sleep(2)
out={"checks":checks,"passed":all(checks.values()),"at":time.time()}
(R/"evidence/cluster-readiness.json").write_text(json.dumps(out,indent=2))
print(json.dumps(out,indent=2))
raise SystemExit(0 if out["passed"] else 1)
