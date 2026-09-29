"""Small loopback-only WebHDFS client; no arbitrary hosts, overwrite, or delete."""
import getpass,hashlib,http.client,io,json,pathlib,urllib.parse,urllib.request,urllib.error
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None
class HDFS:
    def __init__(self):
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    def url(self,path,op,**params):
        if not path.startswith("/osp-offline/") and path not in ("/osp-offline","/"):
            raise ValueError("outside project HDFS namespace")
        if ".." in pathlib.PurePosixPath(path).parts: raise ValueError("path traversal")
        return "http://127.0.0.1:19870/webhdfs/v1"+urllib.parse.quote(path,safe="/")+"?"+urllib.parse.urlencode({"op":op,"user.name":getpass.getuser(),**params})
    def request(self,path,op,method="GET",**params):
        req=urllib.request.Request(self.url(path,op,**params),method=method,data=b"" if method=="PUT" else None)
        return self.opener.open(req,timeout=60)
    def json(self,path,op,method="GET",**params):
        with self.request(path,op,method,**params) as r: return json.load(r)
    def exists(self,path):
        try: self.json(path,"GETFILESTATUS");return True
        except urllib.error.HTTPError as e:
            if e.code==404:return False
            raise
    def mkdir(self,path):
        if not self.json(path,"MKDIRS","PUT",permission="755")["boolean"]: raise RuntimeError("mkdir failed")
    def rename(self,source,dest):
        self.url(dest,"GETFILESTATUS")
        if self.exists(dest):raise FileExistsError(dest)
        if not self.json(source,"RENAME","PUT",destination=dest)["boolean"]:raise RuntimeError("rename failed")
    def permission(self,path,mode="444"):
        with self.request(path,"SETPERMISSION","PUT",permission=mode):pass
    def redirect(self,path,op,method="GET"):
        try:
            r=self.request(path,op,method,**({"overwrite":"false"} if op=="CREATE" else {}))
            r.close();raise RuntimeError("expected DataNode redirect")
        except urllib.error.HTTPError as e:
            if e.code!=307:raise
            parsed=urllib.parse.urlparse(e.headers["Location"])
            if parsed.scheme!="http" or parsed.port!=19864:raise RuntimeError("unexpected DataNode")
            # This cluster has exactly one loopback DataNode; do not follow external hosts.
            return parsed.path+"?"+parsed.query
    def put(self,local,target):
        conn=http.client.HTTPConnection("127.0.0.1",19864,timeout=180)
        try:
            with pathlib.Path(local).open("rb") as body:
                conn.request("PUT",self.redirect(target,"CREATE","PUT"),body=body,
                             headers={"Content-Length":str(pathlib.Path(local).stat().st_size),"Content-Type":"application/octet-stream"})
                r=conn.getresponse();data=r.read()
                if r.status!=201:raise RuntimeError(f"CREATE failed {r.status}: {data[:300]!r}")
        finally:conn.close()
    def sha(self,path):
        conn=http.client.HTTPConnection("127.0.0.1",19864,timeout=180)
        try:
            conn.request("GET",self.redirect(path,"OPEN")); r=conn.getresponse()
            if r.status!=200:raise RuntimeError("OPEN failed")
            h=hashlib.sha256();n=0
            for b in iter(lambda:r.read(1<<20),b""):h.update(b);n+=len(b)
            return {"bytes":n,"sha256":h.hexdigest()}
        finally:conn.close()
    def inventory(self,path):
        result={}
        for item in self.json(path,"LISTSTATUS")["FileStatuses"]["FileStatus"]:
            child=path+"/"+item["pathSuffix"]
            if item["type"]=="DIRECTORY":result.update(self.inventory(child))
            elif item["type"]=="FILE":result[child]=self.sha(child)
            else:raise RuntimeError("unexpected link")
        return result
    def size(self,path="/"):
        return self.json(path,"GETCONTENTSUMMARY")["ContentSummary"]["spaceConsumed"]
