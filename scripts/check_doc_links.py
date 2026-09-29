import json,pathlib,re,sys,urllib.parse
root=pathlib.Path(sys.argv[1]).resolve();issues=[]
for file in [root/'README.md',root/'STATUS.md',*list((root/'docs').glob('*.md'))]:
    for target in re.findall(r'(?<!!)\[[^\]]+\]\(([^)]+)\)',file.read_text(encoding='utf-8-sig')):
        target=target.strip('<>').split('#',1)[0]
        if not target or re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:',target):continue
        if not (file.parent/urllib.parse.unquote(target)).exists():issues.append({'file':file.relative_to(root).as_posix(),'missing':target})
print(json.dumps({'passed':not issues,'issues':issues},indent=2));raise SystemExit(0 if not issues else 1)
