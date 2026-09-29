import json,pathlib,sys,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
from candidate_files import scan,candidates
class CandidateSafetyTests(unittest.TestCase):
    def test_token_detected_without_echoing_value(self):
        with tempfile.TemporaryDirectory(dir=ROOT/'build') as d:
            root=pathlib.Path(d);value='gh'+'p_'+'A'*36
            (root/'README.md').write_text(value)
            result=scan(root)
            self.assertFalse(result['passed'])
            self.assertIn('github_token',[x['reason'] for x in result['issues']])
            self.assertNotIn(value,json.dumps(result))
    def test_raw_history_excluded_and_large_candidate_blocked(self):
        with tempfile.TemporaryDirectory(dir=ROOT/'build') as d:
            root=pathlib.Path(d);(root/'build').mkdir();(root/'evidence').mkdir()
            (root/'build/raw.json.gz').write_bytes(b'private runtime data')
            (root/'evidence/local.log').write_text('local history')
            (root/'README.md').write_bytes(b'x'*(5*1024**2+1))
            self.assertEqual([p.name for p in candidates(root)],['README.md'])
            self.assertIn('file_above_5_MiB',[x['reason'] for x in scan(root)['issues']])
    def test_symlink_candidate_rejected(self):
        with tempfile.TemporaryDirectory(dir=ROOT/'build') as d:
            root=pathlib.Path(d);(root/'outside.txt').write_text('not to be copied')
            (root/'README.md').symlink_to(root/'outside.txt')
            self.assertIn('symlink_or_escape',[x['reason'] for x in scan(root)['issues']])
            (root/'private').mkdir();(root/'private/hidden.md').write_text('not a candidate')
            (root/'docs').symlink_to(root/'private',target_is_directory=True)
            result=scan(root)
            self.assertTrue(any(x['path']=='docs/hidden.md' for x in result['issues']))
            self.assertFalse(any(x['path']=='docs/hidden.md' for x in result['files']))
if __name__=='__main__':unittest.main()
