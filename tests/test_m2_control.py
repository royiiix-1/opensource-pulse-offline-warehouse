import datetime as dt,unittest
from warehouse.common import dates
from warehouse.operations import request_dates
from warehouse.hdfs import HDFS
class M2ControlTests(unittest.TestCase):
    def test_inclusive_range_and_bounds(self):
        self.assertEqual(dates("2024-01-01","2024-01-03"),["2024-01-01","2024-01-02","2024-01-03"])
        for a,b in [("2024-01-02","2024-01-01"),("2024-01-01","2024-01-09"),("2024-02-30","2024-02-30")]:
            with self.assertRaises(ValueError):dates(a,b)
    def test_daily_interval_is_utc_data_date(self):
        self.assertEqual(request_dates({},dt.datetime(2024,1,1,tzinfo=dt.timezone.utc)),["2024-01-01"])
    def test_repair_hour_validation(self):
        for hour in [-1,24,True,"0"]:
            with self.assertRaises(ValueError):request_dates({"process_date":"2024-01-01","process_hour":hour},None)
    def test_incompatible_inputs_rejected(self):
        with self.assertRaises(ValueError):request_dates({"start_date":"2024-01-01","end_date":"2024-01-02","process_hour":0},None)
    def test_namespace_guard(self):
        for p in ["/other-project/file","/osp-offline/../secret"]:
            with self.assertRaises(ValueError):HDFS().url(p,"OPEN")

class M2IsolationTests(unittest.TestCase):
    def test_date_plans_are_independent(self):
        import json,tempfile,pathlib
        from unittest.mock import patch
        from warehouse.common import ROOT,source_plan
        with tempfile.TemporaryDirectory(dir=ROOT/'build') as tmp, patch('warehouse.common.ROOT',pathlib.Path(tmp)):
            plans=pathlib.Path(tmp)/'config/source-plans';plans.mkdir(parents=True)
            for date in ['2024-01-01','2024-01-02']:
                (plans/(date+'.json')).write_text(json.dumps({'date':date,'all_sizes_known':True}))
            self.assertEqual(source_plan('2024-01-01')['date'],'2024-01-01')
            self.assertEqual(source_plan('2024-01-02')['date'],'2024-01-02')
            with self.assertRaises(ValueError):source_plan('../outside')
    def test_network_ledger_cannot_overdraw(self):
        import json,tempfile,pathlib
        from unittest.mock import patch
        from warehouse.common import ROOT,reserve_network
        with tempfile.TemporaryDirectory(dir=ROOT/'build') as tmp, patch('warehouse.common.ROOT',pathlib.Path(tmp)):
            r=pathlib.Path(tmp);(r/'config').mkdir();(r/'evidence').mkdir()
            (r/'config/m2-budget.json').write_text(json.dumps({'prior_runtime_reservations':1,'prior_source_allowance':0,'network_limit_bytes':2}))
            reserve_network(1,'allowed')
            with self.assertRaises(RuntimeError):reserve_network(1,'denied')
            self.assertEqual(len((r/'evidence/network-m2.jsonl').read_text().splitlines()),1)
    def test_date_lease_blocks_other_run(self):
        import tempfile,pathlib
        from unittest.mock import patch
        from warehouse.common import ROOT
        from warehouse.operations import lease
        with tempfile.TemporaryDirectory(dir=ROOT/'build') as tmp, patch('warehouse.operations.CONTROL',pathlib.Path(tmp)):
            a={'date':'2024-01-01','key':'a'};b={'date':'2024-01-01','key':'b'}
            lease(a)
            with self.assertRaises(RuntimeError):lease(b)
            lease(a,'FAILED');lease(b)