import json,pathlib,sys,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from m3.control import run
class M3GateTests(unittest.TestCase):
    def test_partial_range_rejected_before_any_side_effect(self):
        with self.assertRaisesRegex(ValueError,'seven complete'):
            run('2024-01-01','2024-01-06')
    def test_missing_day_blocks_before_spark(self):
        with patch('m3.control.read',side_effect=FileNotFoundError('missing day')),patch('m3.control.subprocess.Popen') as launch:
            with self.assertRaises(FileNotFoundError):run('2024-01-01','2024-01-07')
            launch.assert_not_called()
if __name__=='__main__':unittest.main()
