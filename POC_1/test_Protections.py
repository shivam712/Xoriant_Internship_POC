import unittest
import pandas as pds
import numpy as np
from Protections import Protection
from pandas._testing import assert_frame_equal

locOutputProtection1 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\outputProtection1.csv'
locOutputProtection2 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\outputProtection2.csv'

class TestProtection(unittest.TestCase):

    def test_getOutputProtection1(self):
        file=(locOutputProtection1)
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        
        obtained = Protection().getOutputProtection1()
        
        expected = (expected.round(8)).to_string()
        obtained = (obtained.round(8)).to_string()
        try:
            self.assertAlmostEqual(expected,obtained)
            print(f"Unit test Passed for OutputProtection1")
        except:
            print(f"Unit test Failed for OutputProtection1")
   
    def test_getOutputProtection2(self):
        file=(locOutputProtection2)
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        
        obtained = Protection().getOutputProtection2()
        obtained.to_csv(locOutputProtection2, index = False)   
      
        expected = (expected.round(8)).to_string()
        obtained = (obtained.round(8)).to_string()
        try:
            self.assertAlmostEqual(expected,obtained)
            print(f"Unit test Passed for OutputProtection2")
        except:
            print(f"Unit test Failed for OutputProtection2")
   
if __name__ == '__main__': 
    unittest.main() 
        

     
