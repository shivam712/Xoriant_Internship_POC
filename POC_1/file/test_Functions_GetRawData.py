import unittest
import pandas as pds
from Functions import GetRawData
from pandas._testing import assert_frame_equal

def checkDataFrames(expected,obtained,name):
    try:
        assert_frame_equal(expected,obtained)
        print(f"Unit test Passed for {name}")
    except:  
        print(f"Unit test Failed for {name}")


class TestGetRawData(unittest.TestCase):

    def test_getStateExposureTable(self):
        file=(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\stateExposureTable.csv')
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = GetRawData().getStateExposureTable()
        checkDataFrames(expected,obtained,"stateExposureTable")
        
    def test_getVehicleClassTable(self):
        file=(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\vehicleClassTable.csv')
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = GetRawData().getVehicleClassTable()
        try:
            assert_frame_equal(expected,obtained,check_dtype=False)
            print(f"Unit test Passed for vehicleClassTable")
        except:
            print(f"Unit test Failed for vehicleClassTable")
        
    def test_getStateCountTable(self):
        file=(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\stateCountTable.csv')
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = GetRawData().getStateCountTable()
        checkDataFrames(expected,obtained,"stateCountTable")
        #assert_frame_equal(expected,obtained)
    
    def test_getVehicleTypeWeightTable(self):
        file=(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\vehicleTypeWeightTable.csv')
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = GetRawData().getVehicleTypeWeightTable()
        checkDataFrames(expected,obtained,"vehicleTypeWeightTable")
        #assert_frame_equal(expected,obtained)
    
    def test_getRawDataProtection(self):
        file=(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\rawDataProtection2.csv')
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = GetRawData().getRawDataProtection(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\raw_data_protection2.csv')
        checkDataFrames(expected,obtained,"rawDataProtection")
        #assert_frame_equal(expected,obtained)
    
    def test_getConstantValuesProtection(self):
        file=(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\constantValuesProtection2.csv')
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = GetRawData().getConstantValuesProtection(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\values_protection2_data.csv')
        obtained.columns = expected.columns
        #assert_frame_equal(expected,obtained)
        checkDataFrames(expected,obtained,"constantValuesProtection")

    def test_getLogNormalizationProtectionArray(self):
        file=(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\logNormalizationTable.csv')
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = pds.DataFrame(GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\values_protection1_data.csv'))) 
        expected.columns = obtained.columns
        #assert_frame_equal(expected,obtained)
        checkDataFrames(expected,obtained,"logNormalizationArray")
        
if __name__ == '__main__': 
    unittest.main() 
        

     
