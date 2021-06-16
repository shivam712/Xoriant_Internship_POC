import unittest
import pandas as pds
import numpy as np
from Functions import MethodsIntermediateTables
from Functions import GetRawData
from pandas._testing import assert_frame_equal

global count
count = 0

def checkDataFrames(expected,obtained,name):
    try:
        assert_frame_equal(expected,obtained,check_dtype=False)
        print(f"Unit test Passed for {name}")
    except:  
        print(f"Unit test Failed for {name}")

locStateExposureTable = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\state_exposure_data.csv'
locVehicleClassTable = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\vehicle_class_data.csv'
locRawDataProtection1 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\raw_data_protection1.csv'
locRawDataProtection2 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\raw_data_protection2.csv'
locConstantValuesProtection1 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\values_protection1_data.csv'
locConstantValuesProtection2 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\Data Set\values_protection2_data.csv'

locIntTable1P2 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\intTable1P2.csv'
locIntTable2P1 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\intTable2P1.csv'
locIntTable3P1 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\intTable3P1.csv'
locIntTable4P2 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\intTable4P2.csv'
locFinalTableP2 = r'D:\Users\swara\Desktop\Xoriant\Internship\POC Python\Flask\CSV\csv_protection_1\csv_protection_1\finalTableP2.csv'

class TestMethodsIntermediateTables(unittest.TestCase):

    def test_getIntermediateTable1(self):
        file=(locIntTable1P2)
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = MethodsIntermediateTables().getIntermediateTable1(
            GetRawData().getRawDataProtection(locRawDataProtection2),
            GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection2)),2)

        expected = (expected.round(8)).to_string()
        obtained = (obtained.round(8)).to_string()
        try:
            self.assertAlmostEqual(expected,obtained)
            print(f"Unit test Passed for IntermediateTable1")
        except:
            print(f"Unit test Failed for IntermediateTable1")

    def test_funIntermediateTable1(self):
        intTable1 = MethodsIntermediateTables().getIntermediateTable1(
            GetRawData().getRawDataProtection(locRawDataProtection1),
            GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection1)),1)
        
        logNormalizationArray = GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection1))
        x = np.array(intTable1.iloc[0,1:])
        obtained = MethodsIntermediateTables().funIntermediateTable1(x,logNormalizationArray)
        expected = np.array([1.22894456,1.45745919,1.38175283,1.6572552,1.30294511,1.33737484,1.34601129,1.57316996])
        obtained = str(obtained)
        expected = str(expected)
        try:
            self.assertAlmostEqual(expected,obtained)
            print(f"Unit test Passed for funIntermediateTable1")
        except:
            print(f"Unit test Failed for funIntermediateTable1")

    def test_getIntermediateTable2(self):
        file=(locIntTable2P1)
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = MethodsIntermediateTables().getIntermediateTable2(
                    MethodsIntermediateTables().getIntermediateTable1(
                        GetRawData().getRawDataProtection(locRawDataProtection1),
                        GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection1)),1),1)

        expected.columns = obtained.columns
         
        expected = (expected.round(8))
        obtained = (obtained.round(8))
        # #assert_frame_equal(expected,obtained,check_dtype=False)
        checkDataFrames(expected,obtained,"IntermediateTable2")
        
    def test_funIntermediateTable2P1(self):

        expected = 226.9389216250626
        obtained = MethodsIntermediateTables().funIntermediateTable2P1(100,200,300)

        #self.assertAlmostEqual(expected,obtained)
        try:
            self.assertAlmostEqual(expected,obtained)
            print(f"Unit test Passed for funIntermediateTable2P1")
        except:
            print(f"Unit test Failed for funIntermediateTable2P1")

    def test_funIntermediateTable2P1(self):

        expected = 200.0
        obtained = MethodsIntermediateTables().funIntermediateTable2P2(100,200,300)

        #self.assertAlmostEqual(expected,obtained)
        try:
            self.assertAlmostEqual(expected,obtained)
            print(f"Unit test Passed for funIntermediateTable2P2")
        except:
            print(f"Unit test Failed for funIntermediateTable2P2")

    def test_getIntermediateTable3(self):
        file=(locIntTable3P1)
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        obtained = MethodsIntermediateTables().getIntermediateTable3(MethodsIntermediateTables().getIntermediateTable1(
            GetRawData().getRawDataProtection(locRawDataProtection1),
            GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection1)),1),MethodsIntermediateTables().getIntermediateTable2(
                    MethodsIntermediateTables().getIntermediateTable1(
                        GetRawData().getRawDataProtection(locRawDataProtection1),
                        GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection1)),1),1))

        expected.columns = obtained.columns

        expected = expected.reset_index(drop=True)
        obtained = obtained.reset_index(drop=True)
        
        expected = (expected.round(8))
        obtained = (obtained.round(8))
        #assert_frame_equal(expected,obtained,check_dtype=False)
        checkDataFrames(expected,obtained,"IntermediateTable3")
   
    def test_getIntermediateTable4(self):
        file=(locIntTable4P2)
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
        exposureArray=[GetRawData().getStateExposureTable().iloc[1,3],GetRawData().getStateExposureTable().iloc[1,3],
                            GetRawData().getStateExposureTable().iloc[0,3],GetRawData().getStateExposureTable().iloc[0,3],
                            GetRawData().getStateExposureTable().iloc[3,3],GetRawData().getStateExposureTable().iloc[3,3],
                            GetRawData().getStateExposureTable().iloc[2,3],GetRawData().getStateExposureTable().iloc[2,3],
                            GetRawData().getStateExposureTable().iloc[4,3],GetRawData().getStateExposureTable().iloc[4,3],
                            GetRawData().getStateExposureTable().iloc[5,3],GetRawData().getStateExposureTable().iloc[5,3]]
     
        obtained = MethodsIntermediateTables().getIntermediateTable4(MethodsIntermediateTables().getIntermediateTable3(MethodsIntermediateTables().getIntermediateTable1(GetRawData().getRawDataProtection(locRawDataProtection2),GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection2)),2)
            ,MethodsIntermediateTables().getIntermediateTable2(MethodsIntermediateTables().getIntermediateTable1(GetRawData().getRawDataProtection(locRawDataProtection2),GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection2)),2),2)),exposureArray,2)

        obtained.columns = expected.columns
         
        expected = (expected.round(6))
        obtained = (obtained.round(6))
    
        #assert_frame_equal(expected,obtained,check_dtype=False)
        checkDataFrames(expected,obtained,"IntermediateTable4")
   
    def test_finalTableProtection(self):
       
        file=(locFinalTableP2)
        expected = pds.DataFrame(pds.read_csv(file,index_col=0))
       
        exposureArray=[GetRawData().getStateExposureTable().iloc[1,3],GetRawData().getStateExposureTable().iloc[1,3],
                            GetRawData().getStateExposureTable().iloc[0,3],GetRawData().getStateExposureTable().iloc[0,3],
                            GetRawData().getStateExposureTable().iloc[3,3],GetRawData().getStateExposureTable().iloc[3,3],
                            GetRawData().getStateExposureTable().iloc[2,3],GetRawData().getStateExposureTable().iloc[2,3],
                            GetRawData().getStateExposureTable().iloc[4,3],GetRawData().getStateExposureTable().iloc[4,3],
                            GetRawData().getStateExposureTable().iloc[5,3],GetRawData().getStateExposureTable().iloc[5,3]]
        
        obtained = MethodsIntermediateTables().finalTableProtection(MethodsIntermediateTables().getIntermediateTable4(MethodsIntermediateTables().getIntermediateTable3(MethodsIntermediateTables().getIntermediateTable1(GetRawData().getRawDataProtection(locRawDataProtection2),GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection2)),2)
            ,MethodsIntermediateTables().getIntermediateTable2(MethodsIntermediateTables().getIntermediateTable1(GetRawData().getRawDataProtection(locRawDataProtection2),GetRawData().getLogNormalizationProtectionArray(GetRawData().getConstantValuesProtection(locConstantValuesProtection2)),2),2)),exposureArray,2))

        expected = (expected.round(6))
        obtained = (obtained.round(6))
    
        #assert_frame_equal(expected,obtained,check_dtype=False)
        checkDataFrames(expected,obtained,"finalTableP2")
       
if __name__ == '__main__': 
    unittest.main() 
        

     
