import pandas as pds
import numpy as np
import math
import flask
import Functions
import logging

locStateExposureTable = r'static\state_exposure_data.csv'
locVehicleClassTable = r'static\vehicle_class_data.csv'
locRawDataProtection1 = r'static\raw_data_protection1.csv'
locRawDataProtection2 = r'static\raw_data_protection2.csv'
locConstantValuesProtection1 = r'static\values_protection1_data.csv'
locConstantValuesProtection2 = r'static\values_protection2_data.csv'

class Protection:

    def getOutputProtection1(self):
        
        log_format = '%(levelname)s : %(asctime)s : %(message)s'
        logging.basicConfig(level=logging.DEBUG,
                        format=log_format,
                        filename='logPOC.log')
        logger = logging.getLogger()

        logger.info("This is working for Protection1!")

        self.rawData = Functions.GetRawData()     
        self.funcs = Functions.MethodsIntermediateTables()
        self.intermediateTable1 = self.funcs.getIntermediateTable1(
            self.rawData.getRawDataProtection(locRawDataProtection1),
            self.rawData.getLogNormalizationProtectionArray(self.rawData.getConstantValuesProtection(locConstantValuesProtection1)),
            1
        )
        logger.info("Intermediate Table 1 Generated Successfully!")

        self.intermediateTable2 = self.funcs.getIntermediateTable2(self.intermediateTable1,1)
        logger.info("Intermediate Table 2 Generated Successfully!")

        self.intermediateTable3 = self.funcs.getIntermediateTable3(self.intermediateTable1,self.intermediateTable2)
        logger.info("Intermediate Table 3 Generated Successfully!")

        self.exposureArray = [self.getWeightValue("Vehicle1","Violation"),self.getWeightValue("Vehicle1","Violation"),
                        self.getWeightValue("Vehicle2","No Fault"),self.getWeightValue("Vehicle2","No Fault"),
                        self.getWeightValue("Vehicle1","No Fault"),self.getWeightValue("Vehicle1","No Fault"),
                        self.getWeightValue("Vehicle2","Violation"),self.getWeightValue("Vehicle2","Violation")]

        self.intermediateTable4 = self.funcs.getIntermediateTable4(self.intermediateTable3,self.exposureArray,1)
        logger.info("Intermediate Table 4 Generated Successfully!")

        self.outputProtection1 = self.funcs.finalTableProtection(self.intermediateTable4)
        logger.info("Protection 1 Output Generated Successfully!")

        return self.outputProtection1

    def getOutputProtection2(self):
        
        log_format = '%(levelname)s : %(asctime)s : %(message)s'
        logging.basicConfig(level=logging.DEBUG,
                        format=log_format,
                        filename='logPOC.log')
        logger = logging.getLogger()

        logger.info("This is working for Protection1!")

        self.rawData = Functions.GetRawData()     
        self.funcs = Functions.MethodsIntermediateTables()
        self.intermediateTable1 = self.funcs.getIntermediateTable1(
            self.rawData.getRawDataProtection(locRawDataProtection2),
            self.rawData.getLogNormalizationProtectionArray(self.rawData.getConstantValuesProtection(locConstantValuesProtection2)),
            2
        )
        logger.info("Intermediate Table 1 Generated Successfully!")

        self.intermediateTable2 = self.funcs.getIntermediateTable2(self.intermediateTable1,2)
        logger.info("Intermediate Table 2 Generated Successfully!")

        self.intermediateTable3 = self.funcs.getIntermediateTable3(self.intermediateTable1,self.intermediateTable2)
        logger.info("Intermediate Table 3 Generated Successfully!")

        self.exposureArray=[self.rawData.getStateExposureTable().iloc[1,3],self.rawData.getStateExposureTable().iloc[1,3],
                            self.rawData.getStateExposureTable().iloc[0,3],self.rawData.getStateExposureTable().iloc[0,3],
                            self.rawData.getStateExposureTable().iloc[3,3],self.rawData.getStateExposureTable().iloc[3,3],
                            self.rawData.getStateExposureTable().iloc[2,3],self.rawData.getStateExposureTable().iloc[2,3],
                            self.rawData.getStateExposureTable().iloc[4,3],self.rawData.getStateExposureTable().iloc[4,3],
                            self.rawData.getStateExposureTable().iloc[5,3],self.rawData.getStateExposureTable().iloc[5,3]]
      
        self.intermediateTable4 = self.funcs.getIntermediateTable4(self.intermediateTable3,self.exposureArray,2)
        logger.info("Intermediate Table 4 Generated Successfully!")

        self.outputProtection2 = self.funcs.finalTableProtection(self.intermediateTable4)
        logger.info("Protection 1 Output Generated Successfully!")

        return self.outputProtection2


    def getWeightValue(self,vehicleClass,stateType):
        return (((self.rawData.getVehicleTypeWeightTable().loc[(self.rawData.getVehicleTypeWeightTable()['Vehicle Class'] == vehicleClass) &
        (self.rawData.getVehicleTypeWeightTable()['Type']==stateType)])['Weight']).to_numpy())[0]
