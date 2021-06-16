import pandas as pds
import numpy as np
import math
import flask

global count
count = 0

locStateExposureTable = r'static\state_exposure_data.csv'
locVehicleClassTable = r'static\vehicle_class_data.csv'
locRawDataProtection1 = r'static\raw_data_protection1.csv'
locRawDataProtection2 = r'static\raw_data_protection2.csv'
locConstantValuesProtection1 = r'static\values_protection1_data.csv'
locConstantValuesProtection2 = r'static\values_protection2_data.csv'

class GetRawData:

    #getStateExposureTableDirectly
    def getStateExposureTable(self): 
       file=(locStateExposureTable)
       self.stateExposureTable = pds.DataFrame(pds.read_csv(file))
       self.stateExposureTable['Exposure Percentage'] = self.stateExposureTable['Exposure Value']/(self.stateExposureTable['Exposure Value'].sum()) * 100
       ##logger.info("State Exposure Table Imported Successfully!")
       return self.stateExposureTable

    #getVehicleClassTableDirectly
    def getVehicleClassTable(self): 
       file=(locVehicleClassTable)
       self.vehicleClassTable = pds.DataFrame(pds.read_csv(file))
       self.vehicleClassTable.columns = ['Vehicle Class','Percentage']
       self.vehicleClassTable['Percentage'] = (self.vehicleClassTable['Percentage'].str.replace('%','')).astype(int)
       ##logger.info("Vehicle Class Table Imported Successfully!")
       return self.vehicleClassTable

    #getStateCountTable
    def getStateCountTable(self):
        self.stateExposureTable = self.getStateExposureTable()
        self.stateCountTable = pds.DataFrame(self.stateExposureTable['Violation/No Fault'])
        self.stateCountTable = self.stateCountTable.groupby(['Violation/No Fault']).size().reset_index(name='Count')
        self.stateCountTable.columns = ['State','Count'] 
        self.stateCountTable['Percentage'] = self.stateCountTable['Count']/(self.stateCountTable['Count'].sum()) * 100
        ##logger.info("State Count Table Imported Successfully!")
        return self.stateCountTable

    #getVehicleTypeWeightTable 
    def getVehicleTypeWeightTable(self):
        self.vctable = self.getVehicleClassTable()
        self.sctable = self.getStateCountTable()
        temp = {'Vehicle Class':['Vehicle1','Vehicle2','Vehicle1','Vehicle2'],
                'Type':['Violation','Violation','No Fault','No Fault',],
                'Weight':[ (round(self.vctable.loc[self.vctable['Vehicle Class'] == "Vehicle1" , 'Percentage'].item()) * round(self.sctable.loc[self.sctable['State']=="Violation",'Percentage'].item())/ 100),
                            (round(self.vctable.loc[self.vctable['Vehicle Class'] == "Vehicle2" , 'Percentage'].item()) * round(self.sctable.loc[self.sctable['State']=="Violation",'Percentage'].item())/ 100),
                            (round(self.vctable.loc[self.vctable['Vehicle Class'] == "Vehicle1" , 'Percentage'].item()) * round(self.sctable.loc[self.sctable['State']=="No Fault",'Percentage'].item())/ 100),
                            (round(self.vctable.loc[self.vctable['Vehicle Class'] == "Vehicle2" , 'Percentage'].item()) * round(self.sctable.loc[self.sctable['State']=="No Fault",'Percentage'].item())/ 100),
                        ]}
        self.vehicleTypeWeightTable = pds.DataFrame(temp)
        ##logger.info("Vehicle Type Weight Table Imported Successfully!")
        return self.vehicleTypeWeightTable

    #getRawDataProtectionUsingFileLocation
    def getRawDataProtection(self,fileLocation):
        file=fileLocation
        ##logger.info("Raw Data Protection Table Imported Successfully!")
        return pds.DataFrame(pds.read_csv(file)) 

    #getConstantValuesProtectionUsingFileLocation
    def getConstantValuesProtection(self,fileLocation):
        file=fileLocation
        ##logger.info("Constant Values Table Imported Successfully!")
        return pds.DataFrame(pds.read_csv(file)) 

    #getLogNormalizationProtectionArray
    def getLogNormalizationProtectionArray(self,constantValuesTable):
        
        self.constantValuesTable = constantValuesTable
        retention = (self.constantValuesTable.to_numpy())[0]
        retention = np.delete(retention,0)
        retention = retention.astype(np.float64)

        mu = (self.constantValuesTable.to_numpy())[1]
        mu = np.delete(mu,0)
        mu = mu.astype(np.float64)

        sigma = (self.constantValuesTable.to_numpy())[2]
        sigma = np.delete(sigma,0)
        sigma = sigma.astype(np.float64)

        maxClaim = (self.constantValuesTable.to_numpy())[3]
        maxClaim = np.delete(maxClaim,0)
        maxClaim = maxClaim.astype(np.float64)

        log1 = (sigma*maxClaim*0.5)*(1/(maxClaim*math.sqrt(2*3.14159265)*sigma)*np.exp(-(((np.log(maxClaim)-mu)**2)/(2*(sigma**2)))))
        log2 = (sigma*retention*0.5)*(1/(retention*math.sqrt(2*3.14159265)*sigma)*np.exp(-(((np.log(retention)-mu)**2)/(2*(sigma**2)))))
        ##logger.info("Log Normalization Array Generated Successfully!")
        return log2/log1    

class MethodsIntermediateTables:

    def getIntermediateTable1(self,rawDataProtectionTable,logNormalizationProtectionArray,protection):
        self.rawDataProtectionTable = rawDataProtectionTable
        self.logNormalizationProtectionArray = logNormalizationProtectionArray
        if(protection==1):
            self.intTable1 = self.rawDataProtectionTable.drop([0,1,2])
        if(protection==2):
            self.intTable1 = self.rawDataProtectionTable.drop([0,1])
            
        self.intTable1 = self.intTable1.iloc[:,1:]
        self.intTable1 = self.intTable1.astype(np.float64)
        self.intTable1 = np.apply_along_axis(self.funIntermediateTable1,1,np.array(self.intTable1),self.logNormalizationProtectionArray)
        self.intTable1 = pds.DataFrame(self.intTable1)
        if(protection==2):
            self.intTable1.insert(loc=0, column='Months', value=np.arange(12,31,1))
        if(protection==1):
            self.intTable1.insert(loc=0, column='Months', value=np.arange(15,31,1))
        
        ##logger.info("Intermediate Table 1 Generated Successfully!")
        
        return self.intTable1

    def funIntermediateTable1(self,x,logNormalizationProtectionArray):
        self.logNormalizationProtectionArray = logNormalizationProtectionArray
        return ((x-1)*self.logNormalizationProtectionArray)+1

    def getIntermediateTable2(self,intermediateTable1,protection):
        self.intTable1 = intermediateTable1
        global count
        count = 0

        if(protection==1):
            self.intTable2 = pds.DataFrame(np.zeros([14,9]))
            self.intTable2.iloc[:,0] = np.arange(1,15,1)
            self.firstRowIntTable1 = self.intTable1.iloc[0,1:]
            self.twelthRowIntTable1 = self.intTable1.iloc[12,1:]
            self.intTable2 = pds.DataFrame(np.apply_along_axis(self.funIntermediateTable2P1,1,np.array(self.intTable2.iloc[:,1:]),self.firstRowIntTable1,self.twelthRowIntTable1))
            self.intTable2.insert(loc=0, column='Months', value=np.arange(1,15,1))

            ##logger.info("Intermediate Table 2 Generated Successfully!")

            return (self.intTable2)

        if(protection==2):
            self.intTable2 = pds.DataFrame(np.zeros([11,13]))
            self.intTable2.iloc[:,0] = np.arange(1,12,1)
            self.firstRowIntTable1= self.intTable1.iloc[0,1:]
            self.twelthRowIntTable1 = self.intTable1.iloc[12,1:]
            self.intTable2 = pds.DataFrame(np.apply_along_axis(self.funIntermediateTable2P2,1,np.array(self.intTable2.iloc[:,1:]),self.firstRowIntTable1,self.twelthRowIntTable1))
            self.intTable2.insert(loc=0, column='Months', value=np.arange(1,12,1))

            ##logger.info("Intermediate Table 2 Generated Successfully!")

            return self.intTable2

    def funIntermediateTable2P1(self,x,firstRowIntTable1,twelthRowIntTable1):
        self.firstRowIntTable1=firstRowIntTable1
        self.twelthRowIntTable1 = twelthRowIntTable1
        
        global count
        count+=1

        return self.firstRowIntTable1**((np.log(self.firstRowIntTable1)/np.log(self.twelthRowIntTable1))**((15-count)/(27-15)))*(15/count)

    def funIntermediateTable2P2(self,x,firstRowIntTable1,twelthRowIntTable1):
        self.firstRowIntTable1=firstRowIntTable1
        self.twelthRowIntTable1 = twelthRowIntTable1
        global count
        count+=1        
        return self.firstRowIntTable1**((np.log(self.firstRowIntTable1)/np.log(self.twelthRowIntTable1))**((12-count)/(24-12)))*(12/count)

    def getIntermediateTable3(self,intermediateTable1,intermediateTable2):
        self.intTable1 = intermediateTable1
        self.intTable2 = intermediateTable2
        self.intTable3 = pds.concat([self.intTable2,self.intTable1])
        
        #logger.info("Intermediate Table 3 Generated Successfully!")

        return self.intTable3

    def getIntermediateTable4(self,intermediateTable3,exposureArray,protection):
        self.exposureArray = exposureArray
        self.intTable3 = intermediateTable3
        self.intTable4 = self.intTable3.iloc[:,1:]
        self.intTable4 = pds.DataFrame(np.apply_along_axis(self.funIntermediateTable4,1,self    .intTable4,self.exposureArray))
        
        #logger.info("Intermediate Table 4 Generated Successfully!")

        return self.intTable4

    def funIntermediateTable4(self,x,exposureArray):
        self.exposureArray = exposureArray
        return (self.exposureArray/x)*0.01
    
    def finalTableProtection(self,intermediateTable4):
        self.intTable4 = intermediateTable4
        
        self.finalIncurred = self.intTable4.iloc[:,0::2].sum(axis=1)
        self.finalIncurred = 1/self.finalIncurred
        
        self.finalPaid = self.intTable4.iloc[:,1::2].sum(axis=1)
        self.finalPaid = 1/self.finalPaid

        self.finalTable = pds.DataFrame({'Months':np.arange(1,31,1,dtype=int),
                                        'Final Incurred':self.finalIncurred,
                                        'Final Paid':self.finalPaid})
        
        #logger.info("Final Output Table Generated Successfully!")

        return self.finalTable
        

