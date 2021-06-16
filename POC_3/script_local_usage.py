import pandas as pd
import datetime as dt
import json
from dateutil.relativedelta import relativedelta 


def getLastMonthDate(stock_date):
    return pd.to_datetime(stock_date.date() - relativedelta(months=1))

def getLastMonthRecordDate(pdf,stock_date):
    if(getLastMonthDate(stock_date) >= pdf.iloc[0]['stock_date']):
            dff = pdf[pdf['stock_date'] <= getLastMonthDate(stock_date)]
            return dff.iloc[-1]['stock_date']
    else:
        return dt.datetime(2000,1,1)

def getStockPriceDropRatioMonth(pdf,stock_date):

    stock_price_c2 = ((pdf.loc[pdf.index[pdf['stock_date'] == stock_date]])['stock_price']).tolist()
    stock_price_c3 = ((pdf.loc[pdf.index[pdf['stock_date'] == getLastMonthRecordDate(pdf,stock_date)]])['stock_price']).tolist() 
        
    if not stock_price_c3:
        return 0
    else:
        val1 = stock_price_c2[0]
        val2 = stock_price_c3[0]

        return (val1 - val2)*100/val2

def getLargestDropRatioOneDay(df):
    return df.loc[df['drop_ratio_1_day'].idxmax()]['stock_date'],df.loc[df['drop_ratio_1_day'].idxmax()]['stock_price']

def getLargestDropRatioOneMonth(df):
    return df.loc[df['drop_ratio_1_month'].idxmax()]['stock_date'],df.loc[df['drop_ratio_1_month'].idxmax()]['stock_price']

def processData(df):
    
    #convert to datetime
    df['stock_date']= pd.to_datetime(df['stock_date'])
    
    #remove weekends data
    df = df[df['stock_date'].dt.dayofweek < 5]
    
    #calculate drop ratio for 1 day
    df['drop_ratio_1_day'] = df['stock_price'].pct_change()*100

    #calculate drop ratio for 1 month
    df['drop_ratio_1_month'] = df['stock_date'].apply(lambda x: getStockPriceDropRatioMonth(df,x))

    #fill null values with 0
    df=df.fillna(0)

    return df

data1 = pd.read_csv("BHARTIARTL.csv")
dataAll = pd.read_csv("stocks_data.csv")

grouped = dataAll.groupby(dataAll['company_name'])
for i in grouped:
    processDataFrame = processData(i[1])
    
    max1DayDropDate,max1DayDropPrice = getLargestDropRatioOneDay(processDataFrame)
    max1MonthDropDate,max1MonthDropPrice = getLargestDropRatioOneMonth(processDataFrame)

    jsonFile = {
        "Company_Name" : i[0] ,
        "Largest_1day_stock_drop_price" : max1DayDropPrice,
        "Largest_1day_stock_drop_date" : max1DayDropDate.date().strftime("%d-%m-%Y"),
        "Largest_1month_stock_drop_price" : max1MonthDropPrice,
        "Largest_1month_stock_drop_date" : max1MonthDropDate.date().strftime("%d-%m-%Y"),
        "Stock_details" : json.loads(processDataFrame[['stock_date','stock_price','drop_ratio_1_day','drop_ratio_1_month']].to_json(orient='records'))
    }

   
    json_object = json.dumps(jsonFile,indent=2)
    with open(i[0]+".json", "w") as outfile:
        outfile.write(json_object)
