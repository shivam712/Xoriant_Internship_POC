import boto3
import io
import urllib.parse
import pandas as pd
import pyorc
import datetime as dt
import json
from dateutil.relativedelta import relativedelta 

pd.options.mode.chained_assignment = None

print('Loading function')

s3 = boto3.client('s3')

def load_s3_orc_to_local_df(data):
    orc_bytes = io.BytesIO(data['Body'].read())   
    reader = pyorc.Reader(orc_bytes)
    schema = reader.schema
    columns = [item for item in schema.fields]
    rows = [row for row in reader]   
    df = pd.DataFrame(data=rows, columns=columns)
    return df

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
    df['stock_price'] = df['stock_price'].astype(float)
    df['drop_ratio_1_day'] = df['stock_price'].pct_change()*100
    
    #calculate drop ratio for 1 month
    df['drop_ratio_1_month'] = df['stock_date'].apply(lambda x: getStockPriceDropRatioMonth(df,x))
    
    #fill null values with 0
    df=df.fillna(0)
    
    return df

def lambda_handler(event, context):
    
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        
        dataS3 = load_s3_orc_to_local_df(data=response)

        grouped = dataS3.groupby(dataS3['company_name'])
        for i in grouped:

            print("Now in Process:",i[0])
            
            processDataFrame = processData(i[1])
            
            processDataFrame['stock_date']= processDataFrame['stock_date'].astype(str)
        
            max1DayDropDate,max1DayDropPrice = getLargestDropRatioOneDay(processDataFrame)
            max1MonthDropDate,max1MonthDropPrice = getLargestDropRatioOneMonth(processDataFrame)

            jsonFile = {
                "Company_Name" : i[0] ,
                "Largest_1day_stock_drop_price" : max1DayDropPrice,
                "Largest_1day_stock_drop_date" : max1DayDropDate,
                "Largest_1month_stock_drop_price" : max1MonthDropPrice,
                "Largest_1month_stock_drop_date" : max1MonthDropDate,
                "Stock_details" : json.loads(processDataFrame[['stock_date','stock_price','drop_ratio_1_day','drop_ratio_1_month']].to_json(orient='records'))
            }

            fileName = "Outputs/"+i[0]+'.json'
     
            uploadByteStream = bytes(json.dumps(jsonFile,indent=2).encode('UTF-8'))

            s3.put_object(Bucket=bucket,Key=fileName,Body=uploadByteStream)

            print('Put Complete for',i[0])
        
            
        print("CONTENT TYPE:" + response['ContentType'])
        
        return key

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e



