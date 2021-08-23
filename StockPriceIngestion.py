import json
import boto3
import pandas as pd
import yfinance as yf
import time
import random
import datetime

if __name__ == '__main__':

    # User Input area
    shard_count = 1
    kinesis_client = boto3.client('kinesis', region_name ='us-east-1')
    start_date = "2021-03-30"
    end_date = "2021-04-06"
    list_stocks = ["MSFT", "MVIS", "GOOG", "SPOT", "INO", "OCGN", "ABML", "RLLCF", "JNJ", "PSFE"]

    fiftyTwoWk_highs_for_stocks = dict()
    fiftyTwoWk_lows_for_stocks = dict()

    # Get the all time highs and put that in a dict
    print ("Getting 52w highs and 52w lows for all stocks. This may take a min ....\n")
    for specific_stock in list_stocks:
        specific_stock_info = yf.Ticker(specific_stock)
        fiftyTwoWk_highs_for_stocks[specific_stock] = specific_stock_info.info['fiftyTwoWeekHigh']
        fiftyTwoWk_lows_for_stocks[specific_stock] = specific_stock_info.info['fiftyTwoWeekLow']

    print ("52wk highs\n", fiftyTwoWk_highs_for_stocks)
    print ("52wk lows\n", fiftyTwoWk_lows_for_stocks)

    # Get stock prices of stocks in "list_stocks" list from given start and end date
    print("Extracting prices of all stocks for an interval of 1h between start date", start_date,
          "and end date", end_date)
    stock_prices_all_data = yf.download(list_stocks, start=start_date, end=end_date, interval="1h")
    df1 = pd.DataFrame(stock_prices_all_data)
    df1.to_csv('stockprices.csv')

    # get only the closing price (pandas command)
    stock_prices_closing_data = stock_prices_all_data['Close']

    # Using Pandas command to get the length of the number of rows
    kinesis_dict = dict()
    for timestamp_index in range(len(stock_prices_closing_data.index)):
        # Converts the row data into dict, each stock and its closing price as the key value pair
        stock_prices_closing_data_timestamped = stock_prices_closing_data.iloc[timestamp_index].to_dict()

        # Extracts the current timestamp of the row for further usage
        stock_prices_closing_data_onlyTs = stock_prices_closing_data.index[timestamp_index]
        stock_prices_closing_data_onlyTs = str(stock_prices_closing_data.index[timestamp_index].to_pydatetime())

        # Dict update to send to kinesis
        kinesis_dict['timestamped_data'] = stock_prices_closing_data_timestamped
        kinesis_dict['fiftyTwoWk_high_data'] = fiftyTwoWk_highs_for_stocks
        kinesis_dict['fiftyTwoWk_low_data'] = fiftyTwoWk_lows_for_stocks
        print("\ndata to send to kinesis \n=======\n",
              "\ntimestamp = ", stock_prices_closing_data_onlyTs,
              "\ndata=",json.dumps(kinesis_dict))

        # Actual sending of data to kinesis
        kinesis_client.put_record(StreamName='test_kinesis1',
                                  Data=json.dumps(kinesis_dict),
                                  PartitionKey=stock_prices_closing_data_onlyTs)

        # Adding a sleep so that lambda pulls records properly.
        time.sleep(1)