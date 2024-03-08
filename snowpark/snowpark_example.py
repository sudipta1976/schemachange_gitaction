from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import snowflake.snowpark.functions as f
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')
# https://medium.com/snowflake/your-cheatsheet-to-snowflake-snowpark-dataframes-using-python-e5ec8709d5d7
def get_session()-> Session:
    connection_parameters = {
    "account": "PAB39776.us-east-1",
    "user": "ssarkar",
    "password": "Tcs@12345",
    "database":"DEMO",
    "schema": "DT_DEMO"
    }
    ss = Session.builder.configs(connection_parameters).create()
    return ss

def main():
    session = get_session()
    session.sql('use role accountadmin')
    # session.sql('use database DEMO')
    # session.sql('use schema DT_DEMO')
    session.sql('use warehouse COMPUTE_WH')
    # logging.INFO('set the context')

    df = session.table('CUST_INFO')
    print(f'Record count -> {df}')
    # df.select(col("CNAME")).show()
    df.filter(col("CNAME")=='Scott Reilly').select('CUSTID').show()
    df.agg(f.sum('CUSTID')).show()
    df.agg(f.min('CUSTid').alias('TEST'),f.max('custid').as_("TEST2")).show()
if __name__=="__main__":
    main()
