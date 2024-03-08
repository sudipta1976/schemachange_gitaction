CREATE OR REPLACE PROCEDURE DEMO.PUBLIC.snowpark_sp()
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
comment = 'snowflake as sandbox for snowpark'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import snowflake.snowpark.functions as f

def main(session: snowpark.Session):
    df = session.table('DEMO.DT_DEMO.CUST_INFO')
    agg_df = df.agg(f.min('CUSTid').alias('TEST'),f.max('custid').as_("TEST2"))
    return df
'
