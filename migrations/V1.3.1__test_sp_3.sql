CREATE OR REPLACE PROCEDURE DEMO.PUBLIC.TEST_SP_2()
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
comment = 'Template python code for AMAN'
EXECUTE AS CALLER
AS
$$
def main(ss):
    try:
        df = ss.sql("select * from DT_DEMO.CUST_INFO")
        df.show()
        return df
    except Exception as error:
        print("----> Error")
        raise error
$$    ;