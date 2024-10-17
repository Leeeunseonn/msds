from fastapi.responses import StreamingResponse
import io
import pandas as pd
import oracledb
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from datetime import datetime
from setting import *

# oracleDB_dsn = "220.73.136.151:1521/orcl"
# oracleDB_dsn = "220.73.136.150:1521/XE"  
# oracleDB_dsn = "192.168.120.160:1521/XE"
# oracleDB_user = "ehs" 
# oracleDB_user = "ASIAEHS"
# oracleDB_passwd = "rltnfdusrnth"

oracleDB_connect_timeout = 3600 


def get_db_conn():
    oracleDB = None
    try:
        oracledb.init_oracle_client(lib_dir=ORACLE_HOME)
        oracleDB = oracledb.connect(user=oracleDB_user, password=oracleDB_passwd, dsn=oracleDB_dsn)
    except Exception as e:
        print(e)
        pass

    return oracleDB

def download_excel():
    stream = io.BytesIO()
    oracleDB=get_db_conn()
    cur = oracleDB.cursor()
    print(f"{datetime.now()}_make_df")
    sql = "SELECT A.CAS_NO,A.CHEM_NAME,A.EN_NO,A.KE_NO,A.NICKNAME,A.FLOW_RATE,\
        (CASE WHEN A.PROHIBITED_SUBSTANCES_YN = '1' THEN 'O' ELSE '' END) AS PROHIBITED_SUBSTANCES_YN,\
        (CASE WHEN A.PERMITTED_SUBSTANCES_YN = '1' THEN 'O' ELSE '' END) AS PERMITTED_SUBSTANCES_YN,\
        (CASE WHEN A.TOXIC_MANAGEMENT_YN = '1' THEN 'O' ELSE '' END) AS TOXIC_MANAGEMENT_YN,\
        (CASE WHEN A.SPECIAL_CARE_YN = '1' THEN 'O' ELSE '' END) AS SPECIAL_CARE_YN,\
        (CASE WHEN A.WORK_ENVIRONMENT_YN = '1' THEN 'O' ELSE '' END) AS WORK_ENVIRONMENT_YN,\
        (CASE WHEN A.SPECIAL_HEALTH_YN = '1' THEN 'O' ELSE '' END) AS SPECIAL_HEALTH_YN,\
        (CASE WHEN A.EXPOSURE_SET_YN = '1' THEN 'O' ELSE '' END) AS EXPOSURE_SET_YN,\
        (CASE WHEN A.PSM_YN = '1' THEN 'O' ELSE '' END) AS PSM_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '1' AND NOTE LIKE '%영업%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS SECRET_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '2' AND NOTE ='-' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS CHEMICAL_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '2' AND NOTE LIKE '%금지%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS FORBIDDEN_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '2' AND NOTE LIKE '%제한%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS RESTRICTION_YN,\
        (CASE WHEN A.ACCEPTABLE_SET_YN = '1' THEN 'O' ELSE '' END) AS ACCEPTABLE_SET_YN,\
        (CASE WHEN A.TOXIC_SUBSTANCES_YN = '1' THEN 'O' ELSE '' END) AS TOXIC_SUBSTANCES_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '2' AND NOTE LIKE '%사고%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS ACCIDENT_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '3' AND NOTE LIKE '%비위험%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS DENGER_YN,\
        (SELECT NOTE FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '3' AND REMARK != '비위험물' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) AS QTY \
        FROM MSDS_CHEMICAL_SUBSTANCE A WHERE A.ACTV = '1' ORDER BY A.CAS_NO"
    
    with cur as connection:
        result = connection.execute(sql)
        v = result.fetchall()
        df = pd.DataFrame(v)
    print(f"{datetime.now()}_end_df")
    book = load_workbook('msds_excel.xlsx')
    print(f"{datetime.now()}_make_excel")
    with pd.ExcelWriter(stream, engine='openpyxl') as writer:
        writer.book = book
        ws = writer.book['sheet']
        for r in dataframe_to_rows(df, index=False, header=False):
            ws.append(r)
    print(f"{datetime.now()}_enc_excel")
    xlsx_data = stream.getvalue()

    file_name = f'{datetime.now()}_msds_full'

    return StreamingResponse(io.BytesIO(xlsx_data), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
                             headers={"Content-Disposition": f'attachment; filename="{file_name}.xlsx"'})

def download_csv():
    oracleDB=get_db_conn()
    cur = oracleDB.cursor()
    print(f'{datetime.now()} start api')
    print(f'{datetime.now()} start make df')
    sql = "SELECT A.CAS_NO,A.CHEM_NAME,A.EN_NO,A.KE_NO,A.NICKNAME,A.FLOW_RATE,\
        (CASE WHEN A.PROHIBITED_SUBSTANCES_YN = '1' THEN 'O' ELSE '' END) AS PROHIBITED_SUBSTANCES_YN,\
        (CASE WHEN A.PERMITTED_SUBSTANCES_YN = '1' THEN 'O' ELSE '' END) AS PERMITTED_SUBSTANCES_YN,\
        (CASE WHEN A.TOXIC_MANAGEMENT_YN = '1' THEN 'O' ELSE '' END) AS TOXIC_MANAGEMENT_YN,\
        (CASE WHEN A.SPECIAL_CARE_YN = '1' THEN 'O' ELSE '' END) AS SPECIAL_CARE_YN,\
        (CASE WHEN A.WORK_ENVIRONMENT_YN = '1' THEN 'O' ELSE '' END) AS WORK_ENVIRONMENT_YN,\
        (CASE WHEN A.SPECIAL_HEALTH_YN = '1' THEN 'O' ELSE '' END) AS SPECIAL_HEALTH_YN,\
        (CASE WHEN A.EXPOSURE_SET_YN = '1' THEN 'O' ELSE '' END) AS EXPOSURE_SET_YN,\
        (CASE WHEN A.PSM_YN = '1' THEN 'O' ELSE '' END) AS PSM_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '1' AND NOTE LIKE '%영업%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS SECRET_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '2' AND NOTE ='-' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS CHEMICAL_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '2' AND NOTE LIKE '%금지%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS FORBIDDEN_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '2' AND NOTE LIKE '%제한%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS RESTRICTION_YN,\
        (CASE WHEN A.ACCEPTABLE_SET_YN = '1' THEN 'O' ELSE '' END) AS ACCEPTABLE_SET_YN,\
        (CASE WHEN A.TOXIC_SUBSTANCES_YN = '1' THEN 'O' ELSE '' END) AS TOXIC_SUBSTANCES_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '2' AND NOTE LIKE '%사고%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS ACCIDENT_YN,\
        (CASE WHEN (SELECT COUNT(*) FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '3' AND NOTE LIKE '%비위험%' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) = 0 THEN '' ELSE 'O' END) AS DENGER_YN,\
        (SELECT NOTE FROM MSDS_CHEMICAL_REGULATION WHERE LAW_CODE = '3' AND REMARK != '비위험물' AND CHEMICAL_SUBSTANCE_ID = A.CHEMICAL_SUBSTANCE_ID) AS QTY \
        FROM MSDS_CHEMICAL_SUBSTANCE A WHERE A.ACTV = '1' ORDER BY A.CAS_NO"
    
    columns = ['CAS No.', '화학물질명', 'EN No.', 'KE No.', '관용명/이명', '함량',
               '산업안전보건법\n금지물질\n여부', 
               '산업안전보건법\n허가물질\n여부',
               '산업안전보건법\n관리대상\n유해물질여부',
               '산업안전보건법\n특별관리물질\n여부',
               '산업안전보건법\n작업환경측정\n대상물질 여부',
               '산업안전보건법\n특수건강진단\n대상물질 여부',
               '산업안전보건법\n노출기준\n설정물질 여부',
               '산업안전보건법\nPSM제출\n대상물질 여부',
               '산업안전보건법\n영업비밀인정\n제외물질여부',
               '화학물질관리법\n기존화학물질\n여부',
               '화학물질관리법\n금지물질\n여부',
               '화학물질관리법\n제한물질여부',
               '화학물질관리법\n허가물질\n여부',
               '화학물질관리법\n유독물질\n여부',
               '화학물질관리법\n사고대비물질\n여부',
               '위험물안전관리법에위한규제\n위험물여부',
               '위험물안전관리법에위한규제\n지정수량'
               ]
    stream =io.StringIO()
    with cur as connection:
        result = connection.execute(sql)
        v = result.fetchall()
        df = pd.DataFrame(v, columns=columns)

    print(f'{datetime.now()} end make df')
    print(f'{datetime.now()} start make excel')
    # df.to_csv('aa.csv', index=False, encoding='utf-8-sig')
    df.to_csv(stream, encoding='utf-8-sig', index=False)
    print(f'{datetime.now()} end make excel')
    stream.seek(0)
    file_name = 'msds_csv'
    print(f'{datetime.now()} start return')
    response = StreamingResponse(iter([stream.getvalue().encode('utf-8-sig')]),
                                 media_type="text/csv; charset=utf-8-sig"
                                )
    response.headers["Content-Disposition"] = f"attachment; filename={file_name}.csv"
    response.headers["Content-Type"] = "application/octet-stream"
    return response