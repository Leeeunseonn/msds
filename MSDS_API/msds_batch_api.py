# static variable file #
from config import *
from funcs import retry, dict_fill_na, dict_replace_quote
from MSDS_upsert import *
import MSDS_upsert
from msds_epm_api import *
from setting import *

#### built in modules ####
import requests
from datetime import datetime
import json
from typing import Union
import html
import re
import itertools
from tqdm import tqdm
import time
from time import strftime

#### install modules ####
import xmltodict
import requests
# import cx_Oracle #pip install cx_oracle

import oracledb
import pprint


def batch_func():
    flag,oracleDB=get_db_conn()
    if flag!="00":
        return Exception(IF_ERROR_CODE["83"]), "83"
    cur = oracleDB.cursor()
    result =None
        
    batch_query=f"""SELECT A.* 
         FROM ( 
                  SELECT CHEMICAL_SUBSTANCE_ID	           --화학물질ID 
                       , CAS_NO	                       --CAS NO. 
                       , CHEM_ID                          --화학물질ID 
                       , EN_NO                            --EN No. 
                       , KE_NO                            --KE No. 
                       , ROW_NUMBER() OVER (ORDER BY UPT_DATE ASC) AS RN 
                       , TO_CHAR(UPT_DATE, 'YYYY-MM-DD HH24:MI:SS') AS UPT 
                    FROM MSDS_CHEMICAL_SUBSTANCE 
                   WHERE 1=1 AND ACTV=1 AND CHEMICAL_SUBSTANCE_ID > 99999
                ORDER BY RN ASC 
              ) A 
        WHERE A.RN <= 1000 -- 배치 한번에 1000개씩 연동시.. 
     ORDER BY A.RN """
    

    try:
        cur.execute(batch_query)

        result=cur.fetchall()

        if result:
            # print(result)
            return result if result else [],"00"
    
    except oracledb.Error as ex:
        print(ex)
        if oracleDB:
            oracleDB.rollback()

        return Exception(IF_ERROR_CODE["83"]), "83"
    finally:
        if oracleDB:
            oracleDB.close()

    return Exception(IF_ERROR_CODE["84"]), "84"


def msds_batch(para):

    start = time.time()
    start_time=str(datetime.fromtimestamp(start))

    datalist=None
    success_casno_list,error_casno_list=[],[]
    parameter_dict = para
    detail = {
        "resultCode": None,
        "resultMsg": None,
        "resultDate": datetime.now().strftime(DATE_FORMAT),
        "resultData": None,
    }
    if len(parameter_dict) == 0:
        detail["resultCode"] = "91"
        detail["resultMsg"] = "No parameter"
        return detail
    else:
        chemdetail = dictionary_template()
        detail = {
            "resultCode": None,
            "resultMsg": None,
            "resultDate": None,
            "resultData": None,
        }
        try:
            keycode = parameter_dict["keycode"]

            """
                * API 통신시 SHA256 로 암호화된 값을 keycode로 사용하기로 함 
		        * keycode: edf7f1aa5a3e4f1ddd01fde29707b37e6618918a536f5779f61aef7c2abfd34c   >>> ehs** 
            """	 
            datalist,flag=batch_func()
            if flag != "00":
                raise

            if keycode=="edf7f1aa5a3e4f1ddd01fde29707b37e6618918a536f5779f61aef7c2abfd34c":

                for i,data in tqdm(enumerate(datalist,1)):
                    para = {"casno": data[1],"enno":data[3],"keno":data[4],"cud":"U", "chemical_substance_id":data[0],"reg_user":"API_BATCH"}
                    # print(para)
                    result = MSDS_upsert.msds(para)
                    # print(result)

                    if result['resultCode']!='00':
                        error_casno_list.append(data[1])
                    elif result['resultCode']=='00':
                        success_casno_list.append(data[1])

                    if i==len(datalist):
                        flag="00"
                
            else:#key_error
                flag="71"
                raise

            if flag=="00":
                end = time.time()
                end_time=str(datetime.fromtimestamp(end))

                detail["exec_time"]=f"{time.time() - start} / 시작시간: {start_time} , 종료시간: {end_time}"
                detail["resultCode"] = SUCCESS
                detail["resultMsg"] = SUCCESS_MSG
                detail["resultDate"] = datetime.now().strftime(DATE_FORMAT)
                detail["resultData"] = f"success: {len(success_casno_list)}개 {success_casno_list} error: {len(error_casno_list)}개 {error_casno_list}".replace("'","")

                insert_substance_if("-","-","-",flag,IF_ERROR_CODE[flag],detail,"API_BATCH_LOG")


        except Exception as e:
            print(e)

            end = time.time()
            end_time=str(datetime.fromtimestamp(end))
            
            detail["exec_time"]=f"{time.time() - start} / 시작시간: {start_time} , 종료시간: {end_time}"
            detail["resultCode"] = flag
            detail["resultMsg"] = IF_ERROR_CODE[flag]
            detail["resultDate"] = datetime.now().strftime(DATE_FORMAT)
            detail["resultData"] = []

            if flag not in ["00","83"]:
                insert_substance_if("-","-","-",flag,IF_ERROR_CODE[flag],detail,"API_BATCH_LOG")



        finally:
            return detail
        
