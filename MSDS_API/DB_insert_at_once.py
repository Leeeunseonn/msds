# pip install fastapi
# pip install uvicorn[standard]

# uvicorn version 0.18.3
# xmltodict version 0.13.0
# requests version 2.28.1
# urllib3 version 1.26.11
# fastapi version 0.85.1

from datetime import datetime
from fastapi import FastAPI
import uvicorn
from config import *
import json

import MSDS_upsert

from fastapi import FastAPI,status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from starlette.responses import Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from MSDS_upsert import *

from all_data import *

from collections import Counter
from tqdm import tqdm

def find_db(cas_list):
    

    result_data=[]
    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()
       
        # find_query=f"""
        #     select chemical_substance_id, cas_no from MSDS_CHEMICAL_SUBSTANCE where cas_no='{0}'
        #     """
        for i in tqdm(cas_list):
            cur.execute(f"select chemical_substance_id, cas_no from MSDS_CHEMICAL_SUBSTANCE where cas_no='{i}'")
            result=cur.fetchall()
            if result:
                result_data=result_data+result
            else:
                result_data=result_data+[(None,i)]
            


        # result_data=[i for i in result_data if i[0]==None] #새로운거만
                
        result_data=sorted(result_data, key=lambda d: d[1])
        
        return result_data

    except oracledb.Error as e:
        print(e)
        if oracleDB:
            oracleDB.rollback()
        
    finally:
        if oracleDB:
            oracleDB.close()


def chemid_params(searchwrd:str,searchcnd:str):
    return {
        "searchWrd": searchwrd,
        "searchCnd": int(searchcnd),
        "ServiceKey": MSDS_API_KEY,
        "numOfRows": 10,
        "pageNo": 1,
    }

def get_chemId_num(casno: str) -> str:
    """
    searchCnd = 0 : 국문명
                1 : CAS No
                2 : UN No
                3 : KE No
                4 : EN No
    해당 함수는 INPUT 값으로 항상 CAN.NO를 받는다고 가정하므로 searchCnd 값은 1로 고정
    값이 있거나 성공하면 dictionary 반환, 실패하거나 값이 없으면 Exception 반환
    """

    url = "http://msds.kosha.or.kr/openapi/service/msdschem/chemlist"
    params = {
        "searchWrd": casno,
        "searchCnd": 1,
        "ServiceKey": MSDS_API_KEY,
        "numOfRows": 30000,
        "pageNo": 1,
    }
    req = requests.get(url, params)

    if 200 <= req.status_code < 300:
        result = json.loads(json.dumps(xmltodict.parse(req.text)))
        # print(result)
        if result.get("response").get("header").get("resultCode") == "00":
            result = result["response"].get("body")
            if set(result.keys()) == set(
                ["items", "totalCount", "pageNo", "numOfRows"]
            ):
                if int(result["totalCount"]) > 1:
                    print(int(result["totalCount"]),casno)    
                    data = [i for i in result["items"]["item"]]
                    if data:
                        return data, "00"
                    else:
                        return Exception(f"not_found_casNo"), "89"
                elif (int(result["totalCount"]) == 1) and (
                    result["items"]["item"]["casNo"]):
                    return result["items"]["item"], "00"
            return Exception(f"not_found_casNo"), "89"
        else:
            return (
                Exception(result.get("response").get("header").get("resultMsg")),
                "91",
            )
    else:
        return Exception(f"{req.status_code}_request_error"), "99"



    # if not res:
    #     return Exception(f"casNo,keNo,enNo not_found_data"), "81",None #casno, keno, enno 셋다에 데이터 없는경우


def get_cas():

    result_data=[]
    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()
        
        get_cas=f"""
            select chemical_substance_id from MSDS_CHEMICAL_SUBSTANCE
            """
        cur.execute(get_cas)
        
        result=cur.fetchall()

        if result:
            cas_list=[i[0] for i in result]

        else:
            cas_list=[]

        
        return cas_list

    except oracledb.Error as e:
        print(e)
        if oracleDB:
            oracleDB.rollback()
        
    finally:
        if oracleDB:
            oracleDB.close()


# def what(cas_list):
#     oracleDB=get_db_conn()
#     cur = oracleDB.cursor()

#     res=[]
#     try:
       
#         get_cas=f"""
#             select cas_no from MSDS_CHEMICAL_SUBSTANCE
#             """
#         cur.execute(get_cas)
        
#         result=cur.fetchall()

#         if result:
#             res=[i[0] for i in result]

#         else:
#             res=[]

#         res1= list(set(res) - set(cas_list))
#         res2 = list(set(cas_list) - set(res))

#         print(res1)

#         print(res2)

        

        
#         return cas_list

#     except oracledb.Error as e:
#         print(e)
#         if oracleDB:
#             oracleDB.rollback()
        
#     finally:
#         if oracleDB:
#             oracleDB.close()



if __name__ =="__main__":

    # table=find_reg_date()

    # data=[]
    # for num in range(10):

    #     a=get_chemId_num(num)

    #     data=data+a[0]
    #     pass


    # data=list(set([i['casNo'] for i in data if i['casNo']]))

    try:

        table = data

        table= list(set([i.strip() for i in table.split('\n') if '<casNo>' in i]))

        table=[i.replace('<casNo>','').replace('</casNo>','').strip() for i in table]
        

        pass
        

        # print(what(table))

        id_table=find_db(table) #kosha에 있는 데이터 전부 20536개-> 하나 중복 20537

        # table = mk_table()

        # print(table)
        # error_list=''

        id_table=id_table[20150:]
        for (id,cas) in tqdm(id_table):
            try: 
                if id:
                    pass
                    # para = {"casno": cas,"enno":"","keno":"","cud":"U", "chemical_substance_id":id}
                    # result = MSDS_upsert.msds(para)
                else:
                    para = {"casno": cas,"enno":"","keno":"","cud":"I", "chemical_substance_id":""}
                    result = MSDS_upsert.msds(para)

            except Exception as ex:
                print(ex, id, cas)
                # error_list=error_list+str(ex)+str(id)+str(cas)+" "
                pass
        
        # file=open("240711_error_list.txt","w")
        # file.write(error_list)
        # file.close()

    except Exception as ex:
        print(ex)
        # pass