# static variable file #
from config import *
from funcs import retry, dict_fill_na, dict_replace_quote
from MSDS_upsert import *
import MSDS_upsert
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

#### install modules ####
import xmltodict
import requests
# import cx_Oracle #pip install cx_oracle

import oracledb
import pprint
import pandas as pd

import calendar
import time
import os
import numpy as np

import msds_history

basic_col=["PLANT","DOC_NO","MSDS_NO","DEPT_NM","USE_PROCESS","MATERIAL_NM","USAGE","MONTHLY_AMOUNT",
           "DAILY_AMOUNT","RIVISION_DATE","DISPOSAL_DATE","START_DATE","INTF_DATE","APPR_DATE","APPLY_FLAG","ERROR_MSG",
           "MANUFACTURER_NAME", "MANUFACTURER_TEL", "SUPPLIER_NAME", "SUPPLIER_TEL"
        ]



#############################################

def extract_numbers_and_hyphens(text): # 한글, 영어, 숫자, 하이픈 추출 정규식
    return ''.join(re.findall(r'[A-Za-z0-9가-힣-]+', text))


def get_MSDS_CHEMICAL_SUBSTANCE_ID(cas_no:str): # msds_chemical_substance테이블에 해당 화학물질 있는지 확인/ 있으면 id반환, 없으면 다음id 받아서 반환

    chemical_substance_id_t =(None,None)
    flag=None

    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()

        find_exist_query=f"SELECT CHEMICAL_SUBSTANCE_ID,CHEM_NAME,EN_NO,KE_NO FROM MSDS_CHEMICAL_SUBSTANCE WHERE CAS_NO='{cas_no}'"
        cur.execute(find_exist_query)
        result=cur.fetchall()

        if result: #기존데이터있음 update, 기존데이터 substanceid 넘겨줌
            chemical_substance_id_t= ("U",result[0][0],result[0][1],result[0][2],result[0][3],"C")
            flag="00"

        else: # cas_no로 검색안되면 CAS_NO에 KE_NO가 들어갈 수 도 있으니 KE_NO에서 검색
            find_exist_KE_query=f"SELECT CHEMICAL_SUBSTANCE_ID,CHEM_NAME,EN_NO,KE_NO FROM MSDS_CHEMICAL_SUBSTANCE WHERE KE_NO='{cas_no}'"
            cur.execute(find_exist_KE_query)
            result_ke=cur.fetchall()
            
            if result_ke:
                chemical_substance_id_t= ("U",result[0][0],result[0][1],result[0][2],result[0][3],"K")
                flag="00"
            else:
                # insert, 다음 substanceid 넘겨줌
                # get_id_query=f"SELECT SEQ_MSDS_CHEMICAL_SUBSTANCE.NEXTVAL FROM DUAL"
                # cur.execute(get_id_query)
                # result=cur.fetchall()

                # if result:
                #     chemical_substance_id_t= ("I",result[0][0]) if result else ""
                #     flag="00"

                chemical_substance_id_t= ("I","","","","","C")
                flag="00"


    except Exception as ora_e:
        if oracleDB:
            oracleDB.rollback()
        return chemical_substance_id_t if chemical_substance_id_t else ("","","","","",""),"83",Exception(f"{ora_e}")
    
    finally:
        if oracleDB:
            oracleDB.close()

    return chemical_substance_id_t if chemical_substance_id_t else ("","","","","",""),flag,""



def update_flag(doc_no,apply_flag,errmsg,err_cas):
       
    upate_flag_data=[apply_flag,errmsg,err_cas,doc_no]
    flag="99"

    apply_flag_query=f"""
            UPDATE INTF_TARGET_MSDS_BASIC SET
                    APPLY_FLAG=:1
                    , ERROR_MSG=:2
                    , ERROR_CASNO=:3
            WHERE DOC_NO=:4
            """
    
    try:
        flag,oracleDB=get_db_conn()
        if flag!="00":
            return flag, IF_ERROR_CODE[flag]
        
        cur = oracleDB.cursor()

        cur.execute(apply_flag_query,upate_flag_data)
        oracleDB.commit()
        oracleDB.close()
        flag="00"

    except oracledb.Error as errmsg:
        if oracleDB:
            oracleDB.rollback()
        flag="82"
        return flag,Exception(f"{errmsg}")

    return flag,errmsg


def del_storage(atch_list):
    # if os.path.isfile(r'.\webapps\upload\em\1717575047.pdf'):
    for file in atch_list:
        if os.path.isfile(file[0]):
            os.remove(file[0])


def storage_save(nm,atch,tp,siz):  # 파일 하나 스토리지에 저장 FUNC
    offset=1
    try:
        whole_path=f"{PATH}\{str(calendar.timegm(time.gmtime()))}.{tp}"
        with open(whole_path, "wb") as f:
            while True:
                data = atch.read(offset, siz)
                if data:
                    f.write(data)
                if len(data) < siz:
                    break
                offset += len(data)

        time.sleep(1) #이름 중복 피하기 위함

    except Exception as e:
        print(e)
        # oracleDB.rollback()
        flag="30"
        return "",flag,e
       
    finally:
        return whole_path,"00",""


#############################################

def check_condition(row):
    #################### query ##################

    check_exist_date=f"""SELECT APPR_DATE,DISPOSAL_DATE FROM INTF_TARGET_MSDS_BASIC WHERE DOC_NO='{row['DOC_NO']}'"""
    
    # check_target_id_query=f"""
    #     SELECT TARGET_MATERIAL_ID FROM MSDS_MATERIAL_TARGET WHERE MATERIAL_ID='{row['DOC_NO']}'
    # """

    get_cas_query=f"SELECT * FROM INTF_TARGET_MSDS_CAS WHERE DOC_NO='{row['DOC_NO']}'"

    find_seq_query=f"""SELECT SEQ 
                            FROM ( 
                            SELECT SEQ 
                            FROM INTF_TARGET_MSDS_BASIC_HIS 
                            WHERE DOC_NO = '{row["DOC_NO"]}'
                            ORDER BY seq DESC 
                            ) 
                            WHERE ROWNUM = 1
                        """
    
    get_dup_query=f"""SELECT GET_DUP_INTF('{row["DOC_NO"]}') FROM DUAL"""

    #############################################

    data=dict()
    data["CAS_LIST"]=list()
    errmsg=""
    y_errmsg=""
    flag=None
    data["DUP_FLAG"]="N"
    data["UIflag"]=""
    err_cas=""

    try:
        flag,oracleDB=get_db_conn()
        if flag!="00":
            errmsg=IF_ERROR_CODE[flag]
            raise

        cur = oracleDB.cursor()
        row=dict_replace_quote_2(row.to_dict())


        #### check 결제일자 존재 여부, 폐기 일자 존재 여부 ###

        cur.execute(check_exist_date)
        res_exist_date=cur.fetchone()
        res_exist_appr = res_exist_date[0] if res_exist_date else None #appr_date없으면 E / NOT_EXIST_APPR_DATE
        data["USE_YN"] = 0 if res_exist_date[1] else 1     # disposal_date 있으면 폐기 => USE_YN = 0
                                                            #               없으면 사용중 => USE_YN = 1 
        
        if not res_exist_appr:
            flag="88"
            errmsg=IF_ERROR_CODE[flag]
            raise


        #### check dup 제품 ###

        cur.execute(get_dup_query)
        res_dup=cur.fetchone()
        dup_doc_no = res_dup[0] if res_dup else '' #doc_no 넘어오면 해당 doc_no로 데이터 update, '-'은 그냥 return, null은 그냥 insert
        # print("-----",row["DOC_NO"],dup_doc_no)
        #### get_target_para ####

        if row['DOC_NO'].find("-")>0:
            plant=row['DOC_NO'][:row['DOC_NO'].find("-")]
            # doc_no_num=row['DOC_NO'][row['DOC_NO'].find("-")+1:]
            
            if not plant:
                flag="85"
                errmsg=IF_ERROR_CODE[flag]
                raise
        else:
            flag="85"
            errmsg=IF_ERROR_CODE[flag]
            raise


        if dup_doc_no=='-': #그냥 넘어가기
            #flag변경 data
            # 필요없음 HIS남기고 끝
            data["UIflag"]='R' #RETURN
            # print(data)

            return "00",IF_ERROR_CODE["01"],None,data,err_cas
        else:
            #### get his table seq ####
        
            cur.execute(find_seq_query)
            res_his_seq=cur.fetchone()

            data["SEQ"]=int(res_his_seq[0])+1 if res_his_seq else 0

            if dup_doc_no==None: # insert
                data["UIflag"]='I'

                ####################################################
                seq_id_query=f"""SELECT SEQ_MSDS_MATERIAL_TARGET.NEXTVAL AS TARGET_MATERIAL_ID 
                    , SEQ_MSDS_MATERIAL_USEPLANT.NEXTVAL AS CHEMICAL_USEPLANT_ID FROM DUAL"""
                
                cur.execute(seq_id_query)
                res_seq_id=cur.fetchone()

                data["TARGET_MATERIAL_ID"]=res_seq_id[0] if res_seq_id else ""
                data["CHEMICAL_USEPLANT_ID"]=res_seq_id[1] if res_seq_id else ""


                ####################################################

                plant_para_insert_ver_query=f"""SELECT 
                    PLANT_PRCS_ID 
                    , PLANT_PRCS_LVL2 
                    , PLANT_PRCS_LVL3 
                    ,DEPT_CODE  
                    FROM MSDS_PLANTMAP 
                    WHERE EPM_PLANT='{plant}'  
                    AND EPM_PLANT_LVL2='{row['DEPT_NM']}'
                    AND EPM_PLANT_LVL3='{row['USE_PROCESS']}'
                    ORDER BY PLANT_PRCS_ID,PLANT_PRCS_LVL2,PLANT_PRCS_LVL3"""


                cur.execute(plant_para_insert_ver_query)
                res_plant_para=cur.fetchall()

                if res_plant_para:
                    if len(res_plant_para)>1:
                        data["DUP_FLAG"]="Y"
                    
                    data["PRCS_ID"],data["PRCS_LVL2"],data["PRCS_LVL3"],data["DEPT_CODE"]=res_plant_para[0]

                else: #MSDS_PLANTMAP에 코드 없음
                    y_errmsg=IF_ERROR_CODE["39"]

                    #plant_prcs_id만 넣기
                    find_plant_prcs_id_query=f"""SELECT 
                        PLANT_PRCS_ID 
                        FROM MSDS_PLANTMAP 
                        WHERE EPM_PLANT='{plant}'  
                        ORDER BY PLANT_PRCS_ID,PLANT_PRCS_LVL2,PLANT_PRCS_LVL3"""

                    cur.execute(find_plant_prcs_id_query)
                    plant_prcs_id=cur.fetchone()

                    if plant_prcs_id:
                        plant_prcs_id=plant_prcs_id[0] if plant_prcs_id else ''

                        data["PRCS_ID"]=plant_prcs_id
                        data["PRCS_LVL2"]=''
                        data["PRCS_LVL3"]=''
                        data["DEPT_CODE"]=''

                    else: #plant를 찾을 수 없음
                        errmsg=IF_ERROR_CODE["38"]
                        raise
                    
        
            else: # 674를 할때 dup_doc_no='Yeongju-000636' / dup_doc_no가 기존에 있던거,
                data["UIflag"]="U"
                data["BASIC_DOC_NO"]=dup_doc_no

                find_dup_target_id_query=f"""
                    SELECT TARGET_MATERIAL_ID FROM MSDS_MATERIAL_TARGET WHERE MATERIAL_ID='{dup_doc_no}'
                """
                cur.execute(find_dup_target_id_query)
                dup_target_id=cur.fetchone()

                if not dup_target_id:
                    errmsg=IF_ERROR_CODE["86"]
                    raise

                dup_target_id=dup_target_id[0] if dup_target_id else ''
                
                data["TARGET_MATERIAL_ID"]= dup_target_id
                data["CHEMICAL_USEPLANT_ID"]=None
                

                # useplant는 수정x / target데이터 update를 위해 prcs_id 조회
                plant_para_update_ver_query=f"""SELECT 
                    PLANT_PRCS_ID
                    FROM MSDS_MATERIAL_USEPLANT 
                    WHERE TARGET_MATERIAL_ID={dup_target_id}"""

                cur.execute(plant_para_update_ver_query)
                res_prcs_id=cur.fetchone()

                if not res_prcs_id: #
                    errmsg=IF_ERROR_CODE["86"]
                    raise
                
                data["PRCS_ID"]=res_prcs_id[0] if res_prcs_id else ""


        #### get_cas_list ####

        cur.execute(get_cas_query)
        res_cas_list=cur.fetchall()

        if res_cas_list:
            cas_df = pd.DataFrame(res_cas_list,columns=["DOC_NO","CAS_NO","KOREAN_NM","ENGLISH_NM","CONTENT","INTF_DATE","CONTENT_NM"])
            cas_df['EXT_CAS_NO'] = cas_df['CAS_NO'].apply(extract_numbers_and_hyphens)
            cas_df=cas_df.drop_duplicates(["EXT_CAS_NO"])
                      
            secret_cas_cnt=999990
            for idx, cas_row in cas_df.iterrows(): #화학제품에 포함된 물질들 dataframe
                
               # 1. msds_chemical_substance upsert // 기본물질데이터 추가 필요하기 때문
                if re.compile('[ㄱ-ㅎ가-힣]+').findall(cas_row["EXT_CAS_NO"]) or cas_row["EXT_CAS_NO"]=='-':  #한글포함 ex)영업비밀, 영업비밀1, 미기재, '-'
                    (IU_sep, chemsubid,chem_name,en_no,ke_no,CorK),flag,errmsg=get_MSDS_CHEMICAL_SUBSTANCE_ID(secret_cas_cnt)
                    if flag !="00":
                        raise
                    msds_upsert_res=(chemsubid, "영업비밀",secret_cas_cnt,"영업비밀","영업비밀",cas_row["CONTENT"]) #chemical_substance_id는 99990~99999
                    secret_cas_cnt+=1
                else:
                    (IU_sep, chemsubid,chem_name,en_no,ke_no,CorK),flag,errmsg=get_MSDS_CHEMICAL_SUBSTANCE_ID(cas_row["EXT_CAS_NO"])
                    if flag !="00":
                        raise

                    if CorK=="C":#cas_row["CAS_NO"]가 casno인경우
                        MSDS_upsert_res = MSDS_upsert.msds({"casno": cas_row["EXT_CAS_NO"],"enno":en_no,"keno":ke_no,"cud":IU_sep, "chemical_substance_id":chemsubid,"reg_user":"API_EPM"})
                    elif CorK=="K":# cas_row["CAS_NO"]가 keno인경우 
                        MSDS_upsert_res = MSDS_upsert.msds({"casno":"","enno":"","keno":cas_row["EXT_CAS_NO"],"cud":IU_sep, "chemical_substance_id":chemsubid,"reg_user":"API_EPM"})
                    else:
                        raise
                    if MSDS_upsert_res["resultCode"] !="00":
                        flag=MSDS_upsert_res["resultCode"]
                        errmsg=MSDS_upsert_res["resultMsg"]
                        if MSDS_upsert_res["resultCode"]=="81":
                            err_cas=cas_row["CAS_NO"]
                        raise

                    if chemsubid:
                        if chemsubid < 100000:
                            msds_upsert_res=(int(chemsubid), chem_name, cas_row["EXT_CAS_NO"], ke_no,"","")
                        else:
                            msds_upsert_res=(MSDS_upsert_res["resultData"]["CHEMICAL_SUBSTANCE_ID"], MSDS_upsert_res["resultData"]["CHEM_NAME"],cas_row["EXT_CAS_NO"],MSDS_upsert_res["resultData"]["KE_NO"],MSDS_upsert_res["resultData"]["NICKNAME"],cas_row["CONTENT"])
                    
                    # else: # 수기입력은 제외 (수기입력은 chemsubid 100000 미만)
                    #     msds_upsert_res=(int(chemsubid), chem_name, cas_row["EXT_CAS_NO"], ke_no,"","")

                    
                
                # if msds_upsert_res not in data["CAS_LIST"]:
                #     data["CAS_LIST"].append(msds_upsert_res)
                data["CAS_LIST"].append(msds_upsert_res)

        flag="00"

        
    except oracledb.Error as dberror:
        print(dberror)
        errmsg=dberror if not errmsg else errmsg

        if oracleDB:
            oracleDB.rollback()
        
        y_errmsg=None


    except Exception as ex:
        print(ex)

        errmsg=IF_ERROR_CODE[flag] if flag!="00" else ex 
        if oracleDB:
            oracleDB.rollback()

        y_errmsg=None

    return flag,errmsg,y_errmsg,data,err_cas
    

def insert_INTF_data(row,data):
    flag=None
    errmsg=""

    try:
        
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()

        #####################################################

        target_data=[data["TARGET_MATERIAL_ID"],row["DOC_NO"],data["PRCS_ID"],data["USE_YN"],row['MATERIAL_NM'] if row['MATERIAL_NM'] else '',row['MSDS_NO'] if row['MSDS_NO'] else '',row['USAGE'] if row['USAGE'] else '',row['MANUFACTURER_NAME'] if row['MANUFACTURER_NAME'] else '',row['MANUFACTURER_TEL'] if row['MANUFACTURER_TEL'] else '',row['SUPPLIER_NAME'] if row['SUPPLIER_NAME'] else '',row['SUPPLIER_TEL'] if row['SUPPLIER_TEL'] else '']
        
        target_query= """INSERT INTO MSDS_MATERIAL_TARGET 
                        (TARGET_MATERIAL_ID, MATERIAL_ID, PLANT_PRCS_ID, USE_YN, MATERIAL_NM, CAS_NO, NOTE, REG_DATE, REG_USER, UPT_DATE, UPT_USER, MANUFACTURER_NAME, MANUFACTURER_TEL, SUPPLIER_NAME, SUPPLIER_TEL)
                    VALUES(:1,:2,:3,:4,:5,:6,:7,SYSDATE,'API_EPM',SYSDATE,'API_EPM',:8,:9,:10,:11)
                """
        #####################################################
        
        useplant_data=[data["CHEMICAL_USEPLANT_ID"],data["TARGET_MATERIAL_ID"],data["PRCS_ID"],data["PRCS_LVL2"],data["PRCS_LVL3"],row['USAGE'],data["DEPT_CODE"],data["DUP_FLAG"],'Y']
        # start_date=str(row["START_DATE"].strftime("%Y-%m-%d")) if not pd.isnull(row["START_DATE"]) else ''
        start_date=str(row["START_DATE"].strftime("%Y-%m-%d")) if row["START_DATE"] !='' else ''
        # disposal_date=str(row["DISPOSAL_DATE"].strftime("%Y-%m-%d")) if not pd.isnull(row["START_DATE"]) else ''
        disposal_date=str(row["DISPOSAL_DATE"].strftime("%Y-%m-%d")) if row["DISPOSAL_DATE"] !='' else ''


        useplant_query= f"""INSERT INTO MSDS_MATERIAL_USEPLANT
                        (CHEMICAL_USEPLANT_ID,
                        TARGET_MATERIAL_ID,
                        PLANT_PRCS_ID,
                        PLANT_PRCS_LVL2,
                        PLANT_PRCS_LVL3,
                        USAGE,
                        DEPT_CODE,
                        START_DATE,
                        END_DATE,
                        REG_USER,
                        REG_DEPT,
                        REG_DATE,
                        DUP_FLAG,
                        USE_YN
                        )
                    VALUES(:1,:2,:3,:4,:5,:6,:7,
                        NVL(TO_DATE('{start_date}','YYYY-MM-DD'),''),
                        NVL(TO_DATE('{disposal_date}','YYYY-MM-DD'),''),
                        'API_EPM','API_EPM',SYSDATE,:8,:9)
                """

        #####################################################

        material_sub_query= """INSERT INTO MSDS_MATERIAL_SUBSTANCE 
                            (TARGET_MATERIAL_ID, CHEMICAL_SUBSTANCE_ID, MATERIAL_ID, PLANT_PRCS_ID, MATERIAL_NM, CAS_NO, KE_NO, NICKNAME, RATE)
                            VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9)"""
        
        #####################################################

        get_atch_query=f"SELECT FILE_NM, ATCH_FILE,ATCH_FILE_TYPE, ATCH_FILE_SIZE FROM INTF_TARGET_MSDS_ATCH WHERE DOC_NO='{row['DOC_NO']}' ORDER BY ATCH_FILE_NO"

        #####################################################

        get_atch_id_query=f"SELECT SEQ_CMT_ATCH_MTRL.NEXTVAL FROM DUAL"

        insert_mtrl_query= f"""INSERT INTO CMT_ATCH_MTRL (ATCH_MTRL_ID, REF_TABLE_NAME, REF_TABLE_ID, REF_NAME, REF_PATH, REGDATE, FILE_GUBUN, WRT_EMPCD, WRT_EMPNM)
                VALUES(:1,'MSDS_MATERIAL_MSDS',:2,:3,:4,SYSDATE,:5,'MSDS_EPM_BATCH','MSDS_EPM_BATCH')
            """

        #####################################################

        
        #### target table save ####
        cur.execute(target_query,target_data)

        #### useplant table save ####
        cur.execute(useplant_query,useplant_data)

        #### msds_material_substance table save ####
        if data["CAS_LIST"]:
            for chemsubid, chem_name, cas_no, ke_no, nickname,content in data["CAS_LIST"]:
                material_sub_data=[data["TARGET_MATERIAL_ID"],chemsubid,chemsubid,data["PRCS_ID"],chem_name,cas_no,ke_no,nickname,content]
                cur.execute(material_sub_query,material_sub_data)

        #### storage save ####
        cur.execute(get_atch_query)
        res_get_atch_data = cur.fetchall()

        if res_get_atch_data:
            for idx,(nm,atch,tp,siz) in enumerate(res_get_atch_data):
                whole_path,flag,errmsg=storage_save(nm,atch,tp,siz)
                if flag!="00":
                    raise
                
                cur.execute(get_atch_id_query) # SEQ
                res_mtrl_id=cur.fetchone()
                atch_mtrl_id= res_mtrl_id[0] if res_mtrl_id else ""

                # CMT_ATHC_MTRL SAVE
                mtrl_data=[int(atch_mtrl_id),data["TARGET_MATERIAL_ID"],nm,whole_path,idx+1]
                cur.execute(insert_mtrl_query,mtrl_data)

        oracleDB.commit()
        flag="00"

    except oracledb.Error as dberror:
        print(dberror)
        errmsg=dberror

        if oracleDB:
            oracleDB.rollback()

    except Exception as ex:
        print(ex)
        errmsg=ex

        if oracleDB:
            oracleDB.rollback()

    finally:
        oracleDB.close()
    
    return flag,errmsg
    

def update_INTF_data(row,data):
    flag=None
    errmsg=""

    try:
        
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()

        #####################################################

        del_sub_query=f"DELETE FROM MSDS_MATERIAL_SUBSTANCE WHERE TARGET_MATERIAL_ID={data['TARGET_MATERIAL_ID']}"

        # del_atch_query=f"DELETE FROM CMT_ATCH_MTRL WHERE REF_TABLE_ID={data['TARGET_MATERIAL_ID']} AND REF_TABLE_NAME='MSDS_MATERIAL_MSDS'"

        material_sub_query= """INSERT INTO MSDS_MATERIAL_SUBSTANCE 
                            (TARGET_MATERIAL_ID, CHEMICAL_SUBSTANCE_ID, PLANT_PRCS_ID, MATERIAL_NM, CAS_NO, KE_NO, NICKNAME, RATE)
                            VALUES(:1,:2,:3,:4,:5,:6,:7,:8)"""

        get_atch_query=f"SELECT FILE_NM, ATCH_FILE,ATCH_FILE_TYPE, ATCH_FILE_SIZE FROM INTF_TARGET_MSDS_ATCH WHERE DOC_NO='{row['DOC_NO']}' ORDER BY ATCH_FILE_NO"


        insert_mtrl_query= f"""INSERT INTO CMT_ATCH_MTRL (ATCH_MTRL_ID, REF_TABLE_NAME, REF_TABLE_ID, REF_NAME, REF_PATH, REGDATE, FILE_GUBUN, WRT_EMPCD, WRT_EMPNM)
                VALUES(:1,'MSDS_MATERIAL_MSDS',:2,:3,:4,SYSDATE,:5,'MSDS_EPM_BATCH','MSDS_EPM_BATCH')
            """

        get_atch_id_query=f"SELECT SEQ_CMT_ATCH_MTRL.NEXTVAL FROM DUAL"

                
        find_same_atch_query=f"""SELECT REF_PATH FROM CMT_ATCH_MTRL WHERE REF_TABLE_NAME='MSDS_MATERIAL_MSDS' AND REF_TABLE_ID = {data['TARGET_MATERIAL_ID']}"""


        # find_file_path_query=f"SELECT REF_PATH FROM CMT_ATCH_MTRL WHERE REF_TABLE_ID={data['TARGET_MATERIAL_ID']}"


        target_data=[row["DOC_NO"],data["PRCS_ID"],row['MATERIAL_NM'] if row['MATERIAL_NM'] else '',row['MSDS_NO'] if row['MSDS_NO'] else '',row['USAGE'] if row['USAGE'] else '',row['MANUFACTURER_NAME'] if row['MANUFACTURER_NAME'] else '',row['MANUFACTURER_TEL'] if row['MANUFACTURER_TEL'] else '',row['SUPPLIER_NAME'] if row['SUPPLIER_NAME'] else '',row['SUPPLIER_TEL'] if row['SUPPLIER_TEL'] else '',data['USE_YN'] if data['USE_YN'] else '']
        
        target_query=f"""
            UPDATE MSDS_MATERIAL_TARGET SET
                    MATERIAL_ID=:1
                    , PLANT_PRCS_ID=:2
                    , MATERIAL_NM=:3
                    , CAS_NO=:4
                    , NOTE=:5
                    , UPT_DATE=SYSDATE
                    , UPT_USER='API_EPM'
                    , MANUFACTURER_NAME=:6
                    , MANUFACTURER_TEL=:7
                    , SUPPLIER_NAME=:8
                    , SUPPLIER_TEL=:9
                    , USE_YN=:10
            WHERE MATERIAL_ID='{data["BASIC_DOC_NO"]}'
            """
                

        # useplant는 업데이트 x
        # useplant_data=[data["PRCS_ID"],data["PRCS_LVL2"],data["PRCS_LVL3"],row['USAGE'],data["DEPT_CODE"]]

        # start_date=str(row["START_DATE"].strftime("%Y-%m-%d")) if not pd.isnull(row["START_DATE"]) else ''
        # disposal_date=str(row["DISPOSAL_DATE"].strftime("%Y-%m-%d")) if not pd.isnull(row["DISPOSAL_DATE"]) else ''

        # useplant_query=f"""
        #     UPDATE MSDS_MATERIAL_USEPLANT SET
        #             PLANT_PRCS_ID=:1
        #             , PLANT_PRCS_LVL2=:2
        #             , PLANT_PRCS_LVL3=:3
        #             , USAGE=:4
        #             , DEPT_CODE=:5
        #             , START_DATE=NVL(TO_DATE('{start_date}','YYYY-MM-DD'),'')
        #             , END_DATE=NVL(TO_DATE('{disposal_date}','YYYY-MM-DD'),'')
        #             , UPT_USER='API_EPM'
        #             , UPT_DEPT='API_EPM'
        #             , UPT_DATE=SYSDATE
        #     WHERE TARGET_MATERIAL_ID='{data["TARGET_MATERIAL_ID"]}'
        #     """


        #####################################################
        #####################################################

        
        #### update target table ####
        cur.execute(target_query,target_data)

        #### update useplant table ####
        # cur.execute(useplant_query,useplant_data)

        #### msds_material_substance table delete, save ####
        cur.execute(del_sub_query)
        if data["CAS_LIST"]:
            for chemsubid, chem_name, cas_no, ke_no, nickname, content in data["CAS_LIST"]:
                material_sub_data=[data["TARGET_MATERIAL_ID"],chemsubid,data["PRCS_ID"],chem_name,cas_no,ke_no,nickname,content]
                cur.execute(material_sub_query,material_sub_data)

        #### storage delete, save ####
        
        # cur.execute(find_file_path_query)
        # atch_list=cur.fetchall()

        # if atch_list: #DB상에서는 삭제 # storage 삭제는 rollback이 안되므로 맨 마지막에 실행
        #     cur.execute(del_atch_query)

        cur.execute(get_atch_query)
        res_get_atch_data = cur.fetchall()

        if res_get_atch_data:
            for idx,(nm,atch,tp,siz) in enumerate(res_get_atch_data):

                # atch중복인지 판단후 save
                cur.execute(find_same_atch_query) # 그 화학제품에 포함되어있는 첨부파일 모두 가져오기
                res_ref_path=cur.fetchall()
                res_ref_path_list=[i[0] for i in res_ref_path]
                same_file=[path for path in res_ref_path_list if os.path.getsize(path)==siz]

                if same_file: #중복으로 pass
                    pass
                else:
                    whole_path,flag,errmsg=storage_save(nm,atch,tp,siz) #서버에 진짜 파일 저장
                    if flag!="00":
                        raise
                    
                    cur.execute(get_atch_id_query) # SEQ
                    res_mtrl_id=cur.fetchone()
                    atch_mtrl_id= res_mtrl_id[0] if res_mtrl_id else ""

                    # CMT_ATHC_MTRL SAVE
                    mtrl_data=[int(atch_mtrl_id),data["TARGET_MATERIAL_ID"],nm,whole_path,idx+1]
                    cur.execute(insert_mtrl_query,mtrl_data)


        oracleDB.commit()
        flag="00"

    except oracledb.Error as dberror:
        print(dberror)
        errmsg=dberror

        if oracleDB:
            oracleDB.rollback()

    except Exception as ex:
        print(ex)
        errmsg=ex

        if oracleDB:
            oracleDB.rollback()

    finally:
        oracleDB.close()
    
    # return flag,errmsg,atch_list
    return flag,errmsg

#############################################

def dict_replace_quote_2(data):
    """
    dictionary type data에서
    key : str 데이터의 '데이터를 ''로 변경
    sql query에서 str안의 '은 인식x
    """
    for k, v in data.items():
        if isinstance(v, dict):
            dict_replace_quote(v)
        elif isinstance(v, list):
            if v:
                for d in v:
                    dict_replace_quote(d)
            # else:
            #     data[k] = value
        elif isinstance(v, str):
            if "'" in v or not bool(v):
                v = v.replace("'","''")
                data[k] = v
        elif isinstance(v, int):
            pass
        elif isinstance(v, float):
            pass

    
    return data



def get_epm_func():
    success_list=[]
    error_list=[]

    FULL_TO_SAVE={}

    # INTF_TARGET_MSDS_BASIC의 APPLY_FLAG가 N이거나 NULL인 경우 select
    batch_query=f"""SELECT A.* 
         FROM ( 
                  SELECT PLANT	                                --PLANT
                       , DOC_NO	                                --MSDS 문서번호
                       , MSDS_NO                                --MSDS 고유번호
                       , DEPT_NM                                --사용부서명
                       , USE_PROCESS                            --사용공정 
                       , MATERIAL_NM                            --물질명
                       , USAGE                                 --사용용도
                       , MONTHLY_AMOUNT                         --월취급량 
                       , DAILY_AMOUNT                           --일취급량
                       , RIVISION_DATE                          --개정일자
                       , DISPOSAL_DATE                          --폐기일자 
                       , START_DATE                             --취급시작일 
                       , INTF_DATE                              --인터페이스일시
                       , APPR_DATE                              --결재완료된 마지막버전 문서의 결재완료일
                       , APPLY_FLAG                             --업무테이블 적용여부
                       , ERROR_MSG
                       , MANUFACTURER_NAME                      --제조회사명
                       , MANUFACTURER_TEL                       --제조회사연락처
                       , SUPPLIER_NAME                          --공급업체명
                       , SUPPLIER_TEL                           --공급업체연락처

                       , ROW_NUMBER() OVER (ORDER BY INTF_DATE ASC) AS RN 
                       , TO_CHAR(INTF_DATE, 'YYYY-MM-DD HH24:MI:SS') AS UPT 
                    FROM INTF_TARGET_MSDS_BASIC 
                   WHERE (APPLY_FLAG is NULL or APPLY_FLAG = 'N') AND APPR_DATE is not null
                ORDER BY RN ASC 
              ) A 
     ORDER BY A.RN
                """
    
    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()

        cur.execute(batch_query)
        result=cur.fetchall()
        oracleDB.close()

        df= pd.DataFrame()
        success_list=list()
        error_list=list()
        FULL_TO_SAVE=dict()

        if result:

            # print(result)
            df = pd.DataFrame(result,columns=basic_col+["RN","UPT"])
            df.replace({pd.NaT: None}, inplace=True)
            df=df.fillna('')

            # df=df.iloc[:2]

            for idx, row in tqdm(df.iterrows()):
                errmsg=""
                apply_flag=None
                err_cas=""
                try:
                    flag,oracleDB=get_db_conn()
                    if flag !="00":
                        raise
                    cur = oracleDB.cursor()

                    # row=dict_replace_quote_2(row.to_dict())

                    flag,errmsg,y_errmsg,data,err_cas=check_condition(row) #조건 안맞아서 E반환하고 끝
                    if flag !="00":
                        raise
                    
                    if data['UIflag']=='I':
                        flag,errmsg=insert_INTF_data(row,data)
                        if flag !="00":
                            raise
                    elif data['UIflag']=='U':
                        # flag,errmsg,atch_list=update_INTF_data(row,data)
                        flag,errmsg=update_INTF_data(row,data)
                        if flag !="00":
                            raise
                    elif data['UIflag']=='R':
                        flag="00"
                        pass
                    else:
                        raise
                        

                except Exception as ex:
                    print("ERROR_ROW : ",row["DOC_NO"])
                    print()
                    apply_flag="E"
                    # errmsg_plantmap=None
                    error_list.append(row["DOC_NO"])

                    # ####
                    # log_err=errmsg if errmsg else ""
                    # log_file=log_file+f"{row['DOC_NO']} {ex} {log_err}".format('\n')
                    # ####

                    if oracleDB:
                        oracleDB.rollback()

                    errmsg= ex if not errmsg else errmsg
                    # print(ex)
                    # print(errmsg)
                    # print("---")
                    
                finally:
                    try:
                        apply_flag="Y" if not apply_flag else "E"

                        seq,flag,errmsg2=msds_history.get_his_data(row["DOC_NO"])
                        if flag !="00":
                            print("get_his_data",flag,errmsg2)
                            raise
                            
                        errmsg= y_errmsg if not errmsg else errmsg

                        flag,errmsg2=update_flag(row["DOC_NO"],apply_flag,errmsg,err_cas) #INTF 테이블에 flag update
                        if flag !="00":
                            print("FLAG UPDATE",flag,errmsg2)
                            raise

                        flag,errmsg2=msds_history.put_INTF_HIS(seq,row["DOC_NO"]) #인터페이스 개정이력 남기기
                        if flag !="00":
                            print("put_INTF_HIS",flag,errmsg2)
                            raise
                        
                        # if data['UIflag']=='U':
                        #     if atch_list: # 기존 storage atch 파일 삭제
                        #         del_storage(atch_list)
                                

                        if apply_flag=="Y":
                            success_list.append(row["DOC_NO"])
                            
                    except Exception as ex:
                        # ####
                        # happy=ex
                        
                        print(ex,errmsg2)
                        print("ERROR_ROW2 : ",row["DOC_NO"])
                        print()
                        errmsg2= ex if not errmsg2 else errmsg2
                        if oracleDB:
                            oracleDB.rollback()
                    finally:
                        FULL_TO_SAVE[row["DOC_NO"]]=[apply_flag,errmsg]
                        # #####
                        # log_err2=errmsg if errmsg else ""
                        # log_err3=errmsg2 if errmsg2 else ""
                        # happy=happy if happy else ""
                        
                        # log_file=log_file+f"{row['DOC_NO']} {log_err2} {log_err3} {happy}".format('\n')
                        # #####

                        


    except Exception as ex:
        print(ex)
        if oracleDB:
            oracleDB.rollback()

    
    # ####
    # return "00",len(df),len(success_list),success_list,error_list,FULL_TO_SAVE,log_file
    return "00",len(df),len(success_list),success_list,error_list,FULL_TO_SAVE

#############################################
def msds_epm(para):

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

            if keycode=="edf7f1aa5a3e4f1ddd01fde29707b37e6618918a536f5779f61aef7c2abfd34c":
                # ####
                # flag,total,success_count,success_list,error_list,FULL_TO_SAVE,log_file=get_epm_func()
                flag,total,success_count,success_list,error_list,FULL_TO_SAVE=get_epm_func()
                if flag !="00":
                    raise
                

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
                detail["resultData"] = f"총 {total}개 시도, 성공: {success_count}개 {success_list} 실패: {error_list} {FULL_TO_SAVE}".replace("'","")

                insert_substance_if("-","-","-",flag,IF_ERROR_CODE[flag],detail,"EPM_BATCH_LOG")


        except Exception as errmsg:
            print(errmsg)

            detail["exec_time"]=f"{time.time() - start} / 시작시간: {start_time} , 종료시간: {end_time}"
            detail["resultCode"] = flag
            detail["resultMsg"] = errmsg
            detail["resultDate"] = datetime.now().strftime(DATE_FORMAT)
            detail["resultData"] = None
            # ####
            # detail["resultData"] = log_file

            insert_substance_if("-","-","-",flag,IF_ERROR_CODE[flag],detail,"EPM_BATCH_LOG")


        finally:
            # ####
            # log_file=open("240724_epm_log.txt","w")
            # log_file.write(log_file)
            # log_file.close()



            return detail
        

if __name__=="__main__":

    keycode="edf7f1aa5a3e4f1ddd01fde29707b37e6618918a536f5779f61aef7c2abfd34c"

    para ={"keycode":keycode}
    result = msds_epm(para)


    print(result)