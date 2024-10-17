# static variable file #
from config import *
from funcs import retry, dict_fill_na, dict_replace_quote
from setting import *

from MSDS_upsert import *
import MSDS_upsert

#### built in modules ####
import requests
from datetime import datetime
import json
from typing import Union
import html
import re
import itertools
import pandas as pd

#### install modules ####
import xmltodict
# import cx_Oracle #pip install cx_oracle

import oracledb
import pprint


############################MSDS_REVISION_HISTORY##########################################

def insert_to_table(oracleDB,cur,seq,subid,cas,en,ke,yn_list,create_date,revision_date,upt_user): #개정이력 추가
    # oracleDB=MSDS_upsert.get_db_conn()
    # cur = oracleDB.cursor()
    errmsg=None
    alert_yn,properties_yn,poison_yn,reg_yn,protector_yn,etc_yn=yn_list

    create_date='' if create_date in ['',None,'null'] else create_date
    revision_date='' if revision_date in ['',None,'null'] else revision_date
    
    data = [int(subid), int(seq), cas, en, ke, properties_yn, poison_yn, alert_yn, reg_yn, protector_yn, etc_yn, upt_user]

    msds_his_query= f"""INSERT INTO MSDS_REVISION_HISTORY (
                    CHEMICAL_SUBSTANCE_ID,
                    SEQ,
                    CREATE_DATE,
                    REVISION_DATE,
                    CAS_NO,
                    EN_NO,
                    KE_NO,
                    PROPERTIES_MODIFY_YN,
                    POISON_MODIFY_YN,
                    ALERT_MODIFY_YN,
                    REGULATION_MODIFY_YN,
                    PROTECTOR_MODIFY_YN,
                    ETC_YN,
                    ACTV,

                    REG_DATE,
                    REG_USER) 
                        VALUES( :1,:2,
                    NVL(TO_DATE('{create_date}','YYYY-MM-DD'),''),
                    NVL(TO_DATE('{revision_date}','YYYY-MM-DD'),''),
                    :3,:4,:5,:6,:7,:8,:9,:10,:11,'1',SYSDATE,:12)
                """
    try:
        cur.execute(msds_his_query, data)

        # oracleDB.commit()
        flag="00"

    except oracledb.Error as e:
        print(e)
        errmsg=e
        if oracleDB:
            oracleDB.rollback()
        return "82",Exception(f"{e}")
    # finally:
    #     if oracleDB:
    #         oracleDB.close()
            

    return flag,errmsg


def find_chg_things(new_resultData):  #전처리 후의 데이터 그대로 비교하기
    flag,oracleDB=MSDS_upsert.get_db_conn()
    if flag!="00":
        raise
    cur = oracleDB.cursor()
    errmsg=None
    flag=None
    alert_yn,properties_yn,poison_yn,reg_yn,protector_yn,etc_yn=None,None,None,None,None,None

    alert_chg_col=["TYPE","SEQ","ALERT_TYPE","ALERT_CODE","ALERT"]
    properties_chg_col=["TYPE","SEQ","TYPE_NAME","VALUE","UNIT"]
    poison_chg_col=["TYPE","SEQ","TYPE_NAME","TYPE_NAME2","VALUE","ETC"]
    reg_chg_col=["LAW_CODE","SEQ","LAW_NAME","REGULATION_NAME","NOTE","ACTV","REMARK"]
    protector_chg_col=["TYPE","SEQ","TYPE_NAME","TYPE_NAME2","VALUE","ETC"]

    alert_query=f"""SELECT {','.join(alert_chg_col)}
                    FROM MSDS_CHEMICAL_ALERT 
                    WHERE CHEMICAL_SUBSTANCE_ID={new_resultData["CHEMICAL_SUBSTANCE_ID"]} 
                    order by to_number(type), to_number(seq)
                    """

    properties_query=f"""SELECT {','.join(properties_chg_col)}
                    FROM MSDS_CHEMICAL_PROPERTIES
                    WHERE CHEMICAL_SUBSTANCE_ID={new_resultData["CHEMICAL_SUBSTANCE_ID"]} 
                    order by to_number(type), to_number(seq)
                    """
    
    poison_query=f"""SELECT {','.join(poison_chg_col)}
                    FROM MSDS_CHEMICAL_POISON
                    WHERE CHEMICAL_SUBSTANCE_ID={new_resultData["CHEMICAL_SUBSTANCE_ID"]} 
                    order by to_number(type), to_number(seq)
                    """

    reg_query=f"""SELECT {','.join(reg_chg_col)}
                    FROM MSDS_CHEMICAL_REGULATION
                    WHERE CHEMICAL_SUBSTANCE_ID={new_resultData["CHEMICAL_SUBSTANCE_ID"]} 
                    order by to_number(LAW_CODE), to_number(seq)
                    """

    protector_query=f"""SELECT {','.join(protector_chg_col)}
                    FROM MSDS_CHEMICAL_PROTECTOR
                    WHERE CHEMICAL_SUBSTANCE_ID={new_resultData["CHEMICAL_SUBSTANCE_ID"]} 
                    order by to_number(type), to_number(seq)
                    """
    
    try:
        cur.execute(alert_query)
        res=cur.fetchall()

        if res:
            new_data=[tuple(i.values())[:5] for i in list(itertools.chain(*[v for k,v in new_resultData["CHEM_DETAIL02"].items()]))]
            alert_yn= 'N' if [tuple(j if j!=None else '' for j in i) for i in res]==new_data else 'Y'
        ################################################
        cur.execute(properties_query)
        res=cur.fetchall()
        
        if res:
            new_data=[tuple(i.values())[:5] for i in list(itertools.chain(*[v for k,v in new_resultData["CHEM_DETAIL09"].items()]))]
            properties_yn='N' if [tuple(j if j!=None else '' for j in i) for i in res]==new_data else 'Y'
        ################################################
        cur.execute(poison_query)
        res=cur.fetchall()
        
        if res:
            new_data=[tuple(j if j!=None else '' for j in i.values())[:6] for i in list(itertools.chain(*[v for k,v in new_resultData["CHEM_DETAIL11"].items()]))]
            poison_yn='N' if [tuple(j if j!=None else '' for j in i) for i in res]==new_data else 'Y'
        ################################################
        cur.execute(reg_query)
        res=cur.fetchall()
        
        if res:
            new_data=[tuple(j if j!=None else '' for j in i.values())[:7] for i in list(itertools.chain(*[v for k,v in new_resultData["CHEM_DETAIL15"].items()]))]
            reg_yn='N' if [tuple(j if j!=None else '' for j in i) for i in res]==new_data else 'Y'
        ################################################
        
        cur.execute(protector_query)
        res=cur.fetchall()
        
        if res:
            new_data=[tuple(j if j!=None else '' for j in i.values())[:6] for i in list(itertools.chain(*[v for k,v in new_resultData["CHEM_DETAIL08"].items()]))]
            protector_yn='N' if [tuple(j if j!=None else '' for j in i) for i in res]==new_data else 'Y'

        flag="00"

        #################################################
        # 사실 그외에 변경된 것이 있는지 다 확인필요한데 불가능 -> 5개 테이블중 변경된 것이 있으면 etc_yn은 'N' 넣는걸로 함
        etc_yn= 'Y' if list(set((alert_yn,properties_yn,poison_yn,reg_yn,protector_yn))) == ['N'] else 'N'

    except oracledb.Error as ex:
        print(ex)
        errmsg=ex
        if oracleDB:
            oracleDB.rollback()

        return "83",(alert_yn,properties_yn,poison_yn,reg_yn,protector_yn,etc_yn),errmsg
    finally:
        if oracleDB:
            oracleDB.close()

    return flag,(alert_yn,properties_yn,poison_yn,reg_yn,protector_yn,etc_yn),errmsg


def upsert_msds_history(oracleDB,cur,new_data,upt_user):
    # oracleDB=MSDS_upsert.get_db_conn()
    # cur = oracleDB.cursor()
    seq=0
    yn_list=(None,None,None,None,None,None)

    find_rev_query=f"""SELECT TO_CHAR(REVISION_DATE, 'YYYY-MM-DD') as REVISION_DATE, seq
                        FROM MSDS_REVISION_HISTORY
                        WHERE CHEMICAL_SUBSTANCE_ID={new_data["CHEMICAL_SUBSTANCE_ID"]}
                        order by to_number(seq) desc 
                    """
    try:
        cur.execute(find_rev_query)
        result=cur.fetchall()

        if result: #데이터 있는경우 가져와서 비교
            seq=result[0][1]
            sql_rev=result[0][0] if result[0][0] not in ['',None,'Null'] else ''
            # new_data["KOSHA_REVISION_DATE"]='2024-06-20'
            if new_data["KOSHA_REVISION_DATE"] == sql_rev:
                # 개정x return
                return "00",None
            
            flag,yn_list,errmsg=find_chg_things(new_data) #여기서 바뀐부분 뽑아와
            if flag != "00":
                raise

        
        flag,errmsg=insert_to_table(oracleDB,cur,int(seq)+1,new_data["CHEMICAL_SUBSTANCE_ID"],new_data["CAS_NO"],new_data["EN_NO"],new_data["KE_NO"],yn_list,new_data["KOSHA_CREATE_DATE"],new_data["KOSHA_REVISION_DATE"],upt_user) #개정이력 추가
        if flag != "00":
            raise

            
    except Exception as e:
        print(e)
        if oracleDB:
            oracleDB.rollback()

        return "83",Exception(IF_ERROR_CODE["83"])
        
    # finally:
    #     if oracleDB:
    #         oracleDB.close()

    return flag,errmsg
    

############################INTF_TARGET_MSDS_BASIC_HIS#####################################

def get_his_data(doc_no):

    seq=0
    try:
        flag,oracleDB=MSDS_upsert.get_db_conn()
        cur = oracleDB.cursor()

        find_seq_query=f"""SELECT SEQ 
                            FROM ( 
                            SELECT SEQ 
                            FROM INTF_TARGET_MSDS_BASIC_HIS 
                            WHERE DOC_NO = '{doc_no}'
                            ORDER BY seq DESC 
                            ) 
                            WHERE ROWNUM = 1
                        """
        cur.execute(find_seq_query)
        res_his_seq=cur.fetchone()

        seq=int(res_his_seq[0])+1 if res_his_seq else 1

    except Exception as ora_e:
        if oracleDB:
            oracleDB.rollback()
        return int(seq),"83",Exception(f"{ora_e}")
    finally:
        if oracleDB:
            oracleDB.close()

    return int(seq),"00",None
    

def get_basic_data(doc_no): #INTF_TARGET_MSDS_BASIC 데이터 그대로 받아오기

    basic_data=""
    try:
        flag,oracleDB=MSDS_upsert.get_db_conn()
        cur = oracleDB.cursor()

        find_row_query=f"""SELECT *
                    FROM INTF_TARGET_MSDS_BASIC
                    WHERE DOC_NO='{doc_no}'
                        """
        cur.execute(find_row_query)
        res=cur.fetchall()

        basic_data= res[0] if res else ""

    except Exception as ora_e:
        if oracleDB:
            oracleDB.rollback()
        return basic_data,"82",Exception(f"{ora_e}")
    finally:
        if oracleDB:
            oracleDB.close()

    return basic_data,"00",None


# def get_basic_col(): #INTF_TARGET_MSDS_BASIC 데이터 그대로 받아오기

#     oracleDB=MSDS_upsert.get_db_conn()
#     cur = oracleDB.cursor()
#     col_num=''

#     try:
#         cur.execute("select * from INTF_TARGET_MSDS_BASIC")
        
#         for i in range(1,len(tuple(c[0] for c in cur.description))+2): #seq까지 1추가, 순서가 0부터 시작을 1부터 시작으로 변경해서 1추가
#             col_num+=':'+str(i)+','
#         col_num="("+col_num[:-1]+")"

#         col_names=str(tuple(c[0] for c in cur.description)).replace("'",'')

#     except Exception as ora_e:
#         if oracleDB:
#             oracleDB.rollback()
#         return col_names,col_num,"82",Exception(f"{ora_e}")
#     finally:
#         if oracleDB:
#             oracleDB.close()

#     return col_names,col_num,"00",None


def put_INTF_HIS(seq,doc_no):
    errmsg=None

    try:

        flag,oracleDB=MSDS_upsert.get_db_conn()
        cur = oracleDB.cursor()
        # seq,flag,errmsg=get_his_data(doc_no)
        # if flag !="00":
        #     raise

        # col_names,col_num,flag,errmsg=get_basic_col()
        # if flag !="00":
        #     raise

        basic_data,flag,errmsg=get_basic_data(doc_no)
        if flag !="00":
            raise

        put_row_data= [seq]+[i if i is not None else '' for i in list(basic_data)]
        put_row_query= f"""INSERT INTO INTF_TARGET_MSDS_BASIC_HIS (SEQ,
                            PLANT,
                            DOC_NO,
                            MSDS_NO,
                            DEPT_NM,
                            USE_PROCESS,
                            MATERIAL_NM,
                            USAGE,
                            MONTHLY_AMOUNT,
                            DAILY_AMOUNT,
                            RIVISION_DATE,
                            DISPOSAL_DATE,
                            START_DATE,
                            INTF_DATE,
                            APPR_DATE,
                            APPLY_FLAG,
                            ERROR_MSG,
                            MANUFACTURER_NAME,
                            MANUFACTURER_TEL,
                            SUPPLIER_NAME,
                            SUPPLIER_TEL,
                            RIVISION_NUMBER,
                            ERROR_CASNO
                            )
                            VALUES( :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23)
                """

        cur.execute(put_row_query,put_row_data)
        oracleDB.commit()
        flag="00"

    except oracledb.Error as ora_e:
        if oracleDB:
            oracleDB.rollback()
        ora_e=errmsg if errmsg else ora_e
        return "82",Exception(f"{ora_e}")
    finally:
        if oracleDB:
            oracleDB.close()
            

    return flag,errmsg
    
