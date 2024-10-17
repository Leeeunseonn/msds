# static variable file #
from config import * 
from funcs import dict_fill_na, dict_replace_quote
from setting import *

#### built in modules ####
import requests
from datetime import datetime
import json
from typing import Union
import html
import re
import itertools

#### install modules ####
import xmltodict
# import cx_Oracle #pip install cx_oracle

import oracledb
import pprint

import msds_history


#java에서 select해서
#input: insert인경우 =======>   I;substanceID;casNo
#input: update인경우 =======>   U;substanceID;casNo


MSDS_API_KEY = requests.utils.unquote(msds_api_key)
NO_DATA=['자료없음', '자료 없음','(자료없음)','(자료 없음)', '(해당없음)','해당없음','(해당 안됨)',
         '해당안됨','해당 안됨','없음','(없음)','','(자료 면제)','자료 면제','(자료면제)','자료면제']

SEL_CODE_LIST=["금지물질","폐유기용제","사고대비물질","유독물","취급금지물질","노출기준설정물질",
            "작업환경측정대상물질","비위험물","허용기준설정물질","특수건강진단대상물질","지정폐기물","관리대상유해물질",
            "특별관리물질","위험물질","기존화학물질","PSM 제출대상물질","허가대상물질","취급제한물질"]


############################### mysql 공통 함수 ############################################
from retry import retry

@retry(tries=3)
def get_db_conn():
    oracleDB = None
    flag="83"
    try:
        oracledb.init_oracle_client(lib_dir=ORACLE_HOME)
        oracleDB = oracledb.connect(user=oracleDB_user, password=oracleDB_passwd, dsn=oracleDB_dsn)
        # oracleDB.call = oracleDB_connect_timeout
        # break

        flag="00"

    # except TimeoutError as te:
    #     print("te")
    #     print(te)
    #     raise
    except oracledb.Error as e:
        print("oracle.e")
        print(e)
        raise
    except Exception as e:
        print("e")
        print(e)

    return flag,oracleDB


def insert_substance_if(casno,enno,keno,result_code,result_msg,detail,upt_user):
    flag,oracleDB=get_db_conn()
    if flag!="00":
        return IF_ERROR_CODE[flag]

    cur = oracleDB.cursor()
    result =None

    casno= casno if casno not in ["", None] else "-"
    enno= enno if enno not in ["", None] else "-"
    keno= keno if keno not in ["", None] else "-"

    if detail:
        detail=json.dumps(detail,ensure_ascii = False)
        data = [casno, enno, keno, result_code, result_msg, detail,upt_user]
    else:
        data = [casno, enno, keno, result_code, result_msg, 'null', upt_user]

    log_query= """INSERT INTO MSDS_CHEMICAL_SUBSTANCE_IF (
                    IF_DATE, CAS_NO, EN_NO, KE_NO, RESULT_CODE, RESULT_MSG, RESULT_DATA, REG_USER) 
                    VALUES(SYSDATE, :1,:2,:3,:4,:5,:6,:7)
                """

    try:
        cur.execute(log_query, data)

        oracleDB.commit()
        result="LOG_INSERT_SUCCESS"
    
    except oracledb.Error as e:
        print(e)
        if oracleDB:
            oracleDB.rollback()
    finally:
        if oracleDB:
            oracleDB.close()

    return result


def trim_msg(data,str_msg, int_max_len=4000, encoding='utf-8'):
    try:
        if len(str_msg.encode(encoding))>int_max_len:
            return str_msg.encode(encoding)[:int_max_len-3].decode(encoding)+'…'
        else:
            return str_msg
    except UnicodeDecodeError:
        try:
            return str_msg.encode(encoding)[:int_max_len-4].decode(encoding)+'…'
        except UnicodeDecodeError:        
            return str_msg.encode(encoding)[:int_max_len-5].decode(encoding)+'…'
        
        # finally:
        #     print(str_msg)
        #     print(data)


def chemid_params(searchwrd:str,searchcnd:str):
    return {
        "searchWrd": searchwrd,
        "searchCnd": int(searchcnd),
        "ServiceKey": MSDS_API_KEY,
        "numOfRows": 10,
        "pageNo": 1,
    }

@retry(tries=3)
def get_chemId_api_four(casno: str,keno:str,enno:str) -> str:
    """
    searchCnd = 0 : 국문명
                1 : CAS No
                2 : UN No
                3 : KE No
                4 : EN No
    해당 함수는 INPUT 값으로 항상 CAN.NO를 받는다고 가정하므로 searchCnd 값은 1로 고정
    값이 있거나 성공하면 dictionary 반환, 실패하거나 값이 없으면 Exception 반환
    """

    url = "https://msds.kosha.or.kr/openapi/service/msdschem/chemlist"

    params_dict= {"1":{"casNo":casno}
                , "2":None
                ,"3":{"keNo":keno}
                ,"4":{"enNo":enno}}
    
    res=None
    try:
        for k, dic in params_dict.items():
            if dic:
                for name, v in dic.items():
                    if v not in ['',None,'-']:
                        params = chemid_params(v,k)
                        req = requests.get(url, params)

                        if 200 <= req.status_code < 300:
                            result = json.loads(json.dumps(xmltodict.parse(req.text)))
                            if result.get("response").get("header").get("resultCode") == "00":
                                result = result["response"].get("body")
                                if set(result.keys()) == set(
                                    ["items", "totalCount", "pageNo", "numOfRows"]
                                ):
                                    if int(result["totalCount"]) > 1:
                                        data = [i for i in result["items"]["item"] if i[name] == v]
                                        if data:
                                            return data[0], "00",None
                                    elif (int(result["totalCount"]) == 1) and (
                                        result["items"]["item"][name] == v
                                    ):
                                        return result["items"]["item"] , "00",None
                            else:
                                return (
                                    Exception(result.get("response").get("header").get("resultMsg")),
                                    "91",
                                    result
                                )
                        else:
                            return Exception(f"{req.status_code}_request_error"), "99",result


        if not res:
            return Exception(f"casNo,keNo,enNo not_found_data"), "81",None #casno, keno, enno 셋다에 데이터 없는경우
        
    except requests.exceptions.ConnectTimeout as ex:
        print(ex)
        raise

@retry(tries=3)
def get_chemDetail(chemId: str, operation_name: str) -> list:

    try:
        url = f"https://msds.kosha.or.kr/openapi/service/msdschem/{operation_name}"
        params = {"chemId": chemId, "ServiceKey": MSDS_API_KEY}
        req = requests.get(url, params)
        result=None
        
        if 200 <= req.status_code < 300:
            result = json.loads(json.dumps(xmltodict.parse(req.text)))
            if result.get("response").get("header").get("resultCode") == "00":
                result = result["response"].get("body")
                if set(result.keys()) == set(["items"]):
                    if result['items']:
                        if isinstance(result["items"].get("item"), dict):
                            return [result["items"].get("item")], "00",None
                        else:
                            return result["items"].get("item"), "00",None
                    else:
                        return result.get('items'), "00",None

                return result.get('items'), "00",None
            else:
                return (
                    Exception(result.get("response").get("header").get("resultMsg")),
                    "91",
                    result
                )
        else:
            return Exception(f"{req.status_code}_request_error"), "99",result

    except requests.exceptions.ConnectTimeout as ex:
        print(ex)
        raise

def dictionary_template() -> dict:
    template = {
        "chemdetail01": None,
        "chemdetail02": None,
        "chemdetail03": None,
        "chemdetail04": None,
        "chemdetail05": None,
        "chemdetail06": None,
        "chemdetail07": None,
        "chemdetail08": None,
        "chemdetail09": None,
        "chemdetail10": None,
        "chemdetail11": None,
        "chemdetail12": None,
        "chemdetail13": None,
        "chemdetail14": None,
        "chemdetail15": None,
        "chemdetail16": None,
    }
    return template


def get_item(data, code) -> str: #현코드로 값 return 
    if data:
        d= [i.get("itemDetail", "") for i in data if i["msdsItemCode"] == code][0] if [i.get("itemDetail", "") for i in data if i["msdsItemCode"] == code] else ""
        return d if d else ""
    else:
        return ""
    
def get_item_detail_codes(data,code) : #상위코드로 현 코드 return
    if data:
        return [i.get("msdsItemCode", "") for i in data if i["upMsdsItemCode"] == code] if [i.get("msdsItemCode", "") for i in data if i["upMsdsItemCode"] == code] else ""
    else:
        return ""

def get_item_detail_name(data,code) : #현코드로 코드네임 return 
    if data:
        return [i.get("msdsItemNameKor", "") for i in data if i["msdsItemCode"] == code][0] if [i.get("msdsItemNameKor", "") for i in data if i["msdsItemCode"] == code] else ""
    else:
        return ""


##############################################################################

def chem09_parsing(properties_type, data, code) -> Union[str, list]:

    properties_dict= {"1":"성상", "2": "색상", "3": "냄새", "4": "냄새역치", "5": "pH", "6":"녹는점/어는점", "7": "초기 끓는점과 끓는점 범위", "8": "인화점", "9": "증발속도", "10": "인화성(고체, 기체)",
                     "11": "인화 또는 폭발 범위의 상한/하한", "12": "증기압", "13": "용해도", "14": "증기밀도", "15": "비중", "16": "n-옥탄올/물분배계수 (Kow)", "17": "자연발화온도", "18": "분해온도", "19": "점도", "20": "분자량" }
    #출처, NO_DATA만 제거, 괄호 제거
    chem09_key=[s.strip() for s in get_item(data, code).split('|') if s.strip() not in NO_DATA and '출처' not in s.strip()]
    if chem09_key:
        return [
            {
                "TYPE": properties_type,
                "SEQ": 1,
                "TYPE_NAME": properties_dict[properties_type],
                "VALUE": trim_msg(data,chem09_key[0], int_max_len=4000, encoding='utf-8') if chem09_key else "-",
                "UNIT": "",
                "ACTV":"1",
                "REMARK":"",
            }
            
        ]
    else: #출처만 있음

        return [
            {
                "TYPE": properties_type,
                "SEQ": 1,
                "TYPE_NAME": properties_dict[properties_type],
                "VALUE":"-",
                "UNIT": "",
                "ACTV":"1",
                "REMARK":"",
            }
            
        ]


def alert_parsing(alert_type, data, code) -> list:

    alert_type_dict={"1": "유해성·위험성 분류", "2": "그림문자", "3": "신호어", "4": "유해·위험문구", "5": "예방조치문구(예방)"
                     , "6": "예방조치문구(대응)", "7": "예방조치문구(저장)", "8": "예방조치문구(폐기)"}
    # print("->",data)

    if alert_type in ["2","3"]:
        return [
            {"TYPE": alert_type, "SEQ": idx, "ALERT_TYPE": alert_type_dict[alert_type], "ALERT_CODE":"-", "ALERT": trim_msg(data,i, int_max_len=4000, encoding='utf-8') if i not in NO_DATA else "-","ACTV":"1","REMARK":""}
            for idx, i in enumerate(get_item(data, code).split("|"), 1)
        ]
        
    elif alert_type in ["1", "4", "5", "6", "7", "8"]:
        r_list=list()
        alert_list=[i for idx, i in enumerate(get_item(data, code).split("|"), 1) if i not in NO_DATA]
        
        if alert_list:
            for dx, i in enumerate(alert_list, 0): # 괄호안의 : 구분코드
                stack=list()
                for idx, j in enumerate(i, 0):
                    if j==":":
                        if len(stack)==0:
                            r_list.append((dx,idx))
                            break
                    elif j=="(":
                            stack.append(idx)
                    elif j==")":
                            stack.pop()

            alert_list=[(alert_list[i][:j].strip(),alert_list[i][j+1:].strip()) for (i,j) in r_list]
        
            return [
                {
                    "TYPE": alert_type,
                    "SEQ": idx,
                    "ALERT_TYPE": alert_type_dict[alert_type],
                    "ALERT_CODE": i[0] if i else "-",
                    "ALERT": trim_msg(data,i[1], int_max_len=4000, encoding='utf-8') if i else "-",
                    "ACTV":"1",
                    "REMARK":""
                }
                for idx, i in enumerate(alert_list, 1)
            ]
        else:
            return [
                {
                    "TYPE": alert_type,
                    "SEQ": 1,
                    "ALERT_TYPE": alert_type_dict[alert_type],
                    "ALERT_CODE": "-",
                    "ALERT": "-",
                    "ACTV":"1",
                    "REMARK":""
                }
            ]
        

def conn_lawmap(law_data):
    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()
        result =None
        
        note_query=f"""select indiction from msds_chemical_lawmap where note like :1"""


        for d in law_data:
            cur.execute(note_query,['%'+(d["NOTE"][:6])+'%'])
            result=cur.fetchall()
            if result:
                d["REMARK"]=result[0][0] if result[0][0] else ""
            
            flag="00"
    
    except oracledb.Error as ex:
        print(ex)
        if oracleDB:
            oracleDB.rollback()

        return "83",Exception(IF_ERROR_CODE["83"])
    finally:
        if oracleDB:
            oracleDB.close()

    return flag,law_data


def law_parsing(law_code, data, code) -> list:
    law_dict = {"1": "산업안전보건법", "2": "화학물질관리법", "3": "위험물안전관리법", "4": "폐기물관리법","5":"국내규제","6":"국외규제"}
    etc_dict={"5":["O100202"],
              "6":["O100402","O100404","O100406","O100408","O100410","O100412","O100414","O100416","O100418","O100420","O100422"]}

    law_list =[i for i in get_item(data, code).split("|") if i not in NO_DATA]
    law_data=None

    if law_code in ["1","2","3","4"]:
        law_data= [
            {
                "LAW_CODE": law_code,
                "SEQ": idx,
                "LAW_NAME": law_dict[law_code],
                "REGULATION_NAME": f"{law_dict[law_code]}에 의한 규제",
                "NOTE": trim_msg(data,i.strip(), int_max_len=4000, encoding='utf-8') if i.strip() not in NO_DATA else "-",
                "ACTV":"1",
                "REMARK":"",
            }
            for idx, i in enumerate(law_list, 1)
        ]
        
        if law_data:
            # 주기 괄호안의 중복 제거 ex) 특수건강진단대상물질 (진단주기 : 특수건강진단대상물질 12개월) -> 특수건강진단대상물질 (진단주기 : 12개월)
            for d in law_data:
                if any([i in d["NOTE"] for i in SEL_CODE_LIST]):
                    dup_name=SEL_CODE_LIST[[i in d["NOTE"] for i in SEL_CODE_LIST].index(True)]
                    if d.get("NOTE").count(dup_name)>=2: # 중복멘트가 있는 경우
                        first_sel_code_index=d.get("NOTE").find(dup_name)
                        d["NOTE"]=d.get("NOTE").replace(dup_name,'')
                        d["NOTE"]=(d.get("NOTE")[:first_sel_code_index]+dup_name+d.get("NOTE")[first_sel_code_index:]).replace('  ',' ')
            

            flag,law_data=conn_lawmap(law_data) # REMARK 채우기
            if flag != "00":
                raise
        
        if not law_data:
            law_data= [
            {
                "LAW_CODE": law_code,
                "SEQ": 1,
                "LAW_NAME": law_dict[law_code],
                "REGULATION_NAME": f"{law_dict[law_code]}에 의한 규제",
                "NOTE":"-",
                "ACTV":"1",
                "REMARK":"",
            }
        ]
            
        if code=="O04": # 화학물질관리법만 '-'=> '기존 화학물질'로 변경
            for i in law_data:
                if i["NOTE"] in ["해당없음","-",""]:
                    i["NOTE"]="기존 화학물질"
                    i["REMARK"]="기존 화학물질"
    

    else: #기타 국내, 외국법

        # law_data= [
        #     {
        #         "LAW_CODE": law_code,
        #         "SEQ": 1,
        #         "LAW_NAME": law_dict[law_code],
        #         "REGULATION_NAME": f"{law_dict[law_code]}에 의한 규제",
        #         "NOTE":"-",
        #         "ACTV":"1",
        #         "REMARK":"",
        #     }
        # ]

        # for code in etc_dict[law_code]:

        law_data= [
            {
                "LAW_CODE": law_code,
                "SEQ": idx,
                "LAW_NAME": [i for i in get_item_detail_name(data, c).split("|") if i not in NO_DATA][0],
                "REGULATION_NAME": law_dict[law_code],
                "NOTE": trim_msg(data,get_item(data, c).strip(), int_max_len=4000, encoding='utf-8') if get_item(data, c).strip() not in NO_DATA else "-",
                "ACTV":"1",
                "REMARK":"",
            }
            for idx, c in enumerate(etc_dict[law_code], 1)
        ] if get_item_detail_name(data, etc_dict[law_code][0]) else [{"LAW_CODE": law_code,"SEQ": 1,"LAW_NAME": law_dict[law_code],"REGULATION_NAME": law_dict[law_code],"NOTE": "-","ACTV":"1","REMARK":""}]
        
    return law_data


def protector_parsing(protector_type, data, code) -> list:

    protector_dict = {"H02":["H0202","H0204","H0206","H0208"],
                      "H04":["H04"],
                      "H06":["H0602","H0604","H0606","H0608"]
                        }
    etc_dict= {"H02": ["1","화학물질의 노출기준, 생물학적 노출기준 등"], "H04": ["2",'적절한 공학적 관리'], "H06": ["3","개인보호구"]}

    protector_type2={"1": "국내규정", "2": "ACGIH 규정", "3": "생물학적 노출기준", "4":"기타 노출기준", "5":"적절한 공학적 관리","6": "호흡기 보호","7": "눈 보호", "8":"손 보호", "9": "신체 보호"}

    protector_list=[i.strip() for i in get_item(data, code).split("|") if i!=""]
    # print(protector_list)
    if protector_list:
        
        if code in ["H0202","H0204","H0206","H0208"]: # '화학물질의 노출기준, 생물학적 노출기준 등'은 한줄로

            value= ', '.join(protector_list).rstrip(',')
            return [
                {
                    "TYPE": protector_type,
                    "SEQ": 1,
                    "TYPE_NAME": etc_dict[[k for k,v in protector_dict.items() if code in v][0]][1],
                    "TYPE_NAME2": protector_type2[protector_type],
                    "VALUE": trim_msg(data,value, int_max_len=4000, encoding='utf-8') if value not in NO_DATA else "-",
                    "ETC":etc_dict[[k for k,v in protector_dict.items() if code in v][0]][0],
                    "ACTV":"1",
                    "REMARK":""
                }
            ]
    
        return [  # 그 외의 type은 개행별 하나씩
                {
                    "TYPE": protector_type,
                    "SEQ": idx,
                    "TYPE_NAME": etc_dict[[k for k,v in protector_dict.items() if code in v][0]][1],
                    "TYPE_NAME2": protector_type2[protector_type],
                    "VALUE": trim_msg(data,i.strip(), int_max_len=4000, encoding='utf-8') if i.strip() not in NO_DATA else "-",
                    "ETC":etc_dict[[k for k,v in protector_dict.items() if code in v][0]][0],
                    "ACTV":"1",
                    "REMARK":""
                }
                for idx, i in enumerate(protector_list, 1)
            ]


    else: # 예외인 경우 빈값 넣기
        return [
            {
                "TYPE": protector_type,
                "SEQ": 1,
                "TYPE_NAME": etc_dict[[k for k,v in protector_dict.items() if code in v][0]][1],
                "TYPE_NAME2": protector_type2[protector_type],
                "VALUE": "-",
                "ETC":etc_dict[[k for k,v in protector_dict.items() if code in v][0]][0],
                "ACTV":"1",
                "REMARK":""
            }
        ]


def poison_parsing(poison_type, data, code) -> list:

    poison_dict={"1":"가능성이 높은 노출 경로에 관한 정보","2":"급성독성","3":"피부부식성 또는 자극성",
                    "4":"심한 눈손상 또는 자극성","5":"호흡기과민성","6":"피부과민성","7":"발암성","8":"생식세포변이원성",
                    "9":"생식독성","10":"특정 표적장기 독성 (1회 노출)","11":"특정 표적장기 독성 (반복 노출)","12":"흡인유해성","13":"기타 유해성 영향"}    
    poison_detail_code_dict={"2":["K040202","K040204","K040206"],"7":["K041212","K041214","K041202","K041206","K041210","K041204","K041216"]}
    poison_detail_dict={"K040202":"경구","K040204":"경피","K040206":"흡입","K041212":"산업안전보건법","K041214":"고용노동부고시","K041202":"IARC","K041206":"OSHA","K041210":"ACGIH","K041204":"NTP","K041216":"EU CLP"}

    if poison_type in poison_detail_code_dict.keys(): # type(code)==list

        detail_data = [[i.strip() for i in get_item(data,c).split("|") if i.strip() not in NO_DATA] for c in poison_detail_code_dict[poison_type]]
        if detail_data: #출처만 있으면 빼기
            for idx, i in enumerate(detail_data, 0):
                if i:
                    for j in i:
                        if '출처' not in j:
                            break
                        detail_data[idx]=[]

        return [
            {
                "TYPE": poison_type,
                "SEQ": idx,
                "TYPE_NAME":poison_dict[poison_type],
                "TYPE_NAME2":poison_detail_dict[poison_detail_code_dict[poison_type][idx-1]],
                "VALUE": trim_msg(data,(', '.join(i)).rstrip(','), int_max_len=4000, encoding='utf-8') if i!=[] else "-",
                "ETC": "-",
                "ACTV":"1",
                "REMARK":"",
            } for idx, i in enumerate(detail_data, 1)
        ]

    else:
        
        poison_data =[i.strip() for i in get_item(data, code).split("|") if i.strip() not in NO_DATA and '출처' not in i.strip()]
        source_name=[i.strip() for i in get_item(data, code).split("|") if '※출처' in i]

        return [
            {   
                "TYPE": poison_type,
                "SEQ": 1,
                "TYPE_NAME": poison_dict[poison_type],
                "TYPE_NAME2":"-",
                "VALUE": trim_msg(data,', '.join(poison_data), int_max_len=4000, encoding='utf-8') if poison_data else "-",
                "ETC": ', '.join(source_name) if source_name else "-",
                "ACTV":"1",
                "REMARK":"",
            }
        ]






def json_data_handler(chemlist: dict, chemdetail: dict, chemical_substance_id: int) -> dict:
    result = None
    try:
        chemli = chemlist
        chem02 = chemdetail.get("chemdetail02") if chemdetail["chemdetail02"] else ""
        chem03 = chemdetail.get("chemdetail03") if chemdetail["chemdetail03"] else ""
        chem08 = chemdetail.get("chemdetail08") if chemdetail["chemdetail08"] else ""
        chem09 = chemdetail.get("chemdetail09") if chemdetail["chemdetail09"] else ""
        chem11 = chemdetail.get("chemdetail11") if chemdetail["chemdetail11"] else ""
        chem15 = chemdetail.get("chemdetail15") if chemdetail["chemdetail15"] else ""
        chem16 = chemdetail.get("chemdetail16") if chemdetail["chemdetail16"] else ""

    except Exception as e:
        return result,"91",Exception(f"{e}")

    try:
        kosha_revision_date=None
        kosha_create_date=None

        if chem16:
            kosha_create_date = get_item(chem16, "P04").split("|")[0] if get_item(chem16, "P04").split("|")[0] not in NO_DATA else None
            kosha_revision_date = get_item(chem16, "P0604").split("|")[0] if get_item(chem16, "P0604").split("|")[0] not in NO_DATA else None

            if kosha_create_date:
                kosha_create_date = re.findall(r"\d{4}-\d{2}-\d{2}", kosha_create_date)[0] if re.findall(r"\d{4}-\d{2}-\d{2}", kosha_create_date) else kosha_create_date
            if kosha_revision_date:
                kosha_revision_date = re.findall(r"\d{4}-\d{2}-\d{2}", kosha_revision_date)[0] if re.findall(r"\d{4}-\d{2}-\d{2}", kosha_revision_date) else kosha_revision_date
            

        w_v = (
            [
                i.strip()
                for i in get_item(chem15, "O02").split("|")
                if "작업환경측정대상물질" in i.strip()
            ][0]
            .split()[-1]
            .replace("개월)", "")
            if int("작업환경측정대상물질" in get_item(chem15, "O02"))
            else ""
        )
        work_cycle = re.sub("[^0-9]", "",re.sub("\(.*\)|\s-\s.*" , '',w_v)) # 괄호삭제 후, 숫자아닌것 삭제
        if not work_cycle.isnumeric():
            work_cycle = None
        s_v = (
            [
                i.strip()
                for i in get_item(chem15, "O02").split("|")
                if "특수건강진단대상물질" in i.strip()
            ][0]
            .split()[-1]
            .replace("개월)", "")
            if int("특수건강진단대상물질" in get_item(chem15, "O02"))
            else ""
        )
        special_cycle = re.sub("[^0-9]", "",re.sub("\(.*\)|\s-\s.*" , '',s_v)) # 괄호삭제 후, 숫자아닌것 삭제
        if not special_cycle.isnumeric():
            special_cycle = None


        # CMR
        cmr_dict= {x[0]:x[1] for x in [[j.strip() for j in i.split(':')] for i in get_item(chem02, "B02").split('|') if i.strip() not in NO_DATA] if len(x)>1}
        cmr_c,cmr_m,cmr_r=list(),list(),list()
        if cmr_dict:
            cmr_c= [v for k,v in cmr_dict.items() if "발암성" in k]
            cmr_m= [v for k,v in cmr_dict.items() if "생식세포 변이원성" in k]
            cmr_r= [v for k,v in cmr_dict.items() if "생식독성" in k]

        # exposure_twa, unit
        exposure_twa= [s.strip() for s in get_item(chem08,"H0202").split('|') if s.strip()]
        exposure_twa_unit=""
        v_twa=None

        if exposure_twa:
            exposure_twa=[i for i in exposure_twa if '출처' not in i and i not in NO_DATA]
            # exposure_twa = exposure_twa if exposure_twa[0] not in NO_DATA else ""
            exposure_twa=[s for s in exposure_twa if "TWA" in s] if exposure_twa else []

            if exposure_twa:
                if len([i.strip() for i in exposure_twa[0].split(':')])>=2:
                    twa_here=[index for index, n in enumerate([i.strip() for i in exposure_twa[0].split(':')],0) if 'TWA' in n][0]
                    exposure_twa=[i.strip() for i in exposure_twa[0].split(':')][int(twa_here)+1].lower()

                    exposure_twa_unit_list=[(exposure_twa.find(i),i) for i in ['mg/m3','ppm','㎎/㎥','mg/㎥'] if i in exposure_twa]
                    exposure_twa_unit_list.sort(key= lambda x:x[0])
                    exposure_twa_unit=exposure_twa_unit_list[0][1] if exposure_twa_unit_list else None
                    
                    n_twa=exposure_twa.split(exposure_twa_unit)[0].strip() if exposure_twa else None

                    n_twa = re.sub(pattern=r'\([^)]*\)', repl='', string= n_twa) if re.sub(pattern=r'\([^)]*\)', repl='', string= n_twa) else '' # 괄호제거
                    if len(re.findall(r'-?\d*\.?\d+',n_twa))>1:
                        if n_twa[n_twa.find(re.findall(r'-?\d*\.?\d+',n_twa)[-1])-1] not in ["-","~",","]:
                            v_twa= re.findall(r'-?\d*\.?\d+',n_twa)[-1] if re.findall(r'-?\d*\.?\d+',n_twa) else None
                            if v_twa:
                                v_twa=v_twa if float(v_twa)>=0 else v_twa[1:] # 음수인경우 부호빼기
                    if not v_twa:
                        v_twa=re.findall(r'-?\d*\.?\d+',n_twa)[-1] if re.findall(r'-?\d*\.?\d+',n_twa) else None
                    
        # form 
        form=[s.strip() for s in get_item(chem09, "I0202").split('|') if s.strip()]
        if form:
            form=(', '.join([i for i in form if '출처' not in i and i not in NO_DATA])).rstrip(',')
            if form:
                form= [i for i in ['고체','액체','기체'] if i in form] if [i for i in ['고체','액체','기체'] if i in form] else form
                if isinstance(form, list):
                    form=(', '.join(form)).rstrip(',')
                if isinstance(form, str):
                    form=(', '.join([k for k,v in properties_shape.items() if any([shape for shape in v if shape in form])])).rstrip(',') if len([k for k,v in properties_shape.items() if any([shape for shape in v if shape in form])]) else trim_msg(chemlist,form,60)
        # print("form : ", form)

        #form_code            
        form_code_dict={"기타":None,"고체":"1","액체":"2","기체":"3"}
        form_code=None
        if form:
            if form in ['고체','액체','기체']:
                form_code=form_code_dict[form]


        # boiling
        v_boiling=None
        boiling=[s.strip() for s in get_item(chem09, "I12").split('|') if s.strip()]
        if boiling:
            boiling=[i for i in boiling if '출처' not in i and i not in NO_DATA]
            if boiling:    #℉ 구분 필요
                boiling[0]= '20' if '상온' == boiling[0] else boiling[0]
                boiling=[i.replace('영하','-') for i in boiling]
                boiling = re.sub(pattern=r'\([^)]*\)', repl='', string= boiling[0]) if re.sub(pattern=r'\([^)]*\)', repl='', string= boiling[0]) else '' # 괄호제거
                
                f_unit = True if '℉' in boiling else None
                k_unit = True if 'K' in boiling and all(i not in boiling for i in ['°C','℃']) else None

                if any(s in boiling for s in ['mmHg','mmhg']): #mmHg 제거
                    boiling= boiling.replace(' ','')
                    tmp_H= [boiling.replace(i+'mmHg','') for i in re.findall(r'-?\d*\.?\d+',boiling) if i+'mmHg' in boiling] 
                    boiling= tmp_H[0] if tmp_H else boiling
                    tmp_h = [boiling.replace(i+'mmhg','') for i in re.findall(r'-?\d*\.?\d+',boiling) if i+'mmhg' in boiling] 
                    boiling= tmp_h[0] if tmp_h else boiling

                if '±' in boiling: # 플러스마이너스 처리
                    boiling= boiling.replace(' ','')
                    boiling= boiling.split('~')[-1] if '~' in boiling else boiling
                    addv=[i for i in re.findall(r'-?\d*\.?\d+',boiling) if '±'+i in boiling][-1]
                    val_list=re.findall(r'-?\d*\.?\d+',boiling)
                    val_list.remove(addv)
                    boiling=str(float(val_list[-1])+float(addv))

                if len(re.findall(r'-?\d*\.?\d+',boiling))>1:
                    if boiling[boiling.find(re.findall(r'-?\d*\.?\d+',boiling)[-1])-1] not in ["-","~",","]: #범위없는 경우
                        v_boiling= re.findall(r'-?\d*\.?\d+',boiling)[-1] if re.findall(r'-?\d*\.?\d+',boiling) else None
                        if v_boiling:
                            v_boiling=v_boiling if float(v_boiling)>=0 else v_boiling[1:] # 음수인경우 부호빼기
                if not v_boiling:
                    v_boiling=re.findall(r'-?\d*\.?\d+',boiling)[-1] if re.findall(r'-?\d*\.?\d+',boiling) else None

                # 화씨 => 섭씨로 변환
                v_boiling= str(round((float(float(v_boiling)-32)*(5/9)),5)) if f_unit else v_boiling

                # 켈빈 => 섭씨로 변환
                v_boiling= str(float(v_boiling)-273) if k_unit else v_boiling 
                
            
        # print("boiling: ",v_boiling)

        # MOLECULAR
        v_molecular=None
        molecular=[s.strip() for s in get_item(chem09, "I38").split('|') if s.strip()]
        if molecular:
            molecular=[i for i in molecular if '출처' not in i and i not in NO_DATA]
            if molecular:
                molecular=[i.replace('영하','-') for i in molecular]
                molecular = re.sub(pattern=r'\([^)]*\)', repl='', string= molecular[0]) if re.sub(pattern=r'\([^)]*\)', repl='', string= molecular[0]) else '' # 괄호제거
                molecular=molecular.replace(',','')
                if len(re.findall(r'-?\d*\.?\d+',molecular))>1:
                    if molecular[molecular.find(re.findall(r'-?\d*\.?\d+',molecular)[-1])-1] not in ["-","~",","]:
                        v_molecular= re.findall(r'-?\d*\.?\d+',molecular)[-1] if re.findall(r'-?\d*\.?\d+',molecular) else None
                        if v_molecular:
                            v_molecular=v_molecular if float(v_molecular)>=0 else v_molecular[1:] # 음수인경우 부호빼기
                if not v_molecular:
                    v_molecular=re.findall(r'-?\d*\.?\d+',molecular)[-1] if re.findall(r'-?\d*\.?\d+',molecular) else None
            
        # print("molecular : ", v_molecular)

        # acute_toxic
        acute_toxic = [i for i in [s.strip() for s in get_item(chem02, "B02").split('|')] if '급성 독성' in i]
        # print([s.strip() for s in get_item(chem02, "B02").split('|')])
        # print(acute_toxic)

        
            # print("chemli:",chemli)
            # print("chem02:",chem02)
            # print("chem03:",chem03)
            # print("chem08:",chem08)
            # print("chem09:",chem09)
            # print("chem11:",chem11)
            # print("chem15:",chem15)

        result = {
            "CHEMICAL_SUBSTANCE_ID": int(chemical_substance_id),
            "CAS_NO":chemli.get("casNo", "-") if chemli.get("casNo", "-") not in NO_DATA else "-",
            "CHEM_ID": chemli.get("chemId", "-") if chemli.get("chemId", "-") not in NO_DATA else "-",
            "CHEM_NAME": chemli.get("chemNameKor", "-") if chemli.get("chemNameKor", "-") not in NO_DATA else "-",
            "UN_NO":chemli.get("unNo", "-") if chemli.get("unNo", "-") not in NO_DATA else "-",
            "EN_NO": chemli.get("enNo", "-") if chemli.get("enNo", "-") not in NO_DATA else "-",
            "KE_NO": chemli.get("keNo", "-") if chemli.get("keNo", "-") not in NO_DATA else "-",
            "NICKNAME": get_item(chem03, "C04") if get_item(chem03, "C04") not in NO_DATA else "-",
            "FLOW_RATE": get_item(chem03, "C08").replace('%','') if get_item(chem03, "C08") not in NO_DATA else "-",
            "NOVELIS_NICKNAME_KO":"-",
            "NOVELIS_NICKNAME_EN":"-",
            "CMR_C": cmr_c[0] if cmr_c else "-",
            "CMR_M": cmr_m[0] if cmr_m else "-",
            "CMR_R": cmr_r[0] if cmr_r else "-",
            "EXPOSURE_TWA": v_twa if v_twa else "-",
            "UNIT":exposure_twa_unit if exposure_twa_unit else "",
            "FORM" :form if form else "-",
            "FORM_CODE" :form_code if form_code else "",
            "BOILING": v_boiling if v_boiling else "-",
            "MOLECULAR": v_molecular if v_molecular else "-",
            "ACUTE_TOXIC" : "1" if acute_toxic else "0",
            "TOXIC_SUBSTANCES_YN": int("유독물질" in get_item(chem15, "O04")),
            "PROHIBITED_SUBSTANCES_YN": int("금지물질" in get_item(chem15, "O02")),
            "PERMITTED_SUBSTANCES_YN": int("허가물질" in get_item(chem15, "O02")),
            "TOXIC_MANAGEMENT_YN": int("관리대상유해물질" in get_item(chem15, "O02")),
            "SPECIAL_CARE_YN": int("특별관리물질" in get_item(chem15, "O02")),
            "WORK_ENVIRONMENT_YN": int("작업환경측정대상물질" in get_item(chem15, "O02")),
            "WORK_ENVIRONMENT_CYCLE": work_cycle,
            "SPECIAL_HEALTH_YN": int("특수건강진단대상물질" in get_item(chem15, "O02")),
            "SPECIAL_HEALTH_CYCLE": special_cycle,
            "EXPOSURE_SET_YN": int("노출기준설정물질" in get_item(chem15, "O02")),
            "ACCEPTABLE_SET_YN": int("허용기준설정물질" in get_item(chem15, "O02")),
            "PSM_YN": int("공정안전보고서(PSM) 제출 대상물질" in get_item(chem15, "O02")),
            "KOSHA_REVISION_DATE": kosha_revision_date if kosha_revision_date else "",
            "KOSHA_CREATE_DATE": kosha_create_date if kosha_create_date else "",
            "ACTV":"1",
            "REMARK":"",
            
            "CHEM_DETAIL02": {
                "ALERT_LIST1": alert_parsing("1", chem02, "B02"),
                "ALERT_LIST2": alert_parsing("2", chem02, "B0402"),
                "ALERT_LIST3": alert_parsing("3", chem02, "B0404"),
                "ALERT_LIST4": alert_parsing("4", chem02, "B0406"),
                "ALERT_LIST5": alert_parsing("5", chem02, "B040802"),
                "ALERT_LIST6": alert_parsing("6", chem02, "B040804"),
                "ALERT_LIST7": alert_parsing("7", chem02, "B040806"),
                "ALERT_LIST8": alert_parsing("8", chem02, "B040808"),
            },
            "CHEM_DETAIL15": {
                "REGULATION_LIST1": law_parsing("1", chem15, "O02"),
                "REGULATION_LIST2": law_parsing("2", chem15, "O04"),
                "REGULATION_LIST3": law_parsing("3", chem15, "O06"),
                "REGULATION_LIST4": law_parsing("4", chem15, "O08"),

                "REGULATION_LIST5": law_parsing("5", chem15, "O1002"),
                "REGULATION_LIST6": law_parsing("6", chem15, "O1004"),

            },
            "CHEM_DETAIL08":{
                "PROTECTOR_LIST1": protector_parsing("1", chem08, "H0202"),
                "PROTECTOR_LIST2": protector_parsing("2", chem08, "H0204"),
                "PROTECTOR_LIST3": protector_parsing("3", chem08, "H0206"),
                "PROTECTOR_LIST4": protector_parsing("4", chem08, "H0208"),
                "PROTECTOR_LIST5": protector_parsing("5", chem08, "H04"),
                "PROTECTOR_LIST6": protector_parsing("6", chem08, "H0602"),
                "PROTECTOR_LIST7": protector_parsing("7", chem08, "H0604"),
                "PROTECTOR_LIST8": protector_parsing("8", chem08, "H0606"),
                "PROTECTOR_LIST9": protector_parsing("9", chem08, "H0608"),

            },
            "CHEM_DETAIL09":{
                # "PROPERTIES_LIST1": [{"TYPE":"1","SEQ":1,"TYPE_NAME":"성상","VALUE":form if form else "-","UNIT":"","ACTV":"1","REMARK":""}],
                "PROPERTIES_LIST1": chem09_parsing("1",chem09, "I0202"),
                "PROPERTIES_LIST2": chem09_parsing("2",chem09, "I0204"),
                "PROPERTIES_LIST3": chem09_parsing("3",chem09, "I04"),
                "PROPERTIES_LIST4": chem09_parsing("4",chem09, "I06"),
                "PROPERTIES_LIST5": chem09_parsing("5",chem09, "I08"),
                "PROPERTIES_LIST6": chem09_parsing("6",chem09, "I10"),
                # "PROPERTIES_LIST7": [{"TYPE":"7","SEQ":1,"TYPE_NAME":"초기 끓는점과 끓는점 범위","VALUE":v_boiling if v_boiling else "-","UNIT":"℃" if v_boiling else "","ACTV":"1","REMARK":""}],
                "PROPERTIES_LIST7": chem09_parsing("7",chem09, "I12"),
                "PROPERTIES_LIST8": chem09_parsing("8",chem09, "I14"),
                "PROPERTIES_LIST9": chem09_parsing("9",chem09, "I16"),
                "PROPERTIES_LIST10": chem09_parsing("10",chem09, "I18"),
                "PROPERTIES_LIST11": chem09_parsing("11",chem09, "I20"),
                "PROPERTIES_LIST12": chem09_parsing("12",chem09, "I22"),
                "PROPERTIES_LIST13": chem09_parsing("13",chem09, "I24"),
                "PROPERTIES_LIST14": chem09_parsing("14",chem09, "I26"),
                "PROPERTIES_LIST15": chem09_parsing("15",chem09, "I28"),
                "PROPERTIES_LIST16": chem09_parsing("16",chem09, "I30"),
                "PROPERTIES_LIST17": chem09_parsing("17",chem09, "I32"),
                "PROPERTIES_LIST18": chem09_parsing("18",chem09, "I34"),
                "PROPERTIES_LIST19": chem09_parsing("19",chem09, "I36"),
                "PROPERTIES_LIST20": chem09_parsing("20",chem09, "I38"),
            },
            "CHEM_DETAIL11":{
                "POISON_LIST1": poison_parsing("1", chem11, "K02"),
                "POISON_LIST2": poison_parsing("2", chem11, "K0402"),
                "POISON_LIST3": poison_parsing("3", chem11, "K0404"),
                "POISON_LIST4": poison_parsing("4", chem11, "K0406"),
                "POISON_LIST5": poison_parsing("5", chem11, "K0408"),
                "POISON_LIST6": poison_parsing("6", chem11, "K0410"),
                "POISON_LIST7": poison_parsing("7", chem11, "K0412"),
                "POISON_LIST8": poison_parsing("8", chem11, "K0414"),
                "POISON_LIST9": poison_parsing("9", chem11, "K0416"),
                "POISON_LIST10": poison_parsing("10", chem11, "K0418"),
                "POISON_LIST11": poison_parsing("11", chem11, "K0420"),
                "POISON_LIST12": poison_parsing("12", chem11, "K0422"),
                "POISON_LIST13": poison_parsing("13", chem11, "K0424"),

            },
        }

        dict_fill_na(result, "")
        for k, v in result.items():
            if result[k] in null_list:
                result[k] = ""
            if isinstance(result[k], int):
                result[k] = str(v)

        dict_replace_quote(result)

    except Exception as e:
        return result,"82",Exception(f"{e}")


    # print(result)
    return result,"00",None


def operation_name_info():
    """
    참고용 dummy function
    chemlist     : 화학물질목록
    chemdetail01 : 1. 화학제품과 회사에 관한 정보
    chemdetail02 : 2. 유해성, 위험성
    chemdetail03 : 3. 구성성분의 명칭 및 함유량
    chemdetail04 : 4. 응급조치요령
    chemdetail05 : 5. 폭발, 화재시 대처방법
    chemdetail06 : 6. 누출사고시 대처방법
    chemdetail07 : 7. 취급 및 저장방법
    chemdetail08 : 8. 노출방지 및 개인보호구
    chemdetail09 : 9. 물리화학적 특성
    chemdetail10 : 10. 안정성 및 반응성
    chemdetail11 : 11. 독성에 관한 정보
    chemdetail12 : 12. 환경에 미치는 영향
    chemdetail13 : 13. 폐기시 주의사항
    chemdetail14 : 14. 운송에 필요한 정보
    chemdetail15 : 15. 법적 규제현황
    chemdetail16 : 16. 그 밖의 참고사항
    """

##############################################################################
    
    
def del_for_update_query(table_name:str,id:int):
    return f"DELETE FROM {table_name} WHERE CHEMICAL_SUBSTANCE_ID={id}"


def insert_MSDS_CHEM_DETAIL02(CHEMICAL_SUBSTANCE_ID:str,CHEM_DETAIL02:dict):
    return [(int(CHEMICAL_SUBSTANCE_ID),d['TYPE'],d['SEQ'],d['ALERT_TYPE'],d['ALERT_CODE'],d['ALERT']) for d in list(itertools.chain(*[vlist for vlist in CHEM_DETAIL02.values()]))]


def insert_MSDS_CHEM_DETAIL09(CHEMICAL_SUBSTANCE_ID:str,CHEM_DETAIL09:dict):
    return [(int(CHEMICAL_SUBSTANCE_ID),d['TYPE'],d['SEQ'],d['TYPE_NAME'],d['VALUE'],d['UNIT']) for d in list(itertools.chain(*[vlist for vlist in CHEM_DETAIL09.values()]))]


def insert_MSDS_CHEM_DETAIL11(CHEMICAL_SUBSTANCE_ID:str,CHEM_DETAIL11:dict):
    return [(int(CHEMICAL_SUBSTANCE_ID),d['TYPE'],d['SEQ'],d['TYPE_NAME'],d['TYPE_NAME2'],d['VALUE'],d['ETC']) for d in list(itertools.chain(*[vlist for vlist in CHEM_DETAIL11.values()]))]


def insert_MSDS_CHEM_DETAIL15(CHEMICAL_SUBSTANCE_ID:str,CHEM_DETAIL15:dict):
    return [(int(CHEMICAL_SUBSTANCE_ID),d['LAW_CODE'],d['SEQ'],d['LAW_NAME'],d['REGULATION_NAME'],d['NOTE'],d['REMARK']) for d in list(itertools.chain(*[vlist for vlist in CHEM_DETAIL15.values()]))]


def insert_MSDS_CHEM_DETAIL08(CHEMICAL_SUBSTANCE_ID:str,CHEM_DETAIL08:dict):
    return [(int(CHEMICAL_SUBSTANCE_ID),d['TYPE'],d['SEQ'],d['TYPE_NAME'],d['TYPE_NAME2'],d['VALUE'],d['ETC']) for d in list(itertools.chain(*[vlist for vlist in CHEM_DETAIL08.values()]))]



def insert_MSDS(oracleDB,cur,result_list: dict, upt_user: str, nov_ko:str, nov_en:str):

    flag="99"
    
    try:
        substance_data=[result_list["CHEMICAL_SUBSTANCE_ID"]
                                    , result_list["CAS_NO"]
                                    , result_list["CHEM_ID"]
                                    , result_list["CHEM_NAME"]
                                    , result_list["UN_NO"]
                                    , result_list["EN_NO"]
                                    , result_list["KE_NO"]
                                    , result_list["NICKNAME"]
                                    , nov_ko
                                    , nov_en

                                    , result_list["FLOW_RATE"]
                                    , result_list["CMR_C"]
                                    , result_list["CMR_M"]
                                    , result_list["CMR_R"]
                                    , result_list["EXPOSURE_TWA"]
                                    , result_list["UNIT"]
                                    , result_list["FORM"]
                                    , result_list["FORM_CODE"]
                                    , result_list["BOILING"]
                                    , result_list["TOXIC_SUBSTANCES_YN"]

                                    , result_list["PROHIBITED_SUBSTANCES_YN"]
                                    , result_list["PERMITTED_SUBSTANCES_YN"]
                                    , result_list["TOXIC_MANAGEMENT_YN"]
                                    , result_list["SPECIAL_CARE_YN"]
                                    , result_list["WORK_ENVIRONMENT_YN"]
                                    , result_list["WORK_ENVIRONMENT_CYCLE"]
                                    , result_list["SPECIAL_HEALTH_YN"]
                                    , result_list["SPECIAL_HEALTH_CYCLE"]
                                    , result_list["EXPOSURE_SET_YN"]
                                    , result_list["ACCEPTABLE_SET_YN"]

                                    , result_list["PSM_YN"]
                                    # , result_list["KOSHA_REVISION_DATE"]
                                    
                                    # , '1'
                                    # , SYSDATE
                                    # , 'API'      #EPM은 API_EPM, 우리측 연동은 API_BTN으로 변경
                                    # , SYSDATE
                                    # , 'API'
                                    , result_list["MOLECULAR"]
                                    , result_list["ACUTE_TOXIC"]]
        
        substance_query=f"""
            INSERT INTO MSDS_CHEMICAL_SUBSTANCE (CHEMICAL_SUBSTANCE_ID
                                    , CAS_NO
                                    , CHEM_ID
                                    , CHEM_NAME
                                    , UN_NO
                                    , EN_NO
                                    , KE_NO
                                    , NICKNAME
                                    , NOVELIS_NICKNAME_KO
                                    , NOVELIS_NICKNAME_EN

                                    , FLOW_RATE
                                    , CMR_C
                                    , CMR_M
                                    , CMR_R
                                    , EXPOSURE_TWA
                                    , UNIT
                                    , FORM
                                    , FORM_CODE
                                    , BOILING
                                    , TOXIC_SUBSTANCES_YN

                                    , PROHIBITED_SUBSTANCES_YN
                                    , PERMITTED_SUBSTANCES_YN
                                    , TOXIC_MANAGEMENT_YN
                                    , SPECIAL_CARE_YN
                                    , WORK_ENVIRONMENT_YN
                                    , WORK_ENVIRONMENT_CYCLE
                                    , SPECIAL_HEALTH_YN
                                    , SPECIAL_HEALTH_CYCLE
                                    , EXPOSURE_SET_YN
                                    , ACCEPTABLE_SET_YN

                                    , PSM_YN
                                    , KOSHA_REVISION_DATE
                                    , KOSHA_CREATE_DATE

                                    , ACTV
                                    , REG_DATE
                                    , REG_USER
                                    , UPT_DATE
                                    , UPT_USER

                                    , MOLECULAR
                                    , ACUTE_TOXIC)
                                VALUES ( :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31
                                    , TO_DATE('{result_list["KOSHA_REVISION_DATE"]}','YYYY-MM-DD')
                                    , TO_DATE('{result_list["KOSHA_CREATE_DATE"]}','YYYY-MM-DD')
                                    , '1'
                                    , SYSDATE
                                    , '{upt_user}'
                                    , SYSDATE
                                    , '{upt_user}'

                                    , :32
                                    , :33
                                    )
                                    """
        
        query_02=f"""INSERT INTO MSDS_CHEMICAL_ALERT (CHEMICAL_SUBSTANCE_ID 
                                                        , TYPE 
                                                        , SEQ 
                                                        , ALERT_TYPE 
                                                        , ALERT_CODE 
                                                        , ALERT 
                                                        , REG_DATE 
                                                        , REG_USER
                                                        , UPT_DATE
                                                        , UPT_USER
                                                        , ACTV ) VALUES (:1,:2,:3,:4,:5,:6,SYSDATE,'{upt_user}',SYSDATE,'{upt_user}','1')"""
        
        query_09=f"""INSERT INTO MSDS_CHEMICAL_PROPERTIES (CHEMICAL_SUBSTANCE_ID 
                                    , TYPE 
                                    , SEQ 
                                    , TYPE_NAME 
                                    , VALUE 
                                    , UNIT 
                                    , REG_DATE 
                                    , REG_USER
                                    , UPT_DATE
                                    , UPT_USER
                                    , ACTV )  VALUES (:1,:2,:3,:4,:5,:6,SYSDATE,'{upt_user}',SYSDATE,'{upt_user}','1')"""

        query_11=f"""INSERT INTO MSDS_CHEMICAL_POISON (CHEMICAL_SUBSTANCE_ID 
                                    , TYPE 
                                    , SEQ 
                                    , TYPE_NAME 
                                    , TYPE_NAME2 
                                    , VALUE
                                    , ETC
                                    , REG_DATE 
                                    , REG_USER
                                    , UPT_DATE
                                    , UPT_USER
                                    , ACTV) VALUES (:1,:2,:3,:4,:5,:6,:7, SYSDATE,'{upt_user}',SYSDATE,'{upt_user}','1')"""
        
        query_15=f"""INSERT INTO MSDS_CHEMICAL_REGULATION (CHEMICAL_SUBSTANCE_ID 
                                    , LAW_CODE 
                                    , SEQ 
                                    , LAW_NAME 
                                    , REGULATION_NAME 
                                    , NOTE
                                    , REMARK
                                    , REG_DATE 
                                    , REG_USER
                                    , UPT_DATE
                                    , UPT_USER
                                    , ACTV ) VALUES (:1,:2,:3,:4,:5,:6,:7,SYSDATE,'{upt_user}',SYSDATE,'{upt_user}','1')"""
        
        query_08=f"""INSERT INTO MSDS_CHEMICAL_PROTECTOR (CHEMICAL_SUBSTANCE_ID 
                                    , TYPE 
                                    , SEQ 
                                    , TYPE_NAME 
                                    , TYPE_NAME2 
                                    , VALUE 
                                    , ETC 
                                    , REG_DATE 
                                    , REG_USER
                                    , UPT_DATE
                                    , UPT_USER
                                    , ACTV ) VALUES (:1,:2,:3,:4,:5,:6,:7, SYSDATE,'{upt_user}',SYSDATE,'{upt_user}','1')"""
        
        # print(substance_query)

        cur.execute(substance_query,substance_data)
        cur.executemany(query_02,insert_MSDS_CHEM_DETAIL02(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL02"]))
        cur.executemany(query_09,insert_MSDS_CHEM_DETAIL09(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL09"]))
        cur.executemany(query_11,insert_MSDS_CHEM_DETAIL11(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL11"]))
        cur.executemany(query_15,insert_MSDS_CHEM_DETAIL15(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL15"]))
        cur.executemany(query_08,insert_MSDS_CHEM_DETAIL08(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL08"]))

        insert_rphrase_query=f"""INSERT INTO MSDS_CHEMICAL_RPHRASE
                                    (R_PHRASE, CHEMICAL_SUBSTANCE_ID, TYPE, SEQ, GRADE, TYPE_NM, POISON, REG_DATE, REG_USER, UPT_DATE, UPT_USER)
                                SELECT B.R_PHRASE, A.CHEMICAL_SUBSTANCE_ID, A.TYPE, A.SEQ, B.GRADE, '유해·위험문구(R_PHRASE)' AS TYPE_NM, B.POISON, SYSDATE, '{upt_user}', SYSDATE, '{upt_user}'
                                FROM MSDS_CHEMICAL_ALERT A
                            INNER JOIN MSDS_R_PHRASE       B ON A.ALERT_CODE = B.H_CODE
                                WHERE A.ACTV = '1' 
                                AND A.TYPE = '4'
                                AND A.CHEMICAL_SUBSTANCE_ID = {result_list["CHEMICAL_SUBSTANCE_ID"]}"""
        
        cur.execute(insert_rphrase_query)
        
        oracleDB.commit()
        flag="00"


    except oracledb.Error as e:
        print(e)
        if oracleDB:
            oracleDB.rollback()
        return "82",Exception(f"{e}")
    # finally:
    #     if oracleDB:
    #         oracleDB.close()
            

    return flag,None


def find_reg_data(result_list: dict):
    reg_date,reg_user="", ""
    flag="99"

    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()
        if result_list:
            reg_date_query=f"""SELECT REG_DATE,REG_USER,NOVELIS_NICKNAME_KO,NOVELIS_NICKNAME_EN FROM MSDS_CHEMICAL_SUBSTANCE WHERE CHEMICAL_SUBSTANCE_ID={result_list["CHEMICAL_SUBSTANCE_ID"]}"""
    
            cur.execute(reg_date_query)

            result=cur.fetchall()
            # print(result)
            
            if result:
                if result[0]:
                    reg_date= result[0][0] if result[0][0] else ""
                    reg_user= result[0][1] if result[0][1] else ""
                    nov_ko= result[0][2] if result[0][2] else "-"
                    nov_en= result[0][3] if result[0][3] else "-"
                    flag="00"

            # print(reg_date)

    except oracledb.Error as e:
        print(e)
        if oracleDB:
            oracleDB.rollback()
        flag="82"
        return reg_date,reg_user,flag,Exception(f"{e}")
    finally:
        if oracleDB:
            oracleDB.close()

    return reg_date,reg_user,nov_ko,nov_en,flag,None


def update_MSDS(oracleDB,cur,result_list: dict, reg_date:str, upt_user:str, reg_user:str, nov_ko:str, nov_en:str):

    flag=None

    try:
        substance_data=[    result_list["CAS_NO"]
                        , result_list["CHEM_ID"]
                        , result_list["CHEM_NAME"]
                        , result_list["UN_NO"]
                        , result_list["EN_NO"]
                        , result_list["KE_NO"]
                        , result_list["NICKNAME"]
                        , nov_ko
                        , nov_en
                        , result_list["FLOW_RATE"]
                        , result_list["CMR_C"]
                        , result_list["CMR_M"]
                        , result_list["CMR_R"]
                        , result_list["EXPOSURE_TWA"]
                        , result_list["UNIT"]
                        , result_list["FORM"]
                        , result_list["FORM_CODE"]
                        , result_list["BOILING"]
                        , result_list["TOXIC_SUBSTANCES_YN"]
                        , result_list["PROHIBITED_SUBSTANCES_YN"]
                        , result_list["PERMITTED_SUBSTANCES_YN"]
                        , result_list["TOXIC_MANAGEMENT_YN"]
                        , result_list["SPECIAL_CARE_YN"]
                        , result_list["WORK_ENVIRONMENT_YN"]
                        , result_list["WORK_ENVIRONMENT_CYCLE"]
                        , result_list["SPECIAL_HEALTH_YN"]
                        , result_list["SPECIAL_HEALTH_CYCLE"]
                        , result_list["EXPOSURE_SET_YN"]
                        , result_list["ACCEPTABLE_SET_YN"]
                        , result_list["PSM_YN"]
                        # , result_list["KOSHA_REVISION_DATE"]
                        , result_list["MOLECULAR"]
                        , result_list["ACUTE_TOXIC"]
                        , result_list["CHEMICAL_SUBSTANCE_ID"]
                        ]
        
        # substance_query=f"""
        #     UPDATE MSDS_CHEMICAL_SUBSTANCE SET 
        #                             CAS_NO=:1
        #                             , CHEM_ID=:2
        #                             , CHEM_NAME=:3
        #                             , UN_NO=:4
        #                             , EN_NO=:5
        #                             , KE_NO=:6
        #                             , NICKNAME=:7
                                    
        #                             , FLOW_RATE=:8
        #                             , CMR_C=:9
        #                             , CMR_M=:10
        #                             , CMR_R=:11
        #                             , EXPOSURE_TWA=:12
        #                             , UNIT=:13
        #                             , FORM=:14
        #                             , FORM_CODE=:15
        #                             , BOILING=:16
        #                             , TOXIC_SUBSTANCES_YN=:17
        #                             , PROHIBITED_SUBSTANCES_YN=:18
        #                             , PERMITTED_SUBSTANCES_YN=:19
        #                             , TOXIC_MANAGEMENT_YN=:20
        #                             , SPECIAL_CARE_YN=:21
        #                             , WORK_ENVIRONMENT_YN=:22
        #                             , WORK_ENVIRONMENT_CYCLE=:23
        #                             , SPECIAL_HEALTH_YN=:24
        #                             , SPECIAL_HEALTH_CYCLE=:25
        #                             , EXPOSURE_SET_YN=:26
        #                             , ACCEPTABLE_SET_YN=:27
        #                             , PSM_YN=:28
        #                             , KOSHA_REVISION_DATE=TO_DATE('{result_list["KOSHA_REVISION_DATE"]}','YYYY-MM-DD')
        #                             , ACTV='1'
        #                             , UPT_DATE=SYSDATE
        #                             , UPT_USER='{upt_user}'
        #                             , MOLECULAR=:29
        #                             , ACUTE_TOXIC=:30
        #     WHERE CHEMICAL_SUBSTANCE_ID=:31
        #                         """

        substance_query=f"""
            UPDATE MSDS_CHEMICAL_SUBSTANCE SET 
                                    CAS_NO=:1
                                    , CHEM_ID=:2
                                    , CHEM_NAME=:3
                                    , UN_NO=:4
                                    , EN_NO=:5
                                    , KE_NO=:6
                                    , NICKNAME=:7
                                    , NOVELIS_NICKNAME_KO=:8
                                    , NOVELIS_NICKNAME_EN=:9
                                    , FLOW_RATE=:10
                                    , CMR_C=:11
                                    , CMR_M=:12
                                    , CMR_R=:13
                                    , EXPOSURE_TWA=:14
                                    , UNIT=:15
                                    , FORM=:16
                                    , FORM_CODE=:17
                                    , BOILING=:18
                                    , TOXIC_SUBSTANCES_YN=:19
                                    , PROHIBITED_SUBSTANCES_YN=:20
                                    , PERMITTED_SUBSTANCES_YN=:21
                                    , TOXIC_MANAGEMENT_YN=:22
                                    , SPECIAL_CARE_YN=:23
                                    , WORK_ENVIRONMENT_YN=:24
                                    , WORK_ENVIRONMENT_CYCLE=:25
                                    , SPECIAL_HEALTH_YN=:26
                                    , SPECIAL_HEALTH_CYCLE=:27
                                    , EXPOSURE_SET_YN=:28
                                    , ACCEPTABLE_SET_YN=:29
                                    , PSM_YN=:30
                                    , KOSHA_REVISION_DATE=TO_DATE('{result_list["KOSHA_REVISION_DATE"]}','YYYY-MM-DD')
                                    , KOSHA_CREATE_DATE=TO_DATE('{result_list["KOSHA_CREATE_DATE"]}','YYYY-MM-DD')
                                    , ACTV='1'
                                    , UPT_DATE=SYSDATE
                                    , UPT_USER='{upt_user}'
                                    , MOLECULAR=:31
                                    , ACUTE_TOXIC=:32
            WHERE CHEMICAL_SUBSTANCE_ID=:33
                                """
        
        
        
        
        query_02=f"""INSERT INTO MSDS_CHEMICAL_ALERT (CHEMICAL_SUBSTANCE_ID 
                                                    , TYPE 
                                                    , SEQ 
                                                    , ALERT_TYPE 
                                                    , ALERT_CODE 
                                                    , ALERT 
                                                    , REG_DATE
                                                    , REG_USER
                                                    , UPT_DATE 
                                                    , UPT_USER
                                                    , ACTV ) VALUES (:1,:2,:3,:4,:5,:6,TO_DATE('{reg_date}','YYYY-MM-DD:HH24:MI:SS'),'{reg_user}',SYSDATE,'{upt_user}','1')"""

        query_09=f"""INSERT INTO MSDS_CHEMICAL_PROPERTIES (CHEMICAL_SUBSTANCE_ID 
                                    , TYPE 
                                    , SEQ 
                                    , TYPE_NAME 
                                    , VALUE 
                                    , UNIT
                                    , REG_DATE
                                    , REG_USER
                                    , UPT_DATE 
                                    , UPT_USER
                                    , ACTV )  VALUES (:1,:2,:3,:4,:5,:6,TO_DATE('{reg_date}','YYYY-MM-DD:HH24:MI:SS'),'{reg_user}',SYSDATE,'{upt_user}','1')"""

        query_11=f"""INSERT INTO MSDS_CHEMICAL_POISON (CHEMICAL_SUBSTANCE_ID 
                                    , TYPE 
                                    , SEQ 
                                    , TYPE_NAME 
                                    , TYPE_NAME2 
                                    , VALUE
                                    , ETC
                                    , REG_DATE
                                    , REG_USER
                                    , UPT_DATE 
                                    , UPT_USER
                                    , ACTV) VALUES (:1,:2,:3,:4,:5,:6,:7,TO_DATE('{reg_date}','YYYY-MM-DD:HH24:MI:SS'),'{reg_user}',SYSDATE,'{upt_user}','1')"""
        
        query_15=f"""INSERT INTO MSDS_CHEMICAL_REGULATION (CHEMICAL_SUBSTANCE_ID 
                                    , LAW_CODE 
                                    , SEQ 
                                    , LAW_NAME 
                                    , REGULATION_NAME 
                                    , NOTE
                                    , REMARK
                                    , REG_DATE
                                    , REG_USER
                                    , UPT_DATE 
                                    , UPT_USER
                                    , ACTV ) VALUES (:1,:2,:3,:4,:5,:6,:7,TO_DATE('{reg_date}','YYYY-MM-DD:HH24:MI:SS'),'{reg_user}',SYSDATE,'{upt_user}','1')"""
        
        query_08=f"""INSERT INTO MSDS_CHEMICAL_PROTECTOR (CHEMICAL_SUBSTANCE_ID 
                                    , TYPE 
                                    , SEQ 
                                    , TYPE_NAME 
                                    , TYPE_NAME2 
                                    , VALUE 
                                    , ETC
                                    , REG_DATE
                                    , REG_USER
                                    , UPT_DATE 
                                    , UPT_USER
                                    , ACTV ) VALUES (:1,:2,:3,:4,:5,:6,:7, TO_DATE('{reg_date}','YYYY-MM-DD:HH24:MI:SS'),'{reg_user}',SYSDATE,'{upt_user}','1')"""

        # print(substance_query)

        cur.execute(substance_query, substance_data)

        cur.execute(del_for_update_query("MSDS_CHEMICAL_ALERT",result_list["CHEMICAL_SUBSTANCE_ID"]))
        cur.executemany(query_02,insert_MSDS_CHEM_DETAIL02(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL02"]))
        cur.execute(del_for_update_query("MSDS_CHEMICAL_PROPERTIES",result_list["CHEMICAL_SUBSTANCE_ID"]))
        cur.executemany(query_09,insert_MSDS_CHEM_DETAIL09(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL09"]))
        cur.execute(del_for_update_query("MSDS_CHEMICAL_POISON",result_list["CHEMICAL_SUBSTANCE_ID"]))
        cur.executemany(query_11,insert_MSDS_CHEM_DETAIL11(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL11"]))
        cur.execute(del_for_update_query("MSDS_CHEMICAL_REGULATION",result_list["CHEMICAL_SUBSTANCE_ID"]))
        cur.executemany(query_15,insert_MSDS_CHEM_DETAIL15(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL15"]))
        cur.execute(del_for_update_query("MSDS_CHEMICAL_PROTECTOR",result_list["CHEMICAL_SUBSTANCE_ID"]))
        cur.executemany(query_08,insert_MSDS_CHEM_DETAIL08(result_list["CHEMICAL_SUBSTANCE_ID"],result_list["CHEM_DETAIL08"]))


        # RPHRASE QUERY

        check_query=f"SELECT * FROM MSDS_CHEMICAL_RPHRASE WHERE TYPE = '4' AND CHEMICAL_SUBSTANCE_ID = {result_list['CHEMICAL_SUBSTANCE_ID']}"
        
        cur.execute(check_query)
        result=cur.fetchall()
        check_data= result[0] if result else ""

        if check_data:
            del_query=f"DELETE FROM MSDS_CHEMICAL_RPHRASE WHERE TYPE = '4' AND CHEMICAL_SUBSTANCE_ID = {result_list['CHEMICAL_SUBSTANCE_ID']}"
            cur.execute(del_query)

        update_rphrase_query=f"""INSERT INTO MSDS_CHEMICAL_RPHRASE
                            (R_PHRASE, CHEMICAL_SUBSTANCE_ID, TYPE, SEQ, GRADE, TYPE_NM, POISON, REG_DATE, REG_USER, UPT_DATE, UPT_USER)
                        SELECT B.R_PHRASE, A.CHEMICAL_SUBSTANCE_ID, A.TYPE, A.SEQ, B.GRADE, '유해·위험문구(R_PHRASE)' AS TYPE_NM, B.POISON, TO_DATE('{reg_date}','YYYY-MM-DD:HH24:MI:SS'), '{reg_user}', SYSDATE, '{upt_user}'
                        FROM MSDS_CHEMICAL_ALERT A
                    INNER JOIN MSDS_R_PHRASE       B ON A.ALERT_CODE = B.H_CODE
                        WHERE A.ACTV = '1' 
                        AND A.TYPE = '4'
                        AND A.CHEMICAL_SUBSTANCE_ID = {result_list['CHEMICAL_SUBSTANCE_ID']}"""
        
        cur.execute(update_rphrase_query)
        
        oracleDB.commit()
        flag="00"
    
    except Exception as ex:
        print(ex)
        if oracleDB:
            oracleDB.rollback()
        return "82",Exception(f"{ex}")
    # finally:
    #     if oracleDB:
    #         oracleDB.close()
            

    return flag,None


def get_SEQ_MSDS_CHEMICAL_SUBSTANCE():
    chemical_substance_id =None
    flag="99"

    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()

        get_id_query="SELECT SEQ_MSDS_CHEMICAL_SUBSTANCE.NEXTVAL FROM DUAL"
        cur.execute(get_id_query)
        result=cur.fetchall()

        chemical_substance_id= result[0][0] if result else ""

        flag="00"

    except Exception as ora_e:
        if oracleDB:
            oracleDB.rollback()
        flag="82"
        return int(chemical_substance_id) if chemical_substance_id else "",flag,Exception(f"{ora_e}")
    finally:
        if oracleDB:
            oracleDB.close()

    return int(chemical_substance_id) if chemical_substance_id else "",flag,None

#####################################################

def save_cas_basic(cud,chemical_substance_id,chemlist,upt_user): #insert만 생각 / 기본데이터로 update안함
    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()

        result_list = {
                    "CHEMICAL_SUBSTANCE_ID": int(chemical_substance_id),
                    "CAS_NO":chemlist.get("casNo", "-") if chemlist['casNo'] not in NO_DATA else "-",
                    "CHEM_ID": chemlist.get("chemId", "-") if chemlist.get("chemId", "-") not in NO_DATA else "-",
                    "CHEM_NAME": chemlist.get("chemNameKor", "-") if chemlist.get("chemNameKor", "-") not in NO_DATA else "-",
                    "EN_NO": chemlist.get("enNo", "-") if chemlist.get("enNo", "-") not in NO_DATA else "-",
                    "KE_NO": chemlist.get("keNo", "-") if chemlist.get("keNo", "-") not in NO_DATA else "-",
                }
        
        basic_data=[result_list["CAS_NO"],result_list["CHEM_ID"],result_list["CHEM_NAME"],result_list["EN_NO"],result_list["KE_NO"],str(result_list["CHEMICAL_SUBSTANCE_ID"])]
        
        if cud=="I":

            insert_cas_basic_query=f"""INSERT INTO MSDS_CHEMICAL_SUBSTANCE (
                                    CHEMICAL_SUBSTANCE_ID
                                    , CAS_NO
                                    , CHEM_ID
                                    , CHEM_NAME
                                    , UN_NO
                                    , EN_NO
                                    , KE_NO


                                    , NICKNAME
                                    , NOVELIS_NICKNAME_KO
                                    , NOVELIS_NICKNAME_EN
                                    , FLOW_RATE
                                    , CMR_C
                                    , CMR_M
                                    , CMR_R

                                    , EXPOSURE_TWA
                                    , UNIT
                                    , FORM
                                    , FORM_CODE
                                    , BOILING

                                    , TOXIC_SUBSTANCES_YN
                                    , PROHIBITED_SUBSTANCES_YN
                                    , PERMITTED_SUBSTANCES_YN
                                    , TOXIC_MANAGEMENT_YN
                                    , SPECIAL_CARE_YN
                                    , WORK_ENVIRONMENT_YN
                                    , WORK_ENVIRONMENT_CYCLE
                                    , SPECIAL_HEALTH_YN
                                    , SPECIAL_HEALTH_CYCLE
                                    , EXPOSURE_SET_YN
                                    , ACCEPTABLE_SET_YN
                                    , PSM_YN
                                    , KOSHA_REVISION_DATE
                                    , KOSHA_CREATE_DATE
                                    , ACTV
                                    , REG_DATE
                                    , REG_USER
                                    , UPT_DATE
                                    , UPT_USER

                                    , MOLECULAR
                                    , ACUTE_TOXIC)
                                VALUES ( :6, :1, :2, :3, '-', :4, :5, '-', '-', '-', '100', '-', '-', '-', 
                                    '-','', '-','', '-',
                                    '0','0','0','0','0','0','','0','','0','0','0'
                                    , TO_DATE('','YYYY-MM-DD')
                                    , TO_DATE('','YYYY-MM-DD')
                                    , '1'
                                    , SYSDATE
                                    , '{upt_user}'
                                    , SYSDATE
                                    , '{upt_user}'
                                    , '-'
                                    , '0'
                                    )
                                    """
            
            cur.execute(insert_cas_basic_query,basic_data)

        else: #cud=="U"
            update_cas_basic_query=f"""
                UPDATE MSDS_CHEMICAL_SUBSTANCE SET
                                    CAS_NO=:1
                                    , CHEM_ID=:2
                                    , CHEM_NAME=:3
                                    , EN_NO=:4
                                    , KE_NO=:5

                                    , UN_NO='-'
                                    , NOVELIS_NICKNAME_KO='-'
                                    , NOVELIS_NICKNAME_EN='-'
                                    , FLOW_RATE='100'
                                    , CMR_C='-'
                                    , CMR_M='-'
                                    , CMR_R='-'
                                    , EXPOSURE_TWA='-'
                                    , UNIT=''
                                    , FORM='-'
                                    , FORM_CODE=''
                                    , BOILING='-'
                                    , TOXIC_SUBSTANCES_YN='0'
                                    , PROHIBITED_SUBSTANCES_YN='0'
                                    , PERMITTED_SUBSTANCES_YN='0'
                                    , TOXIC_MANAGEMENT_YN='0'
                                    , SPECIAL_CARE_YN='0'
                                    , WORK_ENVIRONMENT_YN='0'
                                    , WORK_ENVIRONMENT_CYCLE=''
                                    , SPECIAL_HEALTH_YN='0'
                                    , SPECIAL_HEALTH_CYCLE=''
                                    , EXPOSURE_SET_YN='0'
                                    , ACCEPTABLE_SET_YN='0'
                                    , PSM_YN='0'
                                    , KOSHA_REVISION_DATE=TO_DATE('','YYYY-MM-DD')
                                    , KOSHA_CREATE_DATE=TO_DATE('','YYYY-MM-DD')
                                    , ACTV='1'
                                    , UPT_DATE=SYSDATE
                                    , UPT_USER='{upt_user}'
                                    , MOLECULAR='-'
                                    , ACUTE_TOXIC='0'
                                WHERE CHEMICAL_SUBSTANCE_ID=:6
                                """
            
            cur.execute(update_cas_basic_query,basic_data)
            cur.execute(del_for_update_query("MSDS_CHEMICAL_ALERT",result_list["CHEMICAL_SUBSTANCE_ID"]))
            cur.execute(del_for_update_query("MSDS_CHEMICAL_PROPERTIES",result_list["CHEMICAL_SUBSTANCE_ID"]))
            cur.execute(del_for_update_query("MSDS_CHEMICAL_POISON",result_list["CHEMICAL_SUBSTANCE_ID"]))
            cur.execute(del_for_update_query("MSDS_CHEMICAL_REGULATION",result_list["CHEMICAL_SUBSTANCE_ID"]))
            cur.execute(del_for_update_query("MSDS_CHEMICAL_PROTECTOR",result_list["CHEMICAL_SUBSTANCE_ID"]))


            # RPHRASE QUERY DELETE
            del_query=f"DELETE FROM MSDS_CHEMICAL_RPHRASE WHERE TYPE = '4' AND CHEMICAL_SUBSTANCE_ID = {result_list['CHEMICAL_SUBSTANCE_ID']}"
            cur.execute(del_query)

        oracleDB.commit()

    except oracledb.Error as e:
        print(e)
        if oracleDB:
            oracleDB.rollback()
    finally:
        if oracleDB:
            oracleDB.close()





def msds(para,nov_ko=None,nov_en=None):
    # # print(para)
    # try:
    #     flag,oracleDB=get_db_conn()
    #     cur = oracleDB.cursor()
    #     # if flag!="00":
    #     #     return Exception(IF_ERROR_CODE["83"]), "83"
    # except Exception as ex:
    #     detail["resultCode"] = flag
    #     detail["resultMsg"] = IF_ERROR_CODE[flag]
    #     detail["resultDate"] = datetime.now().strftime(DATE_FORMAT)
    #     detail["resultData"] = f"{ex}"
        
    #     return detail
    ex=None
    cud=None
    result_list = []
    exist_cas=None
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
        result_list = []
        chemdetail = dictionary_template()
        detail = {
            "resultCode": None,
            "resultMsg": None,
            "resultDate": None,
            "resultData": None,
        }
        try:
            casno = parameter_dict["casno"]
            keno = parameter_dict["keno"]
            enno = parameter_dict["enno"]
            cud = parameter_dict["cud"]
            chemical_substance_id = parameter_dict["chemical_substance_id"]
            upt_user=parameter_dict["reg_user"] if "reg_user" in parameter_dict.keys() else "API_BTN"

            flag,oracleDB=get_db_conn()
            cur = oracleDB.cursor()

            #### 수기 입력 cas 처리 #### // update하는거 무조건 return // insert인 경우 구분할 수 없으니 error
            if cud=="U" and chemical_substance_id not in ["",None]: #update인 경우 return
                if int(chemical_substance_id) < 100000: #id있는데 그 값이 10만 미만인 경우 수기입력된 cas (1~)

                    detail["resultCode"] = flag
                    detail["resultMsg"] = IF_ERROR_CODE[flag]
                    detail["resultDate"] = datetime.now().strftime(DATE_FORMAT)
                    detail["resultData"] = f"그대로 RETURN | 수기입력 Update : {casno} {keno} {enno} {cud} {chemical_substance_id} {upt_user}"

                    insert_substance_if(casno,enno,keno,flag,IF_ERROR_CODE[flag],detail,upt_user)
                    return detail
                

            chemlist, flag, ex = get_chemId_api_four(casno,keno,enno)
            
            if cud not in ["I","i","U","u"]:
                flag='82'
                raise
            if flag != "00":
                raise chemlist
            else:
                chemId = chemlist.get("chemId")        
            
            if cud=="I" and chemical_substance_id in ["",None]: #insert인 경우 (id없는경우) 찾아서 가져오기
                chemical_substance_id,flag, ex=get_SEQ_MSDS_CHEMICAL_SUBSTANCE()
                if flag != "00":
                    raise
            
            exist_cas=True if chemlist and chemical_substance_id else None # API데이터 이상한 경우 기본데이터 넣기위함

            for i in chemdetail_list:
                chemdetail[i], flag, ex = get_chemDetail(chemId, i)
                if flag != "00":
                    raise

            result_list,flag, ex= json_data_handler(chemlist, chemdetail,chemical_substance_id)
            if flag != "00":
                raise

            #####nick 추가 부분#####
            result_list["NOVELIS_NICKNAME_KO"]=nov_ko if nov_ko not in ["",None] else "-"
            result_list["NOVELIS_NICKNAME_EN"]=nov_en if nov_en not in ["",None] else "-"

            # 데이터 바뀌기전 개정이력부터 insert // MSDS_REVISION_HISTORY
            flag,ex=msds_history.upsert_msds_history(oracleDB,cur,result_list,upt_user)
            if flag != "00":
                raise
            
            ####################################################################
            
            if cud == "I": # 기존 db에 없음 -> insert #http://192.168.10.83:8000/msds?casno=2465-27-2&cud=I&chemical_substance_id=
                                                       # select로 id값 찾아서 넣기
                flag, ex=insert_MSDS(oracleDB,cur,result_list,upt_user,result_list["NOVELIS_NICKNAME_KO"],result_list["NOVELIS_NICKNAME_EN"])
                if flag != "00":
                    raise
            

            if cud =="U": #기존 db에 있음 ->update #http://192.168.10.83:8000/msds?casno=2465-27-2&cud=U&chemical_substance_id=10005
                reg_date,reg_user,nov_ko,nov_en,flag, ex=find_reg_data(result_list)
                if flag != "00":
                    raise

                flag, ex=update_MSDS(oracleDB,cur,result_list,reg_date,upt_user,reg_user,nov_ko,nov_en)
                if flag != "00":
                    raise
            ####################################################################
                
                result_list["NOVELIS_NICKNAME_KO"]=nov_ko
                result_list["NOVELIS_NICKNAME_EN"]=nov_en

            if flag=="00":

                detail["resultCode"] = SUCCESS
                detail["resultMsg"] = SUCCESS_MSG
                detail["resultDate"] = datetime.now().strftime(DATE_FORMAT)
                detail["resultData"] = result_list

                insert_substance_if(casno,enno,keno,SUCCESS,SUCCESS_MSG,detail,upt_user)

            
        except Exception as e:
            print(e)
            print(casno,keno,enno)
            print(ex)
            if oracleDB:
                oracleDB.rollback()

            if exist_cas and flag not in ["82","83"]:
                save_cas_basic(cud,chemical_substance_id,chemlist,upt_user)
                flag="00"
                
            if ex:
                detail["resultCode"] = flag
                detail["resultMsg"] = IF_ERROR_CODE[flag]
                detail["resultDate"] = datetime.now().strftime(DATE_FORMAT)
                detail["resultData"] = f"{ex}"

                if flag not in ["82","83"]:
                    insert_substance_if(casno,enno,keno,flag,IF_ERROR_CODE[flag],detail,upt_user)

            else:
                detail["resultCode"] = flag
                detail["resultMsg"] = IF_ERROR_CODE[flag]
                detail["resultDate"] = datetime.now().strftime(DATE_FORMAT)
                detail["resultData"] = f"{e}"

                if flag not in ["82","83"]:
                    insert_substance_if(casno,enno,keno,flag,IF_ERROR_CODE[flag],detail,upt_user)


        finally:
            if oracleDB:
                oracleDB.close()



            return detail
        

