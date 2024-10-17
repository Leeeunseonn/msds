
import oracledb
import requests
import re

from MSDS_upsert import *

### 조건
# [{LoginId=KIMTAEU -> reg_user
# , copyList=[{AVG_RATE=70.00, BOILING=270, BTN_H_CODE=../images/btn/btn_search.gif, REFORM_MEASURES=null, ELIMINATION=1, EXPOSURE_TWA=-, RISK_ID=100050, MOLECULAR=-, REFORM_DANGER_GRADE=1, VENTILATION=null, MONTHLY_USE_UNIT=ton, REDUCTION_MEASURES=null, CHEMICAL_USEPLANT_ID=102160, MANAGEMENT_LEVEL=null, WORK_TIME=2, REFORM_DANGER_LEVEL=L, CMR_M=-, UNIT=null, ARSENIC_GRADE=null, BTN_R_PHRASE=../images/btn/btn_search.gif, USE_TEMP=null, USE=0.12, MONTHLY_USE=100, TARGET_MATERIAL_ID=100012, ACUTE_TOXIC=0, USE_TIME=단시간작업이란 관리대상 유해물질을 취급하는 시간이 1일 1시간 미만인 작업을 말한다. 다만, 1일 1시간 미만인 작업이 매일 수행되는 경우는 제외한다., USE_GRADE=2(중), DANGER_GRADE=null, HARMFUL_GRADE=3, CMR_R=-, VOLATILIZATION=null, DANGER_LEVEL=null, CHEM_NAME=수소처리된 경질 정제유 (석유)(DISTILLATES (PETROLEUM), HYDROTREATED LIGHT), WORK_TYPE=X, ADMINISTRATIVE=null, H_CODE=H304, EXPOSURE_GRADE=null, R_PHRASE=R25, CMR_C=-, CHEMICAL_SUBSTANCE_ID=100015, FORM_CODE=2, ARSENIC_ACID=null, VOLUME_UNIT=ton, SICK_YN=0, REFORM_HARMFUL_GRADE=1, ENGINEERING=null, REFORM_EXPOSURE_GRADE=1}] 
# , newList=[{RISK_YN=1, AVG_RATE=70.00, BOILING=270, BTN_H_CODE=../images/btn/btn_search.gif, PLANT_PRCS_ID=21000, HARMFUL_FACTOR_ID=null, EXPOSURE_TWA=-, MOLECULAR=-, HARMFUL_FACTOR_UNIT=null, CHEMICAL_USEPLANT_ID=102160, CMR_M=-, UNIT=null, BTN_R_PHRASE=../images/btn/btn_search.gif, TARGET_MATERIAL_ID=100012, ACUTE_TOXIC=0, MEASURED=null, CMR_R=-, CHEM_NAME=수소처리된 경질 정제유 (석유)(DISTILLATES (PETROLEUM), HYDROTREATED LIGHT), H_CODE=H304, PLANT_PRCS_LVL3=131, PLANT_PRCS_LVL2=21200, R_PHRASE=R25, CMR_C=-, CHEMICAL_SUBSTANCE_ID=100015, FORM_CODE=2, SICK_YN=0, MEASUREMENT_YEAR=null}] 
# , risk_id=100062 => risk_id는 이걸로 List안에 있는거는 무시하기
# } 

# 결재 하나당 {} copyList=> 작업환경측정결과 없는경우 기존data, newList=> 새로운 데이터 / 작업환경측정결과없는경우 내용이 계속 변할 수 있어서 있는거만 copyList에서 가져와서 newList에 넣고 DB(msds_risk_nonworkenv)에 데이터넣기
# 사용자가 직접 작성하는 데이터만 집어넣기

# {LoginId=KIMTAEU 
# 옥토시놀# , copyList=[{AVG_RATE=10, BOILING=270, BTN_H_CODE=../images/btn/btn_search.gif, REFORM_MEASURES=null, ELIMINATION=null, EXPOSURE_TWA=-, RISK_ID=100048, MOLECULAR=250.379, REFORM_DANGER_GRADE=6, VENTILATION=null, MONTHLY_USE_UNIT=ton, REDUCTION_MEASURES=M, CHEMICAL_USEPLANT_ID=101544, MANAGEMENT_LEVEL=null, WORK_TIME=1, REFORM_DANGER_LEVEL=M, CMR_M=-, UNIT=null, ARSENIC_GRADE=null, BTN_R_PHRASE=../images/btn/btn_search.gif, USE_TEMP=50, USE=0.02, MONTHLY_USE=5, TARGET_MATERIAL_ID=101513, ACUTE_TOXIC=1, USE_TIME=임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만, 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다., USE_GRADE=2(중), DANGER_GRADE=6, HARMFUL_GRADE=2, CMR_R=-, VOLATILIZATION=2(중), DANGER_LEVEL=2, CHEM_NAME=옥토시놀, WORK_TYPE=X, ADMINISTRATIVE=null, H_CODE=H302, H315, H319, EXPOSURE_GRADE=3, R_PHRASE=R22, R36, R38, CMR_C=-, CHEMICAL_SUBSTANCE_ID=100193, FORM_CODE=2, ARSENIC_ACID=null, VOLUME_UNIT=ton, SICK_YN=0, REFORM_HARMFUL_GRADE=2, ENGINEERING=null, REFORM_EXPOSURE_GRADE=3},
# 텅스텐#                 {AVG_RATE=10, BOILING=5900, BTN_H_CODE=../images/btn/btn_search.gif, REFORM_MEASURES=null, ELIMINATION=null, EXPOSURE_TWA=5, RISK_ID=100048, MOLECULAR=183.85, REFORM_DANGER_GRADE=2, VENTILATION=null, MONTHLY_USE_UNIT=ton, REDUCTION_MEASURES=L, CHEMICAL_USEPLANT_ID=101544, MANAGEMENT_LEVEL=null, WORK_TIME=1, REFORM_DANGER_LEVEL=L, CMR_M=-, UNIT=mg/m3, ARSENIC_GRADE=2(중), BTN_R_PHRASE=../images/btn/btn_search.gif, USE_TEMP=null, USE=0.02, MONTHLY_USE=5, TARGET_MATERIAL_ID=101513, ACUTE_TOXIC=0, USE_TIME=임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만, 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다., USE_GRADE=2(중), DANGER_GRADE=3, HARMFUL_GRADE=1, CMR_R=-, VOLATILIZATION=null, DANGER_LEVEL=1, CHEM_NAME=텅스텐, WORK_TYPE=X, ADMINISTRATIVE=2, H_CODE=null, EXPOSURE_GRADE=3, R_PHRASE=null, CMR_C=-, CHEMICAL_SUBSTANCE_ID=100686, FORM_CODE=1, ARSENIC_ACID=2, VOLUME_UNIT=ton, SICK_YN=0, REFORM_HARMFUL_GRADE=1, ENGINEERING=4, REFORM_EXPOSURE_GRADE=2.1}, 
# 시아노겐#               {AVG_RATE=10, BOILING=-21.2, BTN_H_CODE=../images/btn/btn_search.gif, REFORM_MEASURES=null, ELIMINATION=null, EXPOSURE_TWA=10, RISK_ID=100048, MOLECULAR=52.04, REFORM_DANGER_GRADE=6, VENTILATION=null, MONTHLY_USE_UNIT=ton, REDUCTION_MEASURES=M, CHEMICAL_USEPLANT_ID=101544, MANAGEMENT_LEVEL=null, WORK_TIME=1, REFORM_DANGER_LEVEL=M, CMR_M=-, UNIT=ppm, ARSENIC_GRADE=null, BTN_R_PHRASE=../images/btn/btn_search.gif, USE_TEMP=50, USE=0.02, MONTHLY_USE=5, TARGET_MATERIAL_ID=101513, ACUTE_TOXIC=1, USE_TIME=임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만, 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다., USE_GRADE=2(중), DANGER_GRADE=6, HARMFUL_GRADE=2, CMR_R=-, VOLATILIZATION=3(고), DANGER_LEVEL=2, CHEM_NAME=시아노겐, WORK_TYPE=X, ADMINISTRATIVE=null, H_CODE=H330, H335, EXPOSURE_GRADE=3, R_PHRASE=R20, R26, R37, CMR_C=-, CHEMICAL_SUBSTANCE_ID=104471, FORM_CODE=3, ARSENIC_ACID=null, VOLUME_UNIT=ton, SICK_YN=0, REFORM_HARMFUL_GRADE=2, ENGINEERING=null, REFORM_EXPOSURE_GRADE=3}] 


#####100048

# 텅스텐# , newList=[{RISK_YN=1, AVG_RATE=10, BOILING=5900, BTN_H_CODE=../images/btn/btn_search.gif, PLANT_PRCS_ID=21000, HARMFUL_FACTOR_ID=null, EXPOSURE_TWA=5, MOLECULAR=183.85, HARMFUL_FACTOR_UNIT=null, CHEMICAL_USEPLANT_ID=101544, CMR_M=-, UNIT=mg/m3, BTN_R_PHRASE=../images/btn/btn_search.gif, TARGET_MATERIAL_ID=101513, ACUTE_TOXIC=0, MEASURED=null, CMR_R=-, CHEM_NAME=텅스텐, H_CODE=null, PLANT_PRCS_LVL3=131, PLANT_PRCS_LVL2=21200, R_PHRASE=null, CMR_C=-, CHEMICAL_SUBSTANCE_ID=100686, FORM_CODE=1, SICK_YN=0, MEASUREMENT_YEAR=null},
# 산화아연#               {RISK_YN=1, AVG_RATE=10, BOILING=-, BTN_H_CODE=../images/btn/btn_search.gif, PLANT_PRCS_ID=21000, HARMFUL_FACTOR_ID=null, EXPOSURE_TWA=-, MOLECULAR=81.41, HARMFUL_FACTOR_UNIT=null, CHEMICAL_USEPLANT_ID=101544, CMR_M=-, UNIT=null, BTN_R_PHRASE=../images/btn/btn_search.gif, TARGET_MATERIAL_ID=101513, ACUTE_TOXIC=0, MEASURED=null, CMR_R=-, CHEM_NAME=산화아연, H_CODE=null, PLANT_PRCS_LVL3=131, PLANT_PRCS_LVL2=21200, R_PHRASE=null, CMR_C=-, CHEMICAL_SUBSTANCE_ID=109970, FORM_CODE=1, SICK_YN=0, MEASUREMENT_YEAR=null},
# 시아노겐#               {RISK_YN=1, AVG_RATE=10, BOILING=-21.2, BTN_H_CODE=../images/btn/btn_search.gif, PLANT_PRCS_ID=21000, HARMFUL_FACTOR_ID=null, EXPOSURE_TWA=10, MOLECULAR=52.04, HARMFUL_FACTOR_UNIT=null, CHEMICAL_USEPLANT_ID=101544, CMR_M=-, UNIT=ppm, BTN_R_PHRASE=../images/btn/btn_search.gif, TARGET_MATERIAL_ID=101513, ACUTE_TOXIC=1, MEASURED=null, CMR_R=-, CHEM_NAME=시아노겐, H_CODE=H330, H335, PLANT_PRCS_LVL3=131, PLANT_PRCS_LVL2=21200, R_PHRASE=R20, R26, R37, CMR_C=-, CHEMICAL_SUBSTANCE_ID=104471, FORM_CODE=3, SICK_YN=0, MEASUREMENT_YEAR=null},
# 옥토시놀#               {RISK_YN=1, AVG_RATE=10, BOILING=270, BTN_H_CODE=../images/btn/btn_search.gif, PLANT_PRCS_ID=21000, HARMFUL_FACTOR_ID=null, EXPOSURE_TWA=-, MOLECULAR=250.379, HARMFUL_FACTOR_UNIT=null, CHEMICAL_USEPLANT_ID=101544, CMR_M=-, UNIT=null, BTN_R_PHRASE=../images/btn/btn_search.gif, TARGET_MATERIAL_ID=101513, ACUTE_TOXIC=1, MEASURED=null, CMR_R=-, CHEM_NAME=옥토시놀, H_CODE=H302, H315, H319, PLANT_PRCS_LVL3=131, PLANT_PRCS_LVL2=21200, R_PHRASE=R22, R36, R38, CMR_C=-, CHEMICAL_SUBSTANCE_ID=100193, FORM_CODE=2, SICK_YN=0, MEASUREMENT_YEAR=null}] 
# , risk_id=100063 
# } 
# ] 

# insert_copy_query="""
#             INSERT INTO MSDS_RISK_NONWORKENV( 

#             RISK_ID 
#             ,CHEMICAL_SUBSTANCE_ID 
#             ,AVG_RATE 
#             ,EXPOSURE_TWA 
#             ,FORM_CODE 
            
#             ,CMR_C 
#             ,CMR_M 
#             ,CMR_R 
#             ,ELIMINATION #
#             ,ENGINEERING #
            
#             ,ADMINISTRATIVE #
#             ,USE #
#             ,VOLUME_UNIT #
#             ,WORK_TIME #
#             ,USE_TIME #
            
#             ,USE_TEMP #
#             ,BOILING 
#             ,VOLATILIZATION #
#             ,ARSENIC_ACID #
#             ,ARSENIC_GRADE #
            
#             ,EXPOSURE_GRADE #
#             ,HARMFUL_GRADE #있는 data지만 copy에서 가져오기
#             ,DANGER_GRADE #
#             ,DANGER_LEVEL #
#             ,MANAGEMENT_LEVEL #
            
#             ,REFORM_MEASURES #
#             ,REFORM_EXPOSURE_GRADE #
#             ,REFORM_HARMFUL_GRADE #
#             ,REFORM_DANGER_GRADE #
#             ,REFORM_DANGER_LEVEL #
            
#             ,REDUCTION_MEASURES #
#             ,REMARK null
#             ,REG_DATE sysdate
#             ,REG_USER LoginId
#             ,USE_GRADE #
            
#             ,UNIT
#             ,MOLECULAR 
#             ,ACUTE_TOXIC 
#             ,VENTILATION #
        
#         )VALUES( 
#         :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38
#         ) 
#             """

insert_no_dup_copy_query="""
            INSERT INTO MSDS_RISK_NONWORKENV( 

            RISK_ID 
            ,CHEMICAL_SUBSTANCE_ID 
            ,AVG_RATE 
            ,EXPOSURE_TWA 
            ,FORM_CODE 
            
            ,CMR_C 
            ,CMR_M 
            ,CMR_R 

            ,BOILING 

            ,REMARK
            ,REG_DATE
            ,REG_USER
            
            ,UNIT
            ,MOLECULAR 
            ,ACUTE_TOXIC 
        
        )VALUES( 
        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, SYSDATE, :11, :12, :13, :14
        ) 
            """

insert_dup_copy_query="""
            INSERT INTO MSDS_RISK_NONWORKENV( 

            RISK_ID 
            ,CHEMICAL_SUBSTANCE_ID 
            ,AVG_RATE 
            ,EXPOSURE_TWA 
            ,FORM_CODE 
            
            ,CMR_C 
            ,CMR_M 
            ,CMR_R 
            ,ELIMINATION
            ,ENGINEERING
            
            ,ADMINISTRATIVE
            ,USE
            ,VOLUME_UNIT
            ,WORK_TIME
            ,USE_TIME
            
            ,USE_TEMP
            ,BOILING 
            ,VOLATILIZATION
            ,ARSENIC_ACID
            ,ARSENIC_GRADE
            
            ,EXPOSURE_GRADE
            ,HARMFUL_GRADE
            ,DANGER_GRADE
            ,DANGER_LEVEL
            ,MANAGEMENT_LEVEL
            
            ,REFORM_MEASURES
            ,REFORM_EXPOSURE_GRADE
            ,REFORM_HARMFUL_GRADE
            ,REFORM_DANGER_GRADE
            ,REFORM_DANGER_LEVEL
            
            ,REDUCTION_MEASURES
            ,REMARK
            ,REG_DATE
            ,REG_USER
            ,USE_GRADE
            
            ,UNIT
            ,MOLECULAR 
            ,ACUTE_TOXIC 
            ,VENTILATION
        
        )VALUES( 
        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, SYSDATE, :33, :34, :35, :36, :37, :38
        ) 
            """


def reform_type(s:str):
    try:
        if s:
            res=int(s)
        else:
            return s
    except ValueError:
        res=float(s)

    return res


def msds_risk_copy(item):
    
    # print(id,year,dept_list)
    # print(item)

    flag="fail"

    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()
        if item:

            item = item.dict()['data'] if type(item)!=dict else item['data']

            for pay in item:
                pay['copy_seqlist']={int(i.get('CHEMICAL_SUBSTANCE_ID')):i for i in pay['copyList']}
                pay['new_seqlist']={int(i.get('CHEMICAL_SUBSTANCE_ID')):i for i in pay['newList']}


                for chem,v in pay['new_seqlist'].items():
                    if chem in pay['copy_seqlist'].keys(): # chemical_substance_id 중복되는 것이 있는 경우
                        # v-> new / pay[copy_seqlist][chem]['BOILING'] -> copy
                        dup_insert_data=[int(pay['risk_id']),
                                        int(chem),
                                        v['AVG_RATE'] if 'AVG_RATE' in v else (pay['copy_seqlist'][chem]['AVG_RATE'] if 'AVG_RATE' in pay['copy_seqlist'][chem] else ""),
                                        v['EXPOSURE_TWA'] if 'EXPOSURE_TWA' in v else (pay['copy_seqlist'][chem]['EXPOSURE_TWA'] if 'EXPOSURE_TWA' in pay['copy_seqlist'][chem] else "-"),
                                        v['FORM_CODE'] if 'FORM_CODE' in v else (pay['copy_seqlist'][chem]['FORM_CODE'] if 'FORM_CODE' in pay['copy_seqlist'][chem] else ""),

                                        v['CMR_C'] if 'CMR_C' in v else (pay['copy_seqlist'][chem]['CMR_C'] if 'CMR_C' in pay['copy_seqlist'][chem] else "-"),
                                        v['CMR_M'] if 'CMR_M' in v else (pay['copy_seqlist'][chem]['CMR_M'] if 'CMR_M' in pay['copy_seqlist'][chem] else "-"),
                                        v['CMR_R'] if 'CMR_R' in v else (pay['copy_seqlist'][chem]['CMR_R'] if 'CMR_R' in pay['copy_seqlist'][chem] else "-"),
                                        v['ELIMINATION'] if 'ELIMINATION' in v else (pay['copy_seqlist'][chem]['ELIMINATION'] if 'ELIMINATION' in pay['copy_seqlist'][chem] else ""),
                                        v['ENGINEERING'] if 'ENGINEERING' in v else (pay['copy_seqlist'][chem]['ENGINEERING'] if 'ENGINEERING' in pay['copy_seqlist'][chem] else ""),

                                        v['ADMINISTRATIVE'] if 'ADMINISTRATIVE' in v else (pay['copy_seqlist'][chem]['ADMINISTRATIVE'] if 'ADMINISTRATIVE' in pay['copy_seqlist'][chem] else ""),
                                        reform_type(v['USE']) if 'USE' in v else (reform_type(pay['copy_seqlist'][chem]['USE']) if 'USE' in pay['copy_seqlist'][chem] else ""),
                                        v['VOLUME_UNIT'] if 'VOLUME_UNIT' in v else (pay['copy_seqlist'][chem]['VOLUME_UNIT'] if 'VOLUME_UNIT' in pay['copy_seqlist'][chem] else ""),
                                        v['WORK_TIME'] if 'WORK_TIME' in v else (pay['copy_seqlist'][chem]['WORK_TIME'] if 'WORK_TIME' in pay['copy_seqlist'][chem] else ""),
                                        v['USE_TIME'] if 'USE_TIME' in v else (pay['copy_seqlist'][chem]['USE_TIME'] if 'USE_TIME' in pay['copy_seqlist'][chem] else ""),

                                        v['USE_TEMP'] if 'USE_TEMP' in v else (pay['copy_seqlist'][chem]['USE_TEMP'] if 'USE_TEMP' in pay['copy_seqlist'][chem] else ""),
                                        v['BOILING'] if 'BOILING' in v else (pay['copy_seqlist'][chem]['BOILING'] if 'BOILING' in pay['copy_seqlist'][chem] else "-"),
                                        v['VOLATILIZATION'] if 'VOLATILIZATION' in v else (pay['copy_seqlist'][chem]['VOLATILIZATION'] if 'VOLATILIZATION' in pay['copy_seqlist'][chem] else ""),
                                        v['ARSENIC_ACID'] if 'ARSENIC_ACID' in v else (pay['copy_seqlist'][chem]['ARSENIC_ACID'] if 'ARSENIC_ACID' in pay['copy_seqlist'][chem] else ""),
                                        v['ARSENIC_GRADE'] if 'ARSENIC_GRADE' in v else (pay['copy_seqlist'][chem]['ARSENIC_GRADE'] if 'ARSENIC_GRADE' in pay['copy_seqlist'][chem] else ""),

                                        reform_type(v['EXPOSURE_GRADE']) if 'EXPOSURE_GRADE' in v else (reform_type(pay['copy_seqlist'][chem]['EXPOSURE_GRADE']) if 'EXPOSURE_GRADE' in pay['copy_seqlist'][chem] else ""),
                                        reform_type(v['HARMFUL_GRADE']) if 'HARMFUL_GRADE' in v else (reform_type(pay['copy_seqlist'][chem]['HARMFUL_GRADE']) if 'HARMFUL_GRADE' in pay['copy_seqlist'][chem] else ""),
                                        reform_type(v['DANGER_GRADE']) if 'DANGER_GRADE' in v else (reform_type(pay['copy_seqlist'][chem]['DANGER_GRADE']) if 'DANGER_GRADE' in pay['copy_seqlist'][chem] else ""),
                                        v['DANGER_LEVEL'] if 'DANGER_LEVEL' in v else (pay['copy_seqlist'][chem]['DANGER_LEVEL'] if 'DANGER_LEVEL' in pay['copy_seqlist'][chem] else ""),
                                        v['MANAGEMENT_LEVEL'] if 'MANAGEMENT_LEVEL' in v else (pay['copy_seqlist'][chem]['MANAGEMENT_LEVEL'] if 'MANAGEMENT_LEVEL' in pay['copy_seqlist'][chem] else ""),

                                        v['REFORM_MEASURES'] if 'REFORM_MEASURES' in v else (pay['copy_seqlist'][chem]['REFORM_MEASURES'] if 'REFORM_MEASURES' in pay['copy_seqlist'][chem] else ""),
                                        reform_type(v['REFORM_EXPOSURE_GRADE']) if 'REFORM_EXPOSURE_GRADE' in v else (reform_type(pay['copy_seqlist'][chem]['REFORM_EXPOSURE_GRADE']) if 'REFORM_EXPOSURE_GRADE' in pay['copy_seqlist'][chem] else ""),
                                        reform_type(v['REFORM_HARMFUL_GRADE']) if 'REFORM_HARMFUL_GRADE' in v else (reform_type(pay['copy_seqlist'][chem]['REFORM_HARMFUL_GRADE']) if 'REFORM_HARMFUL_GRADE' in pay['copy_seqlist'][chem] else ""),
                                        reform_type(v['REFORM_DANGER_GRADE']) if 'REFORM_DANGER_GRADE' in v else (reform_type(pay['copy_seqlist'][chem]['REFORM_DANGER_GRADE']) if 'REFORM_DANGER_GRADE' in pay['copy_seqlist'][chem] else ""),
                                        v['REFORM_DANGER_LEVEL'] if 'REFORM_DANGER_LEVEL' in v else (pay['copy_seqlist'][chem]['REFORM_DANGER_LEVEL'] if 'REFORM_DANGER_LEVEL' in pay['copy_seqlist'][chem] else ""),

                                        v['REDUCTION_MEASURES'] if 'REDUCTION_MEASURES' in v else (pay['copy_seqlist'][chem]['REDUCTION_MEASURES'] if 'REDUCTION_MEASURES' in pay['copy_seqlist'][chem] else ""),
                                        v['REMARK'] if 'REMARK' in v else "",
                                        #  SYSDATE,
                                        str(pay['LoginId']),
                                        v['USE_GRADE'] if 'USE_GRADE' in v else (pay['copy_seqlist'][chem]['USE_GRADE'] if 'USE_GRADE' in pay['copy_seqlist'][chem] else ""),

                                        v['UNIT'] if 'UNIT' in v else (pay['copy_seqlist'][chem]['UNIT'] if 'UNIT' in pay['copy_seqlist'][chem] else ""),
                                        v['MOLECULAR'] if 'MOLECULAR' in v else (pay['copy_seqlist'][chem]['MOLECULAR'] if 'MOLECULAR' in pay['copy_seqlist'][chem] else "-"),
                                        v['ACUTE_TOXIC'] if 'ACUTE_TOXIC' in v else (pay['copy_seqlist'][chem]['ACUTE_TOXIC'] if 'ACUTE_TOXIC' in pay['copy_seqlist'][chem] else ""),
                                        v['VENTILATION'] if 'VENTILATION' in v else (pay['copy_seqlist'][chem]['VENTILATION'] if 'VENTILATION' in pay['copy_seqlist'][chem] else "")
                                        ]
                        
                        # dup_insert_data =["" if i in ['Null',None] else i for i in dup_insert_data]
                        
                        cur.execute(insert_dup_copy_query,dup_insert_data)
                        oracleDB.commit()
                        flag="00"


                    else: #중복x 빈데이터는 빈채로 저장
                        no_dup_insert_data=[int(pay['risk_id']),
                                            int(chem),
                                            v['AVG_RATE'] if 'AVG_RATE' in v else "-",
                                            v['EXPOSURE_TWA'] if 'EXPOSURE_TWA' in v else "-",
                                            v['FORM_CODE'] if 'FORM_CODE' in v else "",

                                            v['CMR_C'] if 'CMR_C' in v else "-",
                                            v['CMR_M'] if 'CMR_M' in v else "-",
                                            v['CMR_R'] if 'CMR_R' in v else "-",

                                            v['BOILING'] if 'BOILING' in v else "-",
                                            
                                            v['REMARK'] if 'REMARK' in v else "",
                                            #  SYSDATE,
                                            str(pay['LoginId']),
                                            
                                            v['UNIT'] if 'UNIT' in v else "",
                                            v['MOLECULAR'] if 'MOLECULAR' in v else "-",
                                            v['ACUTE_TOXIC'] if 'ACUTE_TOXIC' in v else "",
 
                                            ]
                        # no_dup_insert_data =["" if i in ['Null',None] else i for i in no_dup_insert_data]

                        cur.execute(insert_no_dup_copy_query,no_dup_insert_data)
                        oracleDB.commit()
                        flag="00"


    except Exception as ora_e:
        if oracleDB:
            oracleDB.rollback()
        flag="99"
        return {"flag": flag, "data": ""}
    
    finally:
        if oracleDB:
            oracleDB.close()

    return {"flag": flag, "data": ""}



# if __name__=="__main__":

#     cl=[{"LoginId":"KIMTAEU"
#             , "copyList":[{"AVG_RATE":"70.00", "BOILING":"270", "BTN_H_CODE":"../images/btn/btn_search.gif", "REFORM_MEASURES":"", "EXPOSURE_TWA":"-", "RISK_ID":"100050", "MOLECULAR":"-", "REFORM_DANGER_GRADE":"1", "VENTILATION":"", "MONTHLY_USE_UNIT":"ton", "REDUCTION_MEASURES":"", "CHEMICAL_USEPLANT_ID":"102160", "MANAGEMENT_LEVEL":"", "WORK_TIME":"2", "REFORM_DANGER_LEVEL":"L", "CMR_M":"-", "UNIT":"", "ARSENIC_GRADE":"", "BTN_R_PHRASE":"../images/btn/btn_search.gif", "USE_TEMP":"", "USE":"0.12", "MONTHLY_USE":"100", "TARGET_MATERIAL_ID":"100012", "ACUTE_TOXIC":"0", "USE_TIME":"단시간작업이란 관리대상 유해물질을 취급하는 시간이 1일 1시간 미만인 작업을 말한다. 다만, 1일 1시간 미만인 작업이 매일 수행되는 경우는 제외한다.", "USE_GRADE":"2(중)", "DANGER_GRADE":"", "HARMFUL_GRADE":"3", "CMR_R":"-", "VOLATILIZATION":"", "DANGER_LEVEL":"", "CHEM_NAME":"수소처리된 경질 정제유 (석유)(DISTILLATES (PETROLEUM), HYDROTREATED LIGHT)", "WORK_TYPE":"X", "ADMINISTRATIVE":"", "H_CODE":"H304", "EXPOSURE_GRADE":"", "R_PHRASE":"R25", "CMR_C":"-", "CHEMICAL_SUBSTANCE_ID":"100015", "FORM_CODE":"2", "ARSENIC_ACID":"", "VOLUME_UNIT":"ton", "SICK_YN":"0", "REFORM_HARMFUL_GRADE":"1", "ENGINEERING":"", "REFORM_EXPOSURE_GRADE":"1"}] 
#             , "newList":[{"RISK_YN":"1", "AVG_RATE":"70.00", "BOILING":"270", "BTN_H_CODE":"../images/btn/btn_search.gif", "PLANT_PRCS_ID":"21000", "HARMFUL_FACTOR_ID":"", "EXPOSURE_TWA":"-", "MOLECULAR":"-", "HARMFUL_FACTOR_UNIT":"", "CHEMICAL_USEPLANT_ID":"102160", "CMR_M":"-", "UNIT":"", "BTN_R_PHRASE":"../images/btn/btn_search.gif", "TARGET_MATERIAL_ID":"100012", "ACUTE_TOXIC":"0", "MEASURED":"", "CMR_R":"-", "CHEM_NAME":"수소처리된 경질 정제유 (석유)(DISTILLATES (PETROLEUM), HYDROTREATED LIGHT)", "H_CODE":"H304", "PLANT_PRCS_LVL3":"131", "PLANT_PRCS_LVL2":"21200", "R_PHRASE":"R25", "CMR_C":"-", "CHEMICAL_SUBSTANCE_ID":"100015", "FORM_CODE":"2", "SICK_YN":"0", "MEASUREMENT_YEAR":""}] 
#             , "risk_id":"202020"},
#             {"LoginId":"KIMTAEU"
#             , "copyList":[{"AVG_RATE":"10","BTN_H_CODE":"../images/btn/btn_search.gif","REFORM_MEASURES":"","RISK_ID":"100048","MOLECULAR":"250.379","MONTHLY_USE_UNIT":"ton","REDUCTION_MEASURES":"M","CHEMICAL_USEPLANT_ID":"101544","MANAGEMENT_LEVEL":"","WORK_TIME":"1","REFORM_DANGER_LEVEL":"M","CMR_M":"-","UNIT":"","ARSENIC_GRADE":"","BTN_R_PHRASE":"../images/btn/btn_search.gif","USE_TEMP":"50","USE":"0.02","MONTHLY_USE":"5","TARGET_MATERIAL_ID":"101513","USE_TIME":"임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다.","USE_GRADE":"2(중)","DANGER_GRADE":"6","HARMFUL_GRADE":"2","CMR_R":"-","VOLATILIZATION":"2(중)","DANGER_LEVEL":"2","CHEM_NAME":"옥토시놀","WORK_TYPE":"X","ADMINISTRATIVE":"","H_CODE":"H302, H315, H319","EXPOSURE_GRADE":"3","R_PHRASE":"R22, R36, R38","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"100193","FORM_CODE":"2","ARSENIC_ACID":"","VOLUME_UNIT":"ton","SICK_YN":"0","REFORM_HARMFUL_GRADE":"2","REFORM_EXPOSURE_GRADE":"3"},
#                         {"AVG_RATE":"10","BOILING":"5900","BTN_H_CODE":"../images/btn/btn_search.gif","REFORM_MEASURES":"","ELIMINATION":"","EXPOSURE_TWA":"5","RISK_ID":"100048","MOLECULAR":"183.85","REFORM_DANGER_GRADE":"2","VENTILATION":"","MONTHLY_USE_UNIT":"ton","REDUCTION_MEASURES":"L","CHEMICAL_USEPLANT_ID":"101544","MANAGEMENT_LEVEL":"","WORK_TIME":"1","REFORM_DANGER_LEVEL":"L","CMR_M":"-","UNIT":"mg/m3","ARSENIC_GRADE":"2(중)","BTN_R_PHRASE":"../images/btn/btn_search.gif","USE_TEMP":"","USE":"0.02","MONTHLY_USE":"5","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"0","USE_TIME":"임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다.","USE_GRADE":"2(중)","DANGER_GRADE":"3","HARMFUL_GRADE":"1","CMR_R":"-","VOLATILIZATION":"","DANGER_LEVEL":"1","CHEM_NAME":"텅스텐","WORK_TYPE":"X","ADMINISTRATIVE":"2","H_CODE":"","EXPOSURE_GRADE":"3","R_PHRASE":"","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"100686","FORM_CODE":"1","ARSENIC_ACID":"2","VOLUME_UNIT":"ton","SICK_YN":"0","REFORM_HARMFUL_GRADE":"1","ENGINEERING":"4","REFORM_EXPOSURE_GRADE":"2.1"},
#                         {"AVG_RATE":"10","BOILING":"-21.2","BTN_H_CODE":"../images/btn/btn_search.gif","REFORM_MEASURES":"","ELIMINATION":"","EXPOSURE_TWA":"10","RISK_ID":"100048","MOLECULAR":"52.04","REFORM_DANGER_GRADE":"6","VENTILATION":"","MONTHLY_USE_UNIT":"ton","REDUCTION_MEASURES":"M","CHEMICAL_USEPLANT_ID":"101544","MANAGEMENT_LEVEL":"","WORK_TIME":"1","REFORM_DANGER_LEVEL":"M","CMR_M":"-","UNIT":"ppm","ARSENIC_GRADE":"","BTN_R_PHRASE":"../images/btn/btn_search.gif","USE_TEMP":"50","USE":"0.02","MONTHLY_USE":"5","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"1","USE_TIME":"임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다.","USE_GRADE":"2(중)","DANGER_GRADE":"6","HARMFUL_GRADE":"2","CMR_R":"-","VOLATILIZATION":"3(고)","DANGER_LEVEL":"2","CHEM_NAME":"시아노겐","WORK_TYPE":"X","ADMINISTRATIVE":"","H_CODE":"H330, H335","EXPOSURE_GRADE":"3","R_PHRASE":"R20, R26, R37","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"104471","FORM_CODE":"3","ARSENIC_ACID":"","VOLUME_UNIT":"ton","SICK_YN":"0","REFORM_HARMFUL_GRADE":"2","ENGINEERING":"","REFORM_EXPOSURE_GRADE":"3"}] 
#             , "newList":[{"RISK_YN":"1","AVG_RATE":"10","BOILING":"5900","BTN_H_CODE":"../images/btn/btn_search.gif","PLANT_PRCS_ID":"21000","HARMFUL_FACTOR_ID":"","EXPOSURE_TWA":"5","MOLECULAR":"183.85","HARMFUL_FACTOR_UNIT":"","CHEMICAL_USEPLANT_ID":"101544","CMR_M":"-","UNIT":"mg/m3","BTN_R_PHRASE":"../images/btn/btn_search.gif","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"0","MEASURED":"","CMR_R":"-","CHEM_NAME":"텅스텐","H_CODE":"","PLANT_PRCS_LVL3":"131","PLANT_PRCS_LVL2":"21200","R_PHRASE":"","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"100686","FORM_CODE":"1","SICK_YN":"0","MEASUREMENT_YEAR":""},
#                     {"RISK_YN":"1","AVG_RATE":"10","BOILING":"-","BTN_H_CODE":"../images/btn/btn_search.gif","PLANT_PRCS_ID":"21000","HARMFUL_FACTOR_ID":"","EXPOSURE_TWA":"-","MOLECULAR":"81.41","HARMFUL_FACTOR_UNIT":"","CHEMICAL_USEPLANT_ID":"101544","CMR_M":"-","UNIT":"","BTN_R_PHRASE":"../images/btn/btn_search.gif","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"0","MEASURED":"","CMR_R":"-","CHEM_NAME":"산화아연","H_CODE":"","PLANT_PRCS_LVL3":"131","PLANT_PRCS_LVL2":"21200","R_PHRASE":"","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"109970","FORM_CODE":"1","SICK_YN":"0","MEASUREMENT_YEAR":""},
#                     {"RISK_YN":"1","AVG_RATE":"10","BOILING":"-21.2","BTN_H_CODE":"../images/btn/btn_search.gif","PLANT_PRCS_ID":"21000","HARMFUL_FACTOR_ID":"","EXPOSURE_TWA":"10","MOLECULAR":"52.04","HARMFUL_FACTOR_UNIT":"","CHEMICAL_USEPLANT_ID":"101544","CMR_M":"-","UNIT":"ppm","BTN_R_PHRASE":"../images/btn/btn_search.gif","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"1","MEASURED":"","CMR_R":"-","CHEM_NAME":"시아노겐","H_CODE":"H330, H335","PLANT_PRCS_LVL3":"131","PLANT_PRCS_LVL2":"21200","R_PHRASE":"R20, R26, R37","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"104471","FORM_CODE":"3","SICK_YN":"0","MEASUREMENT_YEAR":""},
#                     {"RISK_YN":"1","AVG_RATE":"10","BOILING":"270","BTN_H_CODE":"../images/btn/btn_search.gif","PLANT_PRCS_ID":"21000","HARMFUL_FACTOR_ID":"","EXPOSURE_TWA":"-","MOLECULAR":"250.379","HARMFUL_FACTOR_UNIT":"","CHEMICAL_USEPLANT_ID":"101544","CMR_M":"-","UNIT":"","BTN_R_PHRASE":"../images/btn/btn_search.gif","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"1","MEASURED":"","CMR_R":"-","CHEM_NAME":"옥토시놀","H_CODE":"H302, H315, H319","PLANT_PRCS_LVL3":"131","PLANT_PRCS_LVL2":"21200","R_PHRASE":"R22, R36, R38","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"100193","FORM_CODE":"2","SICK_YN":"0","MEASUREMENT_YEAR":""}] 
#             , "risk_id":"202021"} 
#         ]
    
#     cl={"data":cl}

#     a=msds_risk_copy(cl)

#     print(a)