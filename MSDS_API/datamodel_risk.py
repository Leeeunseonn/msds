from pydantic import BaseModel, validator
from typing import Optional, Any, List


class msds_risk_Item(BaseModel): #input
    id: str
    year: str
    dept_list: list = []

class Item_msds_risk(BaseModel): #input
    """
    ajax 로 받는 데이터의 형식에 맞추기 위해
    {
        "data" : {..}
    }
    """
    data : msds_risk_Item

# measure data들과 형식 맞춰주기 위함
# {
#   "data": 
#     {
#       "id": "21000",
#       "year": "2024",
#       "dept_list": ["A204","A302"]
#     }
  
# }



# class msds_risk_copy_Item(BaseModel): #input
#     copy_list: list = []


class Item_msds_risk_copy(BaseModel): #input
    """
    ajax 로 받는 데이터의 형식에 맞추기 위해
    {
        "data" : {..}
    }
    """
    data : list = []


# # test 데이터
# {
#   "data": [{"LoginId":"KIMTAEU"
#             , "copyList":[{"AVG_RATE":"70.00", "BOILING":"270", "BTN_H_CODE":"../images/btn/btn_search.gif", "REFORM_MEASURES":"", "ELIMINATION":"1", "EXPOSURE_TWA":"-", "RISK_ID":"100050", "MOLECULAR":"-", "REFORM_DANGER_GRADE":"1", "VENTILATION":"", "MONTHLY_USE_UNIT":"ton", "REDUCTION_MEASURES":"", "CHEMICAL_USEPLANT_ID":"102160", "MANAGEMENT_LEVEL":"", "WORK_TIME":"2", "REFORM_DANGER_LEVEL":"L", "CMR_M":"-", "UNIT":"", "ARSENIC_GRADE":"", "BTN_R_PHRASE":"../images/btn/btn_search.gif", "USE_TEMP":"", "USE":"0.12", "MONTHLY_USE":"100", "TARGET_MATERIAL_ID":"100012", "ACUTE_TOXIC":"0", "USE_TIME":"단시간작업이란 관리대상 유해물질을 취급하는 시간이 1일 1시간 미만인 작업을 말한다. 다만, 1일 1시간 미만인 작업이 매일 수행되는 경우는 제외한다.", "USE_GRADE":"2(중)", "DANGER_GRADE":"", "HARMFUL_GRADE":"3", "CMR_R":"-", "VOLATILIZATION":"", "DANGER_LEVEL":"", "CHEM_NAME":"수소처리된 경질 정제유 (석유)(DISTILLATES (PETROLEUM), HYDROTREATED LIGHT)", "WORK_TYPE":"X", "ADMINISTRATIVE":"", "H_CODE":"H304", "EXPOSURE_GRADE":"", "R_PHRASE":"R25", "CMR_C":"-", "CHEMICAL_SUBSTANCE_ID":"100015", "FORM_CODE":"2", "ARSENIC_ACID":"", "VOLUME_UNIT":"ton", "SICK_YN":"0", "REFORM_HARMFUL_GRADE":"1", "ENGINEERING":"", "REFORM_EXPOSURE_GRADE":"1"}] 
#             , "newList":[{"RISK_YN":"1", "AVG_RATE":"70.00", "BOILING":"270", "BTN_H_CODE":"../images/btn/btn_search.gif", "PLANT_PRCS_ID":"21000", "HARMFUL_FACTOR_ID":"", "EXPOSURE_TWA":"-", "MOLECULAR":"-", "HARMFUL_FACTOR_UNIT":"", "CHEMICAL_USEPLANT_ID":"102160", "CMR_M":"-", "UNIT":"", "BTN_R_PHRASE":"../images/btn/btn_search.gif", "TARGET_MATERIAL_ID":"100012", "ACUTE_TOXIC":"0", "MEASURED":"", "CMR_R":"-", "CHEM_NAME":"수소처리된 경질 정제유 (석유)(DISTILLATES (PETROLEUM), HYDROTREATED LIGHT)", "H_CODE":"H304", "PLANT_PRCS_LVL3":"131", "PLANT_PRCS_LVL2":"21200", "R_PHRASE":"R25", "CMR_C":"-", "CHEMICAL_SUBSTANCE_ID":"100015", "FORM_CODE":"2", "SICK_YN":"0", "MEASUREMENT_YEAR":""}] 
#             , "risk_id":"202020"},
#             {"LoginId":"KIMTAEU"
#             , "copyList":[{"AVG_RATE":"10","BOILING":"270","BTN_H_CODE":"../images/btn/btn_search.gif","REFORM_MEASURES":"","ELIMINATION":"","EXPOSURE_TWA":"-","RISK_ID":"100048","MOLECULAR":"250.379","REFORM_DANGER_GRADE":"6","VENTILATION":"","MONTHLY_USE_UNIT":"ton","REDUCTION_MEASURES":"M","CHEMICAL_USEPLANT_ID":"101544","MANAGEMENT_LEVEL":"","WORK_TIME":"1","REFORM_DANGER_LEVEL":"M","CMR_M":"-","UNIT":"","ARSENIC_GRADE":"","BTN_R_PHRASE":"../images/btn/btn_search.gif","USE_TEMP":"50","USE":"0.02","MONTHLY_USE":"5","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"1","USE_TIME":"임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다.","USE_GRADE":"2(중)","DANGER_GRADE":"6","HARMFUL_GRADE":"2","CMR_R":"-","VOLATILIZATION":"2(중)","DANGER_LEVEL":"2","CHEM_NAME":"옥토시놀","WORK_TYPE":"X","ADMINISTRATIVE":"","H_CODE":"H302, H315, H319","EXPOSURE_GRADE":"3","R_PHRASE":"R22, R36, R38","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"100193","FORM_CODE":"2","ARSENIC_ACID":"","VOLUME_UNIT":"ton","SICK_YN":"0","REFORM_HARMFUL_GRADE":"2","ENGINEERING":"","REFORM_EXPOSURE_GRADE":"3"},
#                         {"AVG_RATE":"10","BOILING":"5900","BTN_H_CODE":"../images/btn/btn_search.gif","REFORM_MEASURES":"","ELIMINATION":"","EXPOSURE_TWA":"5","RISK_ID":"100048","MOLECULAR":"183.85","REFORM_DANGER_GRADE":"2","VENTILATION":"","MONTHLY_USE_UNIT":"ton","REDUCTION_MEASURES":"L","CHEMICAL_USEPLANT_ID":"101544","MANAGEMENT_LEVEL":"","WORK_TIME":"1","REFORM_DANGER_LEVEL":"L","CMR_M":"-","UNIT":"mg/m3","ARSENIC_GRADE":"2(중)","BTN_R_PHRASE":"../images/btn/btn_search.gif","USE_TEMP":"","USE":"0.02","MONTHLY_USE":"5","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"0","USE_TIME":"임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다.","USE_GRADE":"2(중)","DANGER_GRADE":"3","HARMFUL_GRADE":"1","CMR_R":"-","VOLATILIZATION":"","DANGER_LEVEL":"1","CHEM_NAME":"텅스텐","WORK_TYPE":"X","ADMINISTRATIVE":"2","H_CODE":"","EXPOSURE_GRADE":"3","R_PHRASE":"","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"100686","FORM_CODE":"1","ARSENIC_ACID":"2","VOLUME_UNIT":"ton","SICK_YN":"0","REFORM_HARMFUL_GRADE":"1","ENGINEERING":"4","REFORM_EXPOSURE_GRADE":"2.1"},
#                         {"AVG_RATE":"10","BOILING":"-21.2","BTN_H_CODE":"../images/btn/btn_search.gif","REFORM_MEASURES":"","ELIMINATION":"","EXPOSURE_TWA":"10","RISK_ID":"100048","MOLECULAR":"52.04","REFORM_DANGER_GRADE":"6","VENTILATION":"","MONTHLY_USE_UNIT":"ton","REDUCTION_MEASURES":"M","CHEMICAL_USEPLANT_ID":"101544","MANAGEMENT_LEVEL":"","WORK_TIME":"1","REFORM_DANGER_LEVEL":"M","CMR_M":"-","UNIT":"ppm","ARSENIC_GRADE":"","BTN_R_PHRASE":"../images/btn/btn_search.gif","USE_TEMP":"50","USE":"0.02","MONTHLY_USE":"5","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"1","USE_TIME":"임시작업이란 일시적으로 하는 작업 중 월 24시간 미만인 작업을 말한다. 다만 월 10시간 이상 24시간 미만인 작업이 매월 행하여지는 작업은 제외한다.","USE_GRADE":"2(중)","DANGER_GRADE":"6","HARMFUL_GRADE":"2","CMR_R":"-","VOLATILIZATION":"3(고)","DANGER_LEVEL":"2","CHEM_NAME":"시아노겐","WORK_TYPE":"X","ADMINISTRATIVE":"","H_CODE":"H330, H335","EXPOSURE_GRADE":"3","R_PHRASE":"R20, R26, R37","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"104471","FORM_CODE":"3","ARSENIC_ACID":"","VOLUME_UNIT":"ton","SICK_YN":"0","REFORM_HARMFUL_GRADE":"2","ENGINEERING":"","REFORM_EXPOSURE_GRADE":"3"}] 
#             , "newList":[{"RISK_YN":"1","AVG_RATE":"10","BOILING":"5900","BTN_H_CODE":"../images/btn/btn_search.gif","PLANT_PRCS_ID":"21000","HARMFUL_FACTOR_ID":"","EXPOSURE_TWA":"5","MOLECULAR":"183.85","HARMFUL_FACTOR_UNIT":"","CHEMICAL_USEPLANT_ID":"101544","CMR_M":"-","UNIT":"mg/m3","BTN_R_PHRASE":"../images/btn/btn_search.gif","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"0","MEASURED":"","CMR_R":"-","CHEM_NAME":"텅스텐","H_CODE":"","PLANT_PRCS_LVL3":"131","PLANT_PRCS_LVL2":"21200","R_PHRASE":"","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"100686","FORM_CODE":"1","SICK_YN":"0","MEASUREMENT_YEAR":""},
#                     {"RISK_YN":"1","AVG_RATE":"10","BOILING":"-","BTN_H_CODE":"../images/btn/btn_search.gif","PLANT_PRCS_ID":"21000","HARMFUL_FACTOR_ID":"","EXPOSURE_TWA":"-","MOLECULAR":"81.41","HARMFUL_FACTOR_UNIT":"","CHEMICAL_USEPLANT_ID":"101544","CMR_M":"-","UNIT":"","BTN_R_PHRASE":"../images/btn/btn_search.gif","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"0","MEASURED":"","CMR_R":"-","CHEM_NAME":"산화아연","H_CODE":"","PLANT_PRCS_LVL3":"131","PLANT_PRCS_LVL2":"21200","R_PHRASE":"","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"109970","FORM_CODE":"1","SICK_YN":"0","MEASUREMENT_YEAR":""},
#                     {"RISK_YN":"1","AVG_RATE":"10","BOILING":"-21.2","BTN_H_CODE":"../images/btn/btn_search.gif","PLANT_PRCS_ID":"21000","HARMFUL_FACTOR_ID":"","EXPOSURE_TWA":"10","MOLECULAR":"52.04","HARMFUL_FACTOR_UNIT":"","CHEMICAL_USEPLANT_ID":"101544","CMR_M":"-","UNIT":"ppm","BTN_R_PHRASE":"../images/btn/btn_search.gif","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"1","MEASURED":"","CMR_R":"-","CHEM_NAME":"시아노겐","H_CODE":"H330, H335","PLANT_PRCS_LVL3":"131","PLANT_PRCS_LVL2":"21200","R_PHRASE":"R20, R26, R37","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"104471","FORM_CODE":"3","SICK_YN":"0","MEASUREMENT_YEAR":""},
#                     {"RISK_YN":"1","AVG_RATE":"10","BOILING":"270","BTN_H_CODE":"../images/btn/btn_search.gif","PLANT_PRCS_ID":"21000","HARMFUL_FACTOR_ID":"","EXPOSURE_TWA":"-","MOLECULAR":"250.379","HARMFUL_FACTOR_UNIT":"","CHEMICAL_USEPLANT_ID":"101544","CMR_M":"-","UNIT":"","BTN_R_PHRASE":"../images/btn/btn_search.gif","TARGET_MATERIAL_ID":"101513","ACUTE_TOXIC":"1","MEASURED":"","CMR_R":"-","CHEM_NAME":"옥토시놀","H_CODE":"H302, H315, H319","PLANT_PRCS_LVL3":"131","PLANT_PRCS_LVL2":"21200","R_PHRASE":"R22, R36, R38","CMR_C":"-","CHEMICAL_SUBSTANCE_ID":"100193","FORM_CODE":"2","SICK_YN":"0","MEASUREMENT_YEAR":""}] 
#             , "risk_id":"202021"} 
#         ]
# }




