from datamodel import *
from config import Global_static_data
import unicodedata
import re
from decimal import Decimal

st_val = Global_static_data() # 전역변수 모음 클래스 선언

def normalize_unit(unit : str):
    """
    문자 정규화 함수
    mg/m³ , ㎎/㎥ 등 문자 모두 mg/m3 으로 변환
    """
    if unit:
        unit = unit.lower()
        # if any(['피'in list(unit), 'p'in list(unit), '엠'in list(unit), 'm'in list(unit)]):
        #     unit = 'ppm'
        return unicodedata.normalize('NFKC', unit)
    else:
        return unit

def roundTraditional(val : float, digits : int):
    # python 에서 round 함수는 사사오입이 아님
    # 무조건 사사오입으로 반올림을 하기 위해서 val에 소숫점의 길이에 따라 변하는 아주 작은 값 추가
    return round(val+10**(-len(str(val))-1), digits)
        
class Unit_check:
    """
    물질의 성상과 단위 확인하는 클래스
    성상과 단위의 매핑 확인을 위해 노출기준, 노출기준 단위, 성상, 분자량, 측정치, 측정치 단위 확인
    이후 노출 수준과 유해성 등급 결정 클래스가 해당 클래스 상속받아 사용
    MEASURE_FLAG 와 SICK_YN 은 해당 클래스에서 직접 쓰이지 않고 상속 후 flag 용도로 사용
    """
    def __init__(self, req_json : dict): 
        self.twa = req_json.get('EXPOSURE_TWA') # 노출기준
        self.twa_unit = req_json.get('UNIT') # 노출기준 단위
        self.form_code = req_json.get('FORM_CODE') # 성상
        self.mol = req_json.get('MOLECULAR') # 분자량
        self.measure = req_json.get('MEASURED') # 측정치
        self.measure_unit = req_json.get('HARMFUL_FACTOR_UNIT') # 측정치 단위
        self.acute_toxic = req_json.get('ACUTE_TOXIC') # 급성독성여부
        self.measure_flag = req_json.get('MEASURE_FLAG') # api flag
        self.d1 = req_json.get('SICK_YN') # D1 발생여부

        if self.measure_unit:
            self.measure_unit = normalize_unit(self.measure_unit)

        # if self.measure_flag == 'N':
        #     self.work_time = req_json.get('WORK_TIME') # 하루 사용 시간
        #     if self.work_time not in ['8', 8]:
        #         self.twa = self.work_time_correction(self.twa, self.work_time, self.acute_toxic)

    
    def form_twa_check(self):
        # 성상과 단위 데이터가 있는지 확인
        if ((self.form_code in st_val.chemical_state_list)
            and
            (self.twa_unit in st_val.twa_unit_list)):
            # 성상과 단위가 일치 한다면 그대로 반환 ( 기준은 form_code )
            if st_val.chemical_twa.get(self.form_code) == self.twa_unit:
                return self.twa, self.twa_unit
            # 성상과 단위가 일치하지 않는다면 ( 기준은 form_code )
            else:
                # mol 값 있으면 단위 변환
                if self.mol:
                    form_code = int(self.form_code)
                    mol = float(self.mol)
                    if form_code in [st_val.solid_int, st_val.liquid_int]:
                        twa = roundTraditional((self.twa * (mol/24.45)),2)
                        twa_unit = st_val.solid_unit
                        return twa, twa_unit
                    elif form_code in [st_val.gas_int]:
                        twa = roundTraditional((self.twa / (mol/24.45)),2)
                        twa_unit = st_val.gas_unit
                        return twa, twa_unit
                # 성상과 단위가 일치하지 않는데 mol 값 없다면 방법 3,4로 넘어가야 하므로 강제 오류
                else:
                    raise
        # 성상과 단위 데이터가 없다면 계산 불가
        else:
            raise

    def unit_measure_unit_check(self):
        # measure_y 에서만 실행
        # 성상 단위와 측정 단위 비교 ( 기준은 성상 단위 )
        if self.twa_unit == self.measure_unit:
            return self.measure, self.measure_unit
        else:
            if self.mol and self.measure:
                mol = float(self.mol)
                measure = float(self.measure)
                if self.twa_unit in [st_val.solid_unit, st_val.liquid_unit]:
                    measure = roundTraditional((measure * (mol/24.45)),2)
                    measure_unit = st_val.solid_unit
                    return measure, measure_unit
                elif self.twa_unit in [st_val.gas_unit]:
                    measure = roundTraditional((measure / (mol/24.45)),2)
                    measure_unit = st_val.gas_unit
                    return measure, measure_unit
            # 성상 단위와 측정 단위가 일치하지 않는데 mol 값이나 측정값이 없다면 노출수준 측정 불가
            else:
                raise
    
    # @staticmethod
    # def work_time_correction(twa, work_time, acute_toxic): # 하루 사용 시간 보정 함수
    #     try:
    #         float(work_time)
    #     except ValueError:
    #         return twa
    #     else:
    #         work_time = float(work_time)
    #         try:
    #             if twa:
    #                 twa = float(twa)
    #                 if acute_toxic in [1,'1']:
    #                     twa = twa * (8/work_time)
    #                 else:
    #                     twa = twa * (44/(work_time*5))
    #                 return twa
    #             else:
    #                 return ''
    #         except ZeroDivisionError:
    #             return twa

class now_dan_measuer(Unit_check):
    """
    노출수준 확인 클래스
    meaaure_flag가 'Y' 의 경우 super class 에서 상속받은 값 만으로 계산됨
    measure_flag가 'N' 의 경우 사용시간, 급성독성여부, 취급량, 단위, 온도, 끓는점, 비산성, 환기 상태에 따라 결정됨

    """
    def __init__(self, req_json):
        super().__init__(req_json)
        if self.measure_flag == 'N':
            self.avg_rate = req_json.get('AVG_RATE') # 함량
            self.use = req_json.get('USE') # 하루 취급량
            self.volume_unit = req_json.get('VOLUME_UNIT') # 단위
            self.monthly_use = req_json.get('MONTHLY_USE') # 월취급량
            self.monthly_use_unit  = req_json.get('MONTHLY_USE_UNIT') # 월취급량 단위
            self.use_temp = req_json.get('USE_TEMP') # 휘발성 사용온도
            self.boiling = req_json.get('BOILING') # 휘발성 끓는점
            self.arsenic = req_json.get('ARSENIC_ACID') # 비산정도
            self.ventilation = req_json.get('VENTILATION') # 밀폐/환기상태
            # self.use_grade = self.day_use_lvl(self.use, self.volume_unit) # 하루 취급량 레벨
            # self.volatilization = self.boiling_lvl(self.use_temp, self.boiling) # 끓는점 레벨
            # self.arsenic_grade = self.arsenic_lvl(self.arsenic) # 비산성 레벨
            if self.monthly_use and self.avg_rate:
                # self.monthly_use = req_json.get('MONTHLY_USE') # 월취급량

                self.use = round(((float(self.monthly_use) / 30) * float(self.avg_rate)) / 100 ,5)

                # self.monthly_use_unit  = req_json.get('MONTHLY_USE_UNIT') # 월취급량 단위

                self.volume_unit = self.monthly_use_unit

            self.use_grade = self.day_use_lvl(self.use, self.volume_unit) # 하루 취급량 레벨
            self.volatilization = self.boiling_lvl(self.use_temp, self.boiling) # 끓는점 레벨
            self.arsenic_grade = self.arsenic_lvl(self.arsenic) # 비산성 레벨


    @staticmethod
    def day_use_lvl(use, volume_unit): # 하루 취급량 레벨 함수
        if use and volume_unit:
            try:
                float(use)
            except ValueError:
                return ''
            else:
                use = float(use)

                if volume_unit in ['Kg', 'l']:
                    use *= 1000
                elif volume_unit in ['ton', 'm3']:
                    use *= 1000000
                elif volume_unit in ['g', 'Ml']:
                    pass
                else:
                    return '' # error

                if use < 1000: # lower 1kg
                    return '1(저)'
                elif use < 1000000: # lower 1ton
                    return '2(중)'
                else: # over 1 ton
                    return '3(고)'
        else:
            return ''
        
    @staticmethod
    def boiling_lvl(use_temp, boiling): # 끓는점 레벨 함수
        try:
            float(use_temp)
            float(boiling)
        except ValueError:
            return '1(저)'
        else:
            use_temp = float(use_temp)
            boiling = float(boiling)
            if use_temp == 20:
                if boiling < 50:
                    return '3(고)'
                elif boiling < 150:
                    return '2(중)'
                else:
                    return '1(저)'
            else:
                if boiling < (2*use_temp + 10):
                    return '3(고)'
                elif boiling < (5*use_temp + 50):
                    return '2(중)'
                else:
                    return '1(저)'
                
    @staticmethod
    def arsenic_lvl(arsenic): # 비산성 레벨 함수
        # if _arsenic == '미세하고 가벼운 분말로 취급 시 먼지 구름 형성되는 경우':
        if arsenic in [3,'3']:
            return '3(고)'
        # elif _arsenic == '결정형 입상으로 취급 시 먼지가 보이나 쉽게 가라앉는 경우':
        elif arsenic in [2,'2']:
            return '2(중)'
        # elif _arsenic == '부스러지지 않는 고체로 취급 중에 거의 먼지가 보이지 않는 경우':
        elif arsenic in [1,'1']:
            return '1(저)'
        else:
            return ''
         
    def now_dan_measure_y(self): # measure_flag='Y' 의 경우 실행되는 노출수준 확인 함수
        grade = ''
        """    
        measure : 측정치
        d1 : 직업병 유소견자(d1) 발생여부 -> 1 or '1'이면 무조건 4등급
        twa : 노출기준

        노출수준(%) = 측정치 / 노출기준(TWA) * 100%
        return 값 : 관리수준, 노출수준 등급 (0.2%, 1등급)
        """
        if self.d1 in ['1',1]:
            grade = 4

        try:
            float(self.twa)
        except ValueError:
            result = ''
            return result, grade
        
        try:
            self.measure, self.measure_unit = self.unit_measure_unit_check() # 노출기준과 측정치 단위 매핑 확인
        except Exception:
            result = ''
            return result, grade
        
        try:
            float(self.measure)
        except ValueError:
            if self.measure in ['흔적' or '불검출']: # 흔적이나 불검출 이라면 0으로 처리
                self.measure = 0
            else:
                result = ''
                return result, grade
        finally:
            twa = float(self.twa)
            measure = float(self.measure)
            try:
                result = roundTraditional(((measure * 100) / twa),1)
            except ZeroDivisionError: # 만약 노출기준이 0이라면 4 반환 (확실하진 않음)
                result = 4

            if grade == 4:
                return result, grade

            if result < 10:
                grade = 1
                return result, grade
            elif result < 50:
                grade = 2
                return result, grade
            elif result < 100:
                grade = 3
                return result, grade
            else:
                grade = 4
                return result, grade

    def now_dan_measure_n(self): # measure_flag='N' 의 경우 실행되는 노출수준 확인 함수
        if self.d1 in ['1',1]:
            return 4
        else:
            if self.use_grade and (self.volatilization or self.arsenic_grade):

                # 하위 3개 식 모두 self의 use_grade 를 직접적으로 변환하니 다른 변수명으로 저장
                use_grade = re.sub('[^0-9]','',self.use_grade)
                volatilization = re.sub('[^0-9]','',self.volatilization)
                arsenic_grade = re.sub('[^0-9]','',self.arsenic_grade)

                if volatilization: # 휘발성이면
                    use_grade = int(use_grade)
                    volatilization = int(volatilization)
                    if self.ventilation in ["2", 2, "1", 1]:
                        val = st_val.vol_arr[use_grade][volatilization] - int(self.ventilation)
                        if val < 1:
                            return 1
                        else:
                            return val
                    else:
                        return st_val.vol_arr[use_grade][volatilization]
                elif arsenic_grade: # 비산성이면
                    use_grade = int(use_grade)
                    arsenic_grade = int(arsenic_grade)
                    if self.ventilation in ["2", 2, "1", 1]:
                        val = st_val.ars_arr[use_grade][arsenic_grade] - int(self.ventilation)
                        if val < 1:
                            return 1
                        else:
                            return val
                    else:
                        return st_val.ars_arr[use_grade][arsenic_grade]
            else:
                return ''


# 유해성 등급 결정
class Severity_4lvl(Unit_check):
    """
    유해성 등급 결정 클래스
    1단계는 CMR 유무
    2단계는 노출기준(super class 에서 상속받은걸로 계산)
    3단계는 R_phrase
    4단계는 H_code
    """
    def __init__(self, req_json : dict):
        super().__init__(req_json)
        self.cmr_c = req_json.get('CMR_C') # CMR 발암성
        self.cmr_m = req_json.get('CMR_M') # CMR 생식세포변이
        self.cmr_r = req_json.get('CMR_R') # CMR 생식독성
        self.r_code = req_json.get('R_PHRASE') # 위험문구(R-phrase)
        self.h_code = req_json.get('H_CODE') # 유해위험문구(H-code)

    @staticmethod
    def u_dan_grade(twa, form_code): # 유해성 등급 -> 노출기준 성상 둘 다 있다는 가정하에 시작되는 함수
        # twa : 노출기준
        # form_code : 성상

        twa = float(twa)
        form_code = int(form_code)
        if form_code == st_val.solid_int:
            twa *= 50
        else:
            pass
        if twa > 500:
            raise
        elif twa > 50:
            return 1
        elif twa > 5:
            return 2
        elif twa > 0.5:
            return 3
        else:
            return 4
        
    @staticmethod
    def code_check(input_list): # R,H 코드 매핑 함수

        input_list = input_list.split(',')
        list_to_set = set(input_list)
        find_key = {
            key: st_val.rh_code.get(key) 
            for key in list_to_set 
            if st_val.rh_code.get(key)
            }
        if find_key:
            return max({st_val.rh_code_lvl.get(key) for key in find_key.values()})
        else:
            return 1

    def severity_4lvl(self):
        """
        유해성 등급 최종 결정하는 함수
        먼저 CMR 확인 후 CMR 물질이면 무조건 4등급
        CMR 물질 아니라면 노출기준 확인
        노출기준 확인시 성상과 노출기준의 단위 확인
        단위가 불일치 하면 단위 환산식을 이용해 변환
        단위가 불일치한데 분자량값(MOL) 이 없거나
        노출기준치를 초과 (분진 10mg/m3 초과, 증기 500ppm 초과) 하면 방법 3으로
        방법 3, 4 전부 해당되지 않는다면 1등급
        """        
        if any(x in st_val.cmr_list for x in [
            self.cmr_c, 
            self.cmr_m, 
            self.cmr_r]):
            return 4
        else:
            if self.twa and self.form_code and self.twa_unit:
                try:
                    self.twa, self.twa_unit = self.form_twa_check()
                    result = self.u_dan_grade(self.twa, self.form_code)
                    return result
                except Exception:
                    if self.r_code:
                        return self.code_check(self.r_code)
                    elif self.h_code:
                        return self.code_check(self.h_code)
                    else:
                        return 1
            else:
                if self.r_code:
                    return self.code_check(self.r_code)
                elif self.h_code:
                    return self.code_check(self.h_code)
                else:
                    return 1
                

# 위험성 등급, input 값 = 노출수준, 유해위험 등급
def dan_grade(exposure_grade, harmful_grade):
    try:
        int(exposure_grade) * int(harmful_grade)
    except ValueError:
        return ''
    else:
        return int(exposure_grade) * int(harmful_grade)
    
def dan_grade_after(exposure_grade, harmful_grade):
    try:
        float(exposure_grade) * float(harmful_grade)
    except ValueError:
        return ''
    else:
        return round(float(Decimal(str(exposure_grade)) * Decimal(str(harmful_grade))),0)

#위험성 수준
def dan_grade_sujun(danger_grade): # 1 이상 6미만 -L , 6이상 9미만 M , 9이상 H 위험성 수준 전 후 둘다(현재, 개선후) 공통 적용.
    # if danger_grade < 1:
    #     key = '' # "경미한 위험"
    # elif danger_grade < 6:
    #     key = 1 # "상당한 위험"
    # elif danger_grade < 9 :
    #     key = 2 # "중대한 위험"
    # else:
    #     key = 3
    # return key
# def dan_grade_sujun(danger_grade):
    if danger_grade in [1,2,3,4]:
        key = 'L' # "경미한 위험"
    elif danger_grade in [6,8]:
        key = 'M' # "상당한 위험"
    elif danger_grade in [9,12,16]:
        key = 'H' # "중대한 위험"
    else:
        key = ''
    return key
#위험성 수준
def dan_grade_sujun_after(danger_grade):
    if danger_grade < 1:
        key = '' # "경미한 위험"
    elif danger_grade < 6:
        key = 'L' # "상당한 위험"
    elif danger_grade < 9 :
        key = 'M' # "중대한 위험"
    else:
        key = 'H'
    return key


def improvements(req_json):
    # REFORM_EXPOSURE_GRADE:  Optional[Any] # 개선후 위험성 노출수준 등급
    # REFORM_HARMFUL_GRADE:  Optional[Any] # 개선후 위험성 유해성 등급
    # REFORM_DANGER_LEVEL:  Optional[Any] # 개선후 위험성 위험성 등급
    if req_json['ELIMINATION']:
        if int(req_json['ELIMINATION']) == 1:
            req_json['REFORM_EXPOSURE_GRADE'] = 1
            req_json['REFORM_HARMFUL_GRADE'] = 1
            req_json['REFORM_DANGER_GRADE'] = 1
            req_json['REFORM_DANGER_LEVEL'] = dan_grade_sujun_after(1)

    else:
        if req_json['EXPOSURE_GRADE'] and req_json['HARMFUL_GRADE']:

            if req_json['ENGINEERING_RATE'] and req_json['ADMINISTRATIVE_RATE']:
                res_imp2 = round((req_json['EXPOSURE_GRADE'] - (req_json['ENGINEERING_RATE']/100 * (req_json['EXPOSURE_GRADE'] - 1))),1)
                res_imp3 = round((res_imp2 - (req_json['ADMINISTRATIVE_RATE']/100 * (res_imp2 - 1))),1)

            elif req_json['ENGINEERING_RATE'] and not req_json['ADMINISTRATIVE_RATE']:
                res_imp2 = round((req_json['EXPOSURE_GRADE'] - (req_json['ENGINEERING_RATE']/100 * (req_json['EXPOSURE_GRADE'] - 1))),1)
                res_imp3 = res_imp2

            elif not req_json['ENGINEERING_RATE'] and req_json['ADMINISTRATIVE_RATE']:
                res_imp2 = req_json['EXPOSURE_GRADE']
                res_imp3 = round((res_imp2 - (req_json['ADMINISTRATIVE_RATE']/100 * (res_imp2 - 1))),1)

            elif not req_json['ENGINEERING_RATE'] and not req_json['ADMINISTRATIVE_RATE']:
                res_imp2 = req_json['EXPOSURE_GRADE']
                res_imp3 = res_imp2

            if res_imp3 <= 1:
                res_imp3 = 1

            req_json['REFORM_EXPOSURE_GRADE'] = res_imp3
            req_json['REFORM_HARMFUL_GRADE'] = req_json['HARMFUL_GRADE'] # 조치1항이 없으면 동일값
            req_json['REFORM_DANGER_GRADE'] = dan_grade_after(req_json['REFORM_EXPOSURE_GRADE'], req_json['HARMFUL_GRADE'])
            req_json['REFORM_DANGER_LEVEL'] = dan_grade_sujun_after(req_json['REFORM_DANGER_GRADE'])

def measure_y(item):
    if item:
        item = item.dict()['data']
        for i, req_json in enumerate(item):

            severity = Severity_4lvl(req_json)
            now_dan = now_dan_measuer(req_json)

            harmful_grade = severity.severity_4lvl() # 유해성 등급
            exposure, exposure_grade = now_dan.now_dan_measure_y() # 노출수준 관리수준, 등급 (ex 0.2% , 1등급 )
            danger_grade = dan_grade(exposure_grade, harmful_grade) # 위험성 등급

            req_json['EXPOSURE'] = exposure # 노출수준 관리수준
            req_json['EXPOSURE_GRADE'] = exposure_grade # 노출수준 등급
            req_json['HARMFUL_GRADE'] = harmful_grade # 유해성 등급
            req_json['DANGER_GRADE'] = danger_grade # 위험성 등급
            req_json['DANGER_LEVEL'] = dan_grade_sujun(danger_grade) # 위험성 수준

            improvements(req_json)

            item[i] = measure_y_item(**req_json)
            print(item[i])
        item = Item_measure_y(**{"data" : item})

        item

    return item

def measure_n(item):
    if item:
        item = item.dict()['data']
        for i, req_json in enumerate(item):

            severity = Severity_4lvl(req_json)
            now_dan = now_dan_measuer(req_json)

            harmful_grade = severity.severity_4lvl() # 유해성 등급
            exposure_grade = now_dan.now_dan_measure_n() # 노출수준 관리등급 (ex 1등급 )
            danger_grade = dan_grade(exposure_grade, harmful_grade) # 위험성 등급

            req_json['USE'] = now_dan.use # 사용량
            req_json['VOLUME_UNIT'] = now_dan.volume_unit
            req_json['USE_GRADE'] = now_dan.use_grade # 취급량 레벨
            req_json['VOLATILIZATION'] = now_dan.volatilization # 휘발성 레벨
            req_json['ARSENIC_GRADE'] = now_dan.arsenic_grade # 비산성
            req_json['EXPOSURE_GRADE'] = exposure_grade # 노출수준 등급
            req_json['HARMFUL_GRADE'] = harmful_grade # 유해성 등급
            req_json['DANGER_GRADE'] = danger_grade # 위험성 등급
            req_json['DANGER_LEVEL'] = dan_grade_sujun(danger_grade) # 위험성 수준

            improvements(req_json)

            item[i] = measure_n_item(**req_json)
            print(item[i])
        item = Item_measure_n(**{"data" : item})

        item

    return item