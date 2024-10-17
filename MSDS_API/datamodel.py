from pydantic import BaseModel, validator
from typing import Optional, Any, List

def _is_integer_str(s):
    """
    함수 작동 방식 :
    str 형태로 "5.0" 이 들어오면 int로 인식
    str 형태로 "5.1" 이 들어오면 float로 인식
    float 값 5.0 이 들어오면 int로 인식
    float 값 5.1 이 들어오면 float로 인식
    int 값 5 가 들어오면 int로 인식
    """
    try:
        float_val = float(s)
        return float_val.is_integer()
    except ValueError:
        return False


class measure_y_item(BaseModel):
    CHEM_NAME: Optional[Any] # 화학물질명
    AVG_RATE: Optional[Any] # 함유량
    MEASURED: Optional[Any] # 측정치
    EXPOSURE_TWA: Optional[Any] # 노출기준
    UNIT: Optional[Any] # 단위
    FORM_CODE:  Optional[Any] # 성상
    CMR_C:  Optional[Any] # CMR 발암성
    CMR_M:  Optional[Any] # CMR 생식세포변이
    CMR_R:  Optional[Any] # CMR 생식독성
    R_PHRASE: Optional[Any] # 위험문구(r-phrase)
    H_CODE: Optional[Any] # 유해위험문구(h-code)
    EXPOSURE:  Optional[Any] # 노출수준 관리수준
    EXPOSURE_GRADE:  Optional[Any] # 노출수준 등급
    HARMFUL_GRADE:  Optional[Any] # 유해성등급
    DANGER_GRADE:  Optional[Any] # 위험성 등급
    DANGER_LEVEL:  Optional[Any] # 위험성 수준
    MANAGEMENT_LEVEL:  Optional[Any] # 관리기준
    REFORM_MEASURES:  Optional[Any]
    REFORM_EXPOSURE_GRADE:  Optional[Any] # 개선후 위험성 노출수준 등급
    REFORM_HARMFUL_GRADE:  Optional[Any] # 개선후 위험성 유해성 등급
    REFORM_DANGER_GRADE:  Optional[Any] # 개선후 위험성 등급
    REFORM_DANGER_LEVEL:  Optional[Any] # 개선후 위험성 수준
    REDUCTION_MEASURES:  Optional[Any]
    HARMFUL_FACTOR_UNIT: Optional[Any] # 측정치 단위
    SICK_YN : Optional[Any] # 직업병 유소견자
    MOLECULAR:  Optional[Any] # 분자량
    ACUTE_TOXIC:  Optional[Any] # 급성독성여부
    RISK_ID: Optional[Any]
    CHEMICAL_SUBSTANCE_ID: Optional[Any] 
    HARMFUL_FACTOR_ID: Optional[Any]
    MEASURE_FLAG = 'Y' # 작업환경측정결과 있는 경우 고정 flag
    ELIMINATION  : Optional[Any] # Elimination_Substitution_Resource_Depletion
    ENGINEERING_RATE  : Optional[Any] # Engineering
    ADMINISTRATIVE_RATE  : Optional[Any] # Administrative
    class Config: # 해당 model에 정의되지 않은 변수도 허용
        extra = 'allow'
    @validator('*') # 해당 model 의 모든 변수에 대해 점검
    def dash_to_blank(cls, v): # "-" 라는 문자열 "" 로 변경
        if v == '-':
            return ''
        else:
            return v
    @validator( # 수치형 데이터
            'AVG_RATE',
            'MEASURED', # 측정치
            'FORM_CODE', # 성상
            'EXPOSURE_TWA', # 노출기준
            'EXPOSURE', # 노출수준 관리수준
            'EXPOSURE_GRADE', # 노출수준 등급
            'HARMFUL_GRADE', # 유해성등급
            'DANGER_GRADE', # 위험성 등급
            # 'DANGER_LEVEL', # 위험성 수준
            'SICK_YN', # 직업병 유소견자
            'MOLECULAR', # 분자량
            'ACUTE_TOXIC', # 급성독성여부
            'ELIMINATION',
            'ENGINEERING_RATE',
            'ADMINISTRATIVE_RATE'
            )
    def float_check(cls, v):
        try:
            float(v)
        except ValueError:
            return ''
        except TypeError:
            return ''
        else:
            if _is_integer_str(v):
                return int(float(v))
            else:
                return float(v)

class measure_n_item(BaseModel):
    CHEM_NAME: Optional[Any] # 화학물질명
    AVG_RATE: Optional[Any] # 함유량
    EXPOSURE_TWA:  Optional[Any] # 노출기준
    UNIT : Optional[Any] # 노출기준 단위
    FORM_CODE:  Optional[Any] # 성상
    CMR_C:  Optional[Any] # CMR 발암성
    CMR_M:  Optional[Any] # CMR 생식세포변이
    CMR_R:  Optional[Any] # CMR 생식독성
    # WORK_TIME:  Optional[Any] # 하루 사용 시간
    USE: Optional[Any] # 하루 취급량
    MONTHLY_USE : Optional[Any] # 월취급량
    VOLUME_UNIT : Optional[Any] # 단위
    MONTHLY_USE_UNIT : Optional[Any] # 월 취급량 단위
    USE_GRADE:  Optional[Any] # 취급량 레벨
    USE_TEMP:  Optional[Any] # 휘발성 사용온도
    BOILING:  Optional[Any] # 휘발성 끓는점
    VOLATILIZATION:  Optional[Any] # 휘발성 레벨
    ARSENIC_ACID:  Optional[Any] # 비산정도
    ARSENIC_GRADE:  Optional[Any] # 비산성
    VENTILATION: Optional[Any] # 밀폐/환기상태
    R_PHRASE: Optional[Any] # 위험문구(r-phrase)
    H_CODE: Optional[Any] # 유해위험문구(h-code)
    EXPOSURE_GRADE:  Optional[Any] # 노출수준 가능성 등급
    HARMFUL_GRADE:  Optional[Any] # 유해성 등급
    DANGER_GRADE:  Optional[Any] # 위험성 등급
    DANGER_LEVEL: Optional[Any] # 위험성 수준
    MANAGEMENT_LEVEL:  Optional[Any] # 관리기준
    REFORM_MEASURES:  Optional[Any] # 개선대책
    REFORM_EXPOSURE_GRADE:  Optional[Any] # 개선후 위험성 노출수준 등급
    REFORM_HARMFUL_GRADE:  Optional[Any] # 개선후 위험성 유해성 등급
    REFORM_DANGER_GRADE:  Optional[Any] # 개선후 위험성 등급
    REFORM_DANGER_LEVEL:  Optional[Any] # 개선후 위험성 수준
    REDUCTION_MEASURES:  Optional[Any] # 감소대책 실행여부
    SICK_YN : Optional[Any] # 직업병 유소견자 발생여부
    MOLECULAR:  Optional[Any] # 분자량
    ACUTE_TOXIC:  Optional[Any] # 급성독성여부
    RISK_ID: Optional[Any]
    CHEMICAL_SUBSTANCE_ID: Optional[Any]
    MEASURE_FLAG ='N' # 작업환경측정결과 없는 경우 고정 flag
    ELIMINATION  : Optional[Any] # Elimination_Substitution_Resource_Depletion
    ENGINEERING_RATE  : Optional[Any] # Engineering
    ADMINISTRATIVE_RATE  : Optional[Any] # Administrative
    class Config: # 해당 model에 정의되지 않은 변수도 허용
        extra = 'allow'
    @validator('*') # 해당 model 의 모든 변수에 대해 점검
    def dash_to_blank(cls, v): # "-" 라는 문자열 "" 로 변경
        if v == '-':
            return ''
        else:
            return v
    @validator( # 수치형 데이터
            'AVG_RATE',
            'EXPOSURE_TWA', # 노출기준
            'FORM_CODE', # 성상
            # 'WORK_TIME', # 하루 사용 시간
            'USE', # 하루 취급량
            'MONTHLY_USE', # 월취급량
            'USE_TEMP', # 휘발성 사용온도
            'BOILING', # 휘발성 끓는점
            'ARSENIC_ACID', # 비산정도
            'EXPOSURE_GRADE', # 노출수준 가능성 등급
            'HARMFUL_GRADE', # 유해성 등급
            'DANGER_GRADE', # 위험성 등급
            # 'DANGER_LEVEL', # 위험성 수준
            'SICK_YN', # 직업병 유소견자 발생여부
            'MOLECULAR', # 분자량
            'ACUTE_TOXIC', # 급성독성여부
            'ELIMINATION',
            'ENGINEERING_RATE',
            'ADMINISTRATIVE_RATE'
            )
    def float_check(cls, v):
        try:
            float(v)
        except ValueError:
            return ''
        except TypeError:
            return ''
        else:
            if _is_integer_str(v):
                return int(float(v))
            else:
                return float(v)

class Item_measure_y(BaseModel):
    """
    ajax 로 받는 데이터의 형식에 맞추기 위해
    {
        "data" : [{..}, {...}, {...}, ... ,{...}]
    }
    """
    data : List[measure_y_item]

class Item_measure_n(BaseModel):
    """
    ajax 로 받는 데이터의 형식에 맞추기 위해
    {
        "data" : [{..}, {...}, {...}, ... ,{...}]
    }
    """
    data : List[measure_n_item]