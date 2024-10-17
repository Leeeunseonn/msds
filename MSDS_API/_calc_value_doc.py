"""
작업환경측정결과 있는 경우
measure_y

input :
    MEASURED (측정치) : float
    EXPOSURE_TWA (노출기준) : float
    FORM (성상) : str
    CMR_C, CMR_M, CMR_R (CMR여부) : str
    R_PHRASE (R-code) : str
    H_CODE (H-code) : str
    MOLECULAR (분자량) : float
    SICK_YN (직업병 유소견자) : int
    ACUTE_TOXIC (급성독성 여부) : int

output:
    EXPOSURE (노출수준 관리수준(%)) : float
    EXPOSURE_GRADE (노출수준 등급) : int
    HARMFUL_GRADE (유해성 등급) : int
    DANGER_GRADE (위험성 등급) : int
    DANGER_LEVEL (위험성 수준) : int

    
작업환경측정결과 없는 경우
measure_n

input :
    EXPOSURE_TWA (노출기준) : float
    WORK_TIME(하루 사용 시간) : float
    FORM (성상) : str
    CMR_C, CMR_M, CMR_R (CMR여부) : str
    USE (하루취급량) : float
    VOLUME_UNIT (단위) : str
    USE_TEMP (휘발성 사용온도) : float
    BOILING (휘발성 끓는점) : float
    ARSENIC_ACID (비산정도) : float
    R_PHRASE (R-code) : str
    H_CODE (H-code) : str
    MOLECULAR (분자량) : float
    SICK_YN (직업병 유소견자) : int
    ACUTE_TOXIC (급성독성 여부) : int

output:
    USE_GRADE (취급량 레벨) : str
    VOLATILIZATION (휘발성 레벨) : str
    ARSENIC_GRADE (비산성) : str
    EXPOSURE_GRADE (노출수준 등급) : int
    HARMFUL_GRADE (유해성 등급) : int
    DANGER_GRADE (위험성 등급) : int
    DANGER_LEVEL (위험성 수준) : int
"""