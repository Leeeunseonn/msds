# msds_api_key = "dqJI3PgKNxSm5YocYcz2OPc1KCXKgNcKxaYAND1w7fn9qcbtM5yIkh0at3%2B2k43bsykhuuN8CyxYXFKeS4htEQ%3D%3D"  # 원본
# msds_api_key = "cbYQ7eC1O4Lz40H%2FmO14BtFduzaDfFHSNovoXtD4geobC9LZIcMnx%2FoZumv15%2BMFQSmUWpnZHU%2Fr2326j808GA%3D%3D"  # 박상우
msds_api_key = "kl6slLVa4Dtb3T8jfu4BIWpLDG9hRCRVnxEbaD3rVkn%2BD7h0hqaSHL2XfhphToZOZsm9GFDSutD9ZXG4PXo5Qg%3D%3D"  # 이은선
# msds_api_key = "PpO%2Bs8H0IwEU0mDaCnKI6M683eoYA%2FRErqQrWi4o61h5mXHzd%2BzIGu8Pqw%2FMyUFc8NLiQNOcVji11HClGZLwqA%3D%3D"  # 이태윤
# msds_api_key = "ZoiFirB9wHMoIhHxOzoZ4VW6RYBqmsInyWLhKX8IXNgcJtWzlJ9vpA%2BPw3FYjSNd%2FkHuSQDBmRwYhpkm1ZkW6w%3D%3D"  # 김부장님

MAX_RETRY = 5
TIMEOUT_SECONDS = 3000
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

SUCCESS = "00"
SUCCESS_MSG = "성공"

# 81 ~ 89 -> 통신장애 등등 데이터 없음
# 91 ~ 99 -> API ERROR

chemdetail_list = [
    "chemdetail02",
    "chemdetail03",
    "chemdetail08",
    "chemdetail09",
    "chemdetail11",
    "chemdetail15",
    "chemdetail16"
]
# ERROR_CODE = {
#     "00": "정상",
#     "01": "APPLICATION_ERROR",
#     "02": "DB_ERROR",
#     "03": "NODATA_ERROR",
#     "04": "HTTP_ERROR",
#     "05": "SERVICETIME_OUT",
#     "10": "INVALID_REQUEST_PARAMETER_ERROR",
#     "11": "NO_MANDATORY_REQUEST_PARAMETERS_ERROR",
#     "12": "NO_OPENAPI_SERVICE_ERROR",
#     "20": "SERVICE_ACCESS_DENIED_ERROR",
#     "21": "TEMPORARILY_DISABLE_THE_SERVICEKEY_ERROR",
#     "22": "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR",
#     "30": "SERVICE_KEY_IS_NOT_REGISTERED_ERROR",
#     "31": "DEADLINE_HAS_EXPIRED_ERROR",
#     "32": "UNREGISTERED_IP_ERROR",
#     "33": "UNSIGNED_CALL_ERROR",
#     "99": "UNKNOWN_ERROR",
# }


properties_shape={
    "고체":["파우더","가루","결정","분말","박편","크리스탈","Solid","solid","금속","(금속)"],
    "액체":["Liquid","liquid","용액","(용액)"],
    "기체":["가스","gas","Gas"]
}

IF_ERROR_CODE={
    "00":"성공",
    "01":"NOT_INSERT_TARGET",
    "30":"STORAGE_SAVE_ERROR", #ATCH 첨부파일 서버스토리지에 저장 실패    
    "38":"NO_PLANT", #plant_prcs_id match가 없음
    "39":"PLANTMAP_ERROR", #MSDS_PLANTMAP MATCH 실패
    "71":"KEYCODE_ERROR",
    "81":"API_CAS_ERROR", # CAS 비정상
    "82":"DB_SAVE_FAIL", # DB저장 에러
    "83":"DB_READ_FAIL", # DB읽기 에러
    "84":"NOT_FOUND_DB", #MSDS_CHEMICAL_SUBSTANCE에서 batch 돌릴 수 있는 데이터가 조회되지 않음
    "85":"DOC_NO_ERROR_in_INTF_TARGET_MSDS_BASIC", #DOC_NO plant없음, DOC_NO가 비정상
    "86":"NOT_FOUND_ORIGINAL_DATA", #UPDATE시 화학제품에 해당하는 기존 USEPLANT 데이터 없음
    # "87": "MSDS_PLANTMAP 1:N MAPPING", #에러x 정상저장 + ERRORMSG에 해당 메세지만 저장
    "88": "NOT_EXIST_APPR_DATE", # 결제일자 없는 경우 ERROR
    "90": "PARAMETER_ERROR",
    "91":"API_ERROR", #API 데이터 비정상, KOSHA에서 CAS검색 가능시 기본만 저장
    "99":"UNKNOWN_ERROR",
}


null_list = [None
            #  , "-"
             ]

config_id = "EHS"
config_pw = "ehs123!"
config_ip = "sylinux.sycns.com"
config_port = "1523"
config_sid = "DBIMS"
sql_query = "SELECT CAS_NO FROM MSDS_CHAMICAL_SUBSTANCE_TEMP"
sql_query2 = "SELECT CAS_NO FROM MSDS_CHAMICAL_SUBSTANCE_IF"

class Global_static_data:
    cmr_list = [
        '구분1A', 
        '구분1B', 
        '구분2', 
        '구분1a', 
        '구분1b', 
        '1a', 
        '1b', 
        '2', 
        2
        ]

    solid_int = 1
    solid_str = "1"
    liquid_int = 2
    liquid_str = "2"
    gas_int = 3
    gas_str = "3"

    chemical_state_list = [
        solid_int,
        solid_str,
        liquid_int,
        liquid_str,
        gas_int,
        gas_str
        ]

    solid_unit = 'mg/m3'
    liquid_unit = 'mg/m3'
    # 'mg/m³'
    # '㎎/㎥'
    gas_unit = 'ppm'

    twa_unit_list = [solid_unit, liquid_unit, gas_unit]


    chemical_twa = {
        solid_int : solid_unit,
        solid_str : solid_unit,
        liquid_int : liquid_unit,
        liquid_str : liquid_unit,
        gas_int : gas_unit, 
        gas_str : gas_unit
    }

    vol_arr = [
        [],
        [0,1,2,2],
        [0,2,3,3],
        [0,2,3,4]
    ]

    ars_arr = [
        [],
        [0,1,1,2],
        [0,2,3,3],
        [0,2,4,4]
    ]

    rh_code = {
        'R36' : "A", 
        'R36/38' : "A", 
        'R38' : "A",

        'R20' : "B", 
        'R20/21' : "B", 
        'R20/21/22' : "B", 
        'R20/22' : "B", 
        'R21' : "B", 
        'R21/22' : "B", 
        'R22' : "B",

        'R23' : "C", 
        'R23/24' : "C", 
        'R23/24/25' : "C", 
        'R23/25' : "C", 
        'R24' : "C", 
        'R24/25' : "C", 
        'R25' : "C", 
        'R34' : "C",
        'R35' : "C", 
        'R36/37' : "C", 
        'R36/37/38' : "C",
        'R37' : "C",
        'R37/38' : "C",
        'R41' : "C",
        'R43' : "C",
        'R48/20' : "C",
        'R48/20/21' : "C",
        'R48/20/21/22' : "C",
        'R48/20/22' : "C",
        'R48/21' : "C",
        'R48/21/22' : "C",
        'R48/22' : "C",

        'R26' : "D",
        'R26/27' : "D",
        'R26/27/28' : "D",
        'R26/28' : "D",
        'R27' : "D",
        'R27/28' : "D",
        'R28' : "D",
        'Carc cat 3 R40' : "D",
        'R48/23' : "D",
        'R48/23/24' : "D",
        'R48/23/24/25' : "D",
        'R48/23/25' : "D",
        'R48/24' : "D",
        'R48/24/25' : "D",
        'R48/25' : "D",
        'R60' : "D",
        'R61' : "D",
        'R62' : "D",
        'R63': "D",
        
        'Muta cat 3 R40' : "E",
        'R42' : "E",
        'R42/43' : "E",
        'R45' : "E",
        'R46' : "E",
        'R49' : "E",

        "H319" : "A",
        "H319+H315" : "A",
        "H315" : "A",

        "H332" : "B",
        "H332+H312" : "B",
        "H332+H312+H302" : "B",
        "H332+H302" : "B",
        "H312" : "B",
        "H312+H302" : "B",
        "H302" : "B",

        "H330" : "C",
        "H331" : "C",
        "H330+H311" : "C",
        "H331+H311" : "C",
        "H330+H311+H301" : "C",
        "H331+H311+H301" : "C",
        "H330+H301" : "C",
        "H331+H301" : "C",
        "H311" : "C",
        "H311+H301" : "C",
        "H301" : "C",
        "H314" : "C",
        "H319+H335" : "C",
        "H319+H335+H315" : "C",
        "H335" : "C",
        "H335+H315" : "C",
        "H318" : "C",
        "H317" : "C",
        "H373" : "C",

        "H330" : "D",
        "H330+H310" : "D",
        "H330+H310+H300" : "D",
        "H330+H300" : "D",
        "H310" : "D",
        "H310+H300" : "D",
        "H300" : "D",
        "H351" : "D",
        "H372" : "D",
        "H360" : "D",
        "H361" : "D",

        "H341" : "E",
        "H334+H317" : "E",
        "H350" : "E",
        "H340" : "E"
    }

    rh_code_lvl = {
        "A" : 1,
        "B" : 2,
        "C" : 3,
        "D" : 4,
        "E" : 4
    }