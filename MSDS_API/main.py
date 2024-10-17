# pip install fastapi
# pip install uvicorn[standard]

# uvicorn version 0.18.3
# xmltodict version 0.13.0
# requests version 2.28.1
# urllib3 version 1.26.11
# fastapi version 0.85.1

#### msds module ####
from datamodel import *
from datamodel_risk import *
from config import *
import MSDS_upsert
import msds_calcul
import msds_batch_api
import msds_epm_api
import msds_risk_api
import msds_risk_copy

#### python module ####
from datetime import datetime
import uvicorn
import json
import typing
from fastapi import FastAPI,status
from fastapi.exceptions import RequestValidationError, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse, Response, StreamingResponse, FileResponse
from fastapi.encoders import jsonable_encoder

# logging module
import logging
from fastapi.requests import Request
from starlette.middleware.base import BaseHTTPMiddleware

################################ logging ################################
###### 밑 3줄은 터미널 창에 뜨는 log 옵션. 필요 없으면 삭제해도 됨 ######
log_config = uvicorn.config.LOGGING_CONFIG
log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"
log_config["formatters"]["default"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"

##### 여기서 부터 log 저장 #####
current_date = datetime.now().strftime('%Y-%m-%d')
handler = logging.FileHandler(filename=f'msds_{current_date}.log', mode='a', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        logger.info(f"Client IP: {request.client.host} - \"{request.method} {request.url.path} HTTP/{request.url}\"")
        response = await call_next(request)
        logger.info(f"Response status: {response.status_code}")
        return response
#############################################################################

## msds 연동 REST API 정보   http://61.78.63.49:8001/msds?casno=71-43-2  
## /msds				>>	기존 물질정보 등록/수정  
## /msds_epm			>>	epm 물질정보 등록  
## /msds_bacth			>>	기존 물질정보 최신정보 수정 
## /msds_cal			>>	위험성평가 위험도 계산 
## /msds_risk           >>  위험성평가 통계 pivot 생성

class JSONprettyResponse(Response): # response 데이터 가독성 좋게
    media_type = "application/json"
    def render(self, content: typing.Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=True,
            indent=2,
            separators=(",", ":"),
        ).encode("utf-8")

# app = FastAPI(docs_url=None, redoc_url=None, swagger_ui_oauth2_redirect_url=None, openapi_url=None)
app = FastAPI()
# app.add_middleware(LoggingMiddleware)

############ exception_handler ############

def error_template(CODE,MSG, exc):
    return {
    "resultCode":CODE,
    # "resultMsg":ERROR_CODE.get(CODE),
    "resultMsg":MSG,
    "resultDate":datetime.now().strftime(DATE_FORMAT),
    "resultData":exc
    }


@app.exception_handler(HTTPException) # http 에러
def http_exception_handler(request, exc):
    return JSONprettyResponse(
        status_code=exc.status_code,
        content=jsonable_encoder(error_template(exc.status_code, 'HTTP_ERROR',exc.detail)),
    )

@app.exception_handler(RequestValidationError) # 파라미터 에러
def validation_exception_handler(request, exc):
    return JSONprettyResponse(
        status_code=422,
        content=jsonable_encoder(error_template(422, 'INVALID_REQUEST_PARAMETER_ERROR',exc.errors()[0]['msg']))
    )

@app.exception_handler(Exception) # python 에러
def python_exception_handler(request, exc):
    return JSONprettyResponse(
        status_code=500,
        content=jsonable_encoder(error_template(500, 'UNKNOWN_ERROR',exc))
    )

###### 본사와 VPN 제외한 ip에서 endpoint 접근 시 403 ######
@app.middleware("http")
async def limit_remote_addr(request: Request, call_next):
    if request.client.host not in ['180.71.194.232', '180.71.194.248', '192.168.120.0/24', '127.0.0.1', '124.50.104.215']:
        raise HTTPException(status_code=403)
    response = await call_next(request)
    return response
#######################################################################

##### main #####

@app.get(
    '/', 
    description='health check point',
    status_code = status.HTTP_200_OK,
    response_class = PlainTextResponse,
    responses = {200: {"description" : "Health check 응답"}}
)
def health_check():
    return "server health check status_200_OK"

@app.get(
    "/msds",
    description='',
    status_code=status.HTTP_200_OK,
    response_class=JSONResponse,
    responses = {200 : {"description" : "MSDS 응답"}},
    # response_model = Item.Msda_item,
)
def upsert_msds_four(casno: str,enno:str,keno:str,cud: str, chemical_substance_id: str): # 보내주신 값으로 i냐 u냐에 따라서 업데이트
    para = {"casno": casno,"enno":enno,"keno":keno,"cud":cud, "chemical_substance_id":chemical_substance_id}
    result = MSDS_upsert.msds(para)

    return JSONprettyResponse(content=result)


@app.get(
    "/msds_bacth",
    description='',
    status_code=status.HTTP_200_OK,
    response_class=JSONResponse,
    responses = {200 : {"description" : "MSDS BATCH 응답"}},
    # response_model = Item.Msda_item,
)
def msds_batch(keycode:str):
    para ={"keycode":keycode}
    result = msds_batch_api.msds_batch(para)
    
    return JSONprettyResponse(content=result)


@app.get(
    "/msds_epm",
    description='',
    status_code=status.HTTP_200_OK,
    response_class=JSONResponse,
    responses = {200 : {"description" : "MSDS_EPM 응답"}},
    # response_model = Item.Msda_item,
)
def insert_msds_epm(keycode:str): # keycode 맞는경우 epm에서 없는 data insert
    para ={"keycode":keycode}
    result = msds_epm_api.msds_epm(para)
    
    return JSONprettyResponse(content=result)

@app.post("/msds_risk") # 위험성평가 pivot 그릴때
def msds_api(item:Item_msds_risk):
    result = msds_risk_api.msds_risk(item)
    return JSONprettyResponse(content=result)


@app.post("/msds_risk_copy") # 복사시 chem 데이터 저장
def msds_api(item:Item_msds_risk_copy):
    result = msds_risk_copy.msds_risk_copy(item)
    return JSONprettyResponse(content=result)


@app.post("/msds_cal/measure_y") # 작업환경측정결과 있을때
def msds_api(item : Item_measure_y):
    item = msds_calcul.measure_y(item)
    # return item # 일반적으로 return 하려면 이렇게
    return JSONprettyResponse(content=item.dict()) # 가독성 좋게 return 하려면 이렇게


@app.post("/msds_cal/measure_n") # 작업환경측정결과 없을때
def msds_api(item : Item_measure_n):
    item =msds_calcul.measure_n(item)
    # return item # 일반적으로 return 하려면 이렇게
    return JSONprettyResponse(content=item.dict()) # 가독성 좋게 return 하려면 이렇게

@app.get("/msds_list/excel", response_class=FileResponse)
async def msds_list_to_excel():
    import msds_list
    # return msds_list.download_excel()
    return msds_list.download_csv()

if __name__ == "__main__":
    app = FastAPI()
    uvicorn.run("main:app", host = "0.0.0.0", port = 8001, reload=True)

# cmd command : uvicorn main:msds_cal --host=0.0.0.0 --port=8001 --reload
