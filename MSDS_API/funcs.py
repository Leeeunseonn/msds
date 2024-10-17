from config import *
from functools import wraps
from threading import Thread

import oracledb
import os
import pandas as pd



def dict_fill_na(data, value):
    """
    dictionary type data에서
    key : None 데이터를 원하는 value로 채우는 함수
    """
    for k, v in data.items():
        if isinstance(v, dict):
            dict_fill_na(v, value)
        elif isinstance(v, list):
            if v:
                for d in v:
                    dict_fill_na(d, value)
            else:
                data[k] = value
        elif isinstance(v, str):
            if v in null_list or not bool(v):
                v = value
                data[k] = v
        elif isinstance(v, int):
            pass
        elif isinstance(v, float):
            pass


def dict_replace_quote(data):
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


def retry(number_of_retry: int, match_type: type):
    """
    decorator.
    number_of_retry에 해당 함수 실행 횟수 지정(1 이상이여야함).
    func의 return type이 match_type에서 지정한 type과 다르면 None 반환.
    """
    number_of_retry = int(number_of_retry)
    if number_of_retry < 0:
        raise "최대 반복회수는 양의 정수를 입력하시오"

    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def new_func():
                try:
                    ret = func(*args, **kwargs)
                    return ret
                except Exception as e:
                    return e

            count = 0
            while True:
                if count > number_of_retry:
                    return ret  # type -> Exception
                else:
                    ret = new_func()
                    if not isinstance(ret[0], match_type):
                        count += 1
                    else:
                        return ret  # type -> Any ...

        return wrapper

    return deco

