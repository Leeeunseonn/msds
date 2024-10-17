import oracledb
import requests
import re

from MSDS_upsert import *


def msds_risk(item):
    
    # print(id,year,dept_list)
    # print(item)
    res=None
    flag="fail"
    pv=[]

    try:
        flag,oracleDB=get_db_conn()
        cur = oracleDB.cursor()
        if item:
            item = item.dict()['data']

            mk_pivot_query=f"""
                SELECT * 
                        FROM ( 
                                    SELECT DANGER_LEVEL 
                                        , DEPT_CODE 
                                        , DANGER_CNT 
                                        , ARR_NO 
                                    FROM MSDS_DANGER_LEVEL 
                                    WHERE 1=1 
                                        AND PLANT_PRCS_ID = '{item['id']}' 
                                        AND YYYY = '{item['year']}' 
                                        
                                ) 
                        PIVOT (SUM(DANGER_CNT) FOR DEPT_CODE IN ({str(item['dept_list'])[1:-1]})
                        ) 
                        ORDER BY ARR_NO

                """
            # print(mk_pivot_query)
            cur.execute(mk_pivot_query)
            res=cur.fetchall()

            if res: 
                # if len(res[0])>2: # 정상 데이터 조회
                # else: # dept_list=[] 
                vals= [list(0 if x == None else x for x in list(i)) for i in res] #None to 0
                total_list=[sum([j for j in v[2:]]) for v in vals]
                for n in range(3):
                    vals[n].append(total_list[n])
                keys=("DANGER_LEVEL","ARR_NO")+tuple(item['dept_list'])+("TOTAL",)
                pv=[dict(zip(keys,v)) for v in vals]

            else: #조회 데이터 없음 year
                pv= [{'DANGER_LEVEL': 'L', 'ARR_NO': 1, "TOTAL":0}, {'DANGER_LEVEL': 'M', 'ARR_NO': 2, "TOTAL":0}, {'DANGER_LEVEL': 'H', 'ARR_NO': 3, "TOTAL":0}]
            flag="success"
        else:
            pv="WRONG_INPUT"
            raise

    except Exception as ora_e:
        if oracleDB:
            oracleDB.rollback()
        return {"flag": flag, "data": pv}
    
    finally:
        if oracleDB:
            oracleDB.close()

    return {"flag": flag, "data": pv}



# if __name__=="__main__":
#     a=msds_risk(id='21000',year='2024',dept_list=['A204','A302'])

#     print(a)