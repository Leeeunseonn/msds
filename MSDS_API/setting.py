import requests
import re


############################### Host Setting #######################################################
# 로컬/개발/운영서버 별 자동으로 host(ip) 변경
import socket

curr_host_ip = socket.gethostbyname((socket.gethostname()))

req = requests.get("http://ipconfig.kr")
publicIP= re.search(r'IP Address : (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', req.text)[1]

MAX_RETRIES=5

# 운영서버 > 리눅스:127.0.0.1, 윈도우:10.1.0.4

# 임시 세팅/ 세윤 공인 ip 외에 novelis 개발, 운영
ORACLE_HOME ="D:\\Tomcat 7.0_Tomat-AsiaEHS\\python\\MSDS_API\\instantclient"
oracleDB_dsn = "AKCNAPWVEHSDD01:1521/EHSDEV" 
oracleDB_user = "ASIAEHS"
oracleDB_passwd = "asiaehs1#"
PATH=r"D:\Tomcat 7.0_Tomat-AsiaEHS\webapps\ROOT\upload\msds" # 개발
# PATH=r"D:\apache-tomcat-7_AsiaEHS\webapps\ROOT\upload\msds" 운영


# if publicIP in ["61.105.87.200"]: # CISCO 노벨리스 VPN 접속시 local에서

# # 노벨리스 dev 정보
# ORACLE_HOME ="D:\instantclient_11_2"
# oracleDB_dsn = "AKCNAPWVEHSDD01:1521/EHSDEV" 
# oracleDB_user = "ASIAEHS" 
# oracleDB_passwd = "asiaehs1#"
# PATH=r"D:\Tomcat 7.0_Tomat-AsiaEHS\webapps\ROOT\upload\msds"

# 노벨리스 prod 정보
ORACLE_HOME ="D:\instantclient_11_2"
oracleDB_dsn = "AKCNAPWVEHSDP01:1521/EHSPROD" 
oracleDB_user = "ASIAEHS" 
oracleDB_passwd = "asiaehs1#"
PATH=r"D:\apache-tomcat-7_AsiaEHS\webapps\ROOT\upload\msds"


if publicIP in ["180.71.194.248","180.71.194.232"]: # 세윤 공인ip // 로컬/개발서버 //ehs

    # PATH=r".\webapps\upload\em"
   
    # # oracleDB 서버 (GAMS)
    # ORACLE_HOME ="D:\instantclient_11_2"
    # oracleDB_dsn = "220.73.136.150:1521/XE"   #150 접속정보
    # oracleDB_user = "ehs"
    # oracleDB_passwd = "rltnfdusrnth"
 
    # oracleDB_connect_timeout = 3600


        #epm_batch 개발용 oracle 정보
    ORACLE_HOME ="D:\instantclient_11_2"
    oracleDB_dsn = "220.73.136.151:1521/orcl" #151 접속정보
    oracleDB_user = "ASIAEHS"
    oracleDB_passwd = "rltnfdusrnth"

    oracleDB_connect_timeout = 3600



# if publicIP in ["121.189.8.194","192.168.120.162"]: #개발서버2 / demo //asiaehs

#     PATH=r".\webapps\upload\em"

#     # ORACLE 19C 설치 정보
#     ORACLE_HOME ="C:\instantclient_11_2"
#     oracleDB_dsn = "192.168.120.163:1521/orcl" #151
#     oracleDB_user = "ASIAEHS"
#     oracleDB_passwd = "rltnfdusrnth"

#     oracleDB_connect_timeout = 3600



# else: # 임시 세팅 / 세윤 공인ip외 모두 vpn으로 접속되게 설정
    
#     PATH=r".\webapps\upload\em"

#     # oracleDB VPN 정보 (GAMS_VPN)
#     ORACLE_HOME ="D:\instantclient_11_2"
#     oracleDB_dsn = "192.168.120.160:1521/XE"                                                       
#     oracleDB_user = "ehs"
#     oracleDB_passwd = "rltnfdusrnth"

#     oracleDB_connect_timeout = 3600

