'''
    需安裝相關套件
    pip install wheel
    pip install pyodbc
    pip install pandas
    pip install numpy
    pip install beautifulsoup4
    pip install lxml
'''

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import datetime
import os
import pyodbc

file_path = os.getcwd()

# set dir
today = str (datetime.date.today())
cwb_data = "cwb_weather_data"
if not os.path.exists(cwb_data):
    os.mkdir(cwb_data)

# connect api
import urllib.request
import zipfile
res = "http://opendata.cwb.gov.tw/opendataapi?dataid=F-D0047-093&authorizationkey=CWB-5DFE041D-0E57-4947-9F3B-874229CB9FAE"
urllib.request.urlretrieve(res, "F-D0047-093.zip")
f = zipfile.ZipFile('F-D0047-093.zip')

# 解ZIP
'''
with zipfile.ZipFile('F-D0047-093.zip', 'r') as zip_ref:
    zip_ref.extractall(file_path + "/" + cwb_data + "/")
'''

# 整理資料
files = ['63_72hr_CH.xml','64_72hr_CH.xml','65_72hr_CH.xml','66_72hr_CH.xml','67_72hr_CH.xml','68_72hr_CH.xml',
        '09007_72hr_CH.xml','09020_72hr_CH.xml','10002_72hr_CH.xml','10004_72hr_CH.xml','10005_72hr_CH.xml',
        '10007_72hr_CH.xml','10008_72hr_CH.xml','10009_72hr_CH.xml','10010_72hr_CH.xml','10013_72hr_CH.xml',
        '10014_72hr_CH.xml','10015_72hr_CH.xml','10016_72hr_CH.xml','10017_72hr_CH.xml','10018_72hr_CH.xml',
        '10020_72hr_CH.xml'
        ]
CITY = []
DISTRICT = []
GEOCODE = []
DAY = []
TIME = []
T = []
TD = []
WD = []
WS = []
BF = []
AT = []
Wx = []
PoP12h = []
get_day = []
RH = []

for filename in files:
    try:
        data = f.read(filename).decode('utf8')
        soup = BeautifulSoup(data, 'xml')
        city = soup.locationsName.text
        a = soup.find_all("location")
        for i in range(0, len(a)):
            location = a[i]
            district = location.find_all("locationName")[0].text
            geocode = location.geocode.text
            weather = location.find_all("weatherElement")
            # Time
            time = weather[1].find_all("dataTime")

            for j in range(0, len(time)):
                x = time[j].text.split("T")
                DAY.append(x[0])
                time_1 = x[1].split("+")
                TIME.append(time_1[0])
                CITY.append(city)
                DISTRICT.append(district)
                GEOCODE.append(geocode)
                get_day.append(today)

            for t in weather[0].find_all("value"):
                T.append(t.text)

            for td in weather[1].find_all("value"):
                TD.append(td.text)

            for rh in weather[2].find_all("value"):
                RH.append(rh.text)

            for wd in weather[5].find_all("value"):
                WD.append(wd.text)

            ws = weather[6].find_all("value")
            for k in range(0, len(ws), 2):
                WS.append(ws[k].text)
                BF.append(ws[k+1].text)

            for at in weather[8].find_all("value"):
                AT.append(at.text)

            wx = weather[9].find_all("value")
            for w in range(0, len(wx), 2):
                Wx.append(wx[w].text)

            rain2 = weather[4].find_all("value")
            for m in range(0, len(rain2)):
                pop12 = rain2[m].text
                PoP12h.append(pop12)
                PoP12h.append(pop12)
                PoP12h.append(pop12)
                PoP12h.append(pop12)
    except:
        break
f.close()

data = {"城市(CITY)":CITY,"行政區(DISTRICT)":DISTRICT,"地理編碼(GEOCODE)":GEOCODE,"日期(DAY)" : DAY,"時間(TIME)" : TIME,
        "溫度(T)":T,"熱帶低氣壓(TD)" : TD,"濕度(RH)":RH,"風向(WD)" : WD,"風速(WS)" : WS,"蒲福風級(BF)":BF,"體感溫度(AT)" : AT,
        "天氣現象(Wx)": Wx,"降雨機率(PoP12h)" :PoP12h,"資料取得時間(get_day)":get_day}

df = pd.DataFrame(data,columns=["城市(CITY)","行政區(DISTRICT)","地理編碼(GEOCODE)","日期(DAY)","時間(TIME)","溫度(T)",
                                "熱帶低氣壓(TD)","濕度(RH)","風向(WD)","風速(WS)","蒲福風級(BF)","體感溫度(AT)","天氣現象(Wx)",
                                "降雨機率(PoP12h)","資料取得時間(get_day)"])

# 建立儲存路徑檔
save_name = "taiwan_cwb" + today + ".csv"
save_name = file_path + "/" + cwb_data + "/" + save_name

df.to_csv(save_name, index=False, encoding="utf_8_sig")