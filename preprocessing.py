import pandas as pd
import numpy as np
import requests
from multiprocessing import Pool


def geocoding_reverse(lat: str, lng: str):
    """ 위도, 경도 -> 행정도 주소값 변환 함수
    :param lat: 위도
    :param lng: 경도
    :return: 주소값
    """
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lng}&language=ko&key=AIzaSyB3IjKPOCo1LwD9VMFIKd3F37baTr15VLU"

    data = requests.get(url).json()
    str_list = ["동", "면", "읍"]
    for d in data["results"]:
        if any(keyword in d["formatted_address"] for keyword in str_list):
            return d["formatted_address"].replace("대한민국 ", "")
    return "뭐야"


def parallelize_dataframe(data, func):
    """ DataFrame 객체의 데이터 전처리를 위한 멀티프로세싱 함수

    :param data: DataFrame 객체
    :param func: 멀티프로세싱을 수행할 함수
    :return: 전처리된 dataframe 객체
    """
    core_nums = 8
    df_split = np.array_split(data, core_nums)
    pool = Pool(core_nums)
    data = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()

    return data


def data_preprocessing(data):
    for index, data_row in data.iterrows():
        if data.at[index, 'isnull'] or (
                '특별시' not in data.at[index, '소재지지번주소'] and '광역시' not in data.at[index, '소재지지번주소']):
            data.at[index, '소재지지번주소'] = geocoding_reverse(data.at[index, '위도'], data.at[index, '경도'])
    return data
