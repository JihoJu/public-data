from preprocessing import parallelize_dataframe, data_preprocessing
import pandas as pd

if __name__ == '__main__':
    # csv 파일 read
    df = pd.read_csv('./data/어린이보호구역.csv', encoding='euc-kr')
    df['isnull'] = df['소재지지번주소'].isnull()  # 주소값이 null 인지를 판단하는 열 추가

    df = parallelize_dataframe(df, data_preprocessing)

    df.to_csv('./data/preprocessed_school_zone.csv', encoding='utf-8')
