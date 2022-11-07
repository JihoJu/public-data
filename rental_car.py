from preprocessing import parallelize_dataframe, data_preprocessing
import pandas as pd

if __name__ == '__main__':
    # csv 파일 read
    df = pd.read_csv('./data/렌터카.csv', encoding='euc-kr')
    df['isnull'] = df['소재지지번주소'].isnull()  # 주소값이 null 인지를 판단하는 열 추가

    df = parallelize_dataframe(df, data_preprocessing)

    med_df = df[['업체명', '소재지도로명주소', '소재지지번주소', '위도', '경도', '차고지도로명주소', '차고지지번주소', '보유차고지수용능력',
                 '자동차총보유대수', '승용차보유대수', '승합차보유대수', '전기승용자동차보유대수', '전기승합자동차보유대수', '경차요금', '소형차요금',
                 '중형차요금', '대형차요금', '승합차요금', '레저용차요금', '수입차요금']]
    med_df.to_csv('./data/test.csv', encoding='utf-8')
