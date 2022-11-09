from preprocessing import parallelize_dataframe, data_preprocessing
import pandas as pd


class PublicDataPreprocessor:
    """ 공공 데이터 전처리 클래스

    - 전국 렌터카 업체 데이터
    - 전국 어린이보호구역 데이터
    - 전국 주차장 데이터

    전처리 방향
    - 소재지지번주소가 null 값인 경우 (위도, 경도) -> 지번 주소(행정동)로 변환
    - 모든 전처리 후 지정 column 만 csv 파일로 변환
    """

    def __init__(self, input_path, output_path, req_columns=None, core_num=8):
        self.input = input_path  # 전처리할 데이터 경로
        self.output = output_path  # 전처리된 데이터셋 저장 경로
        self.output_columns = req_columns  # output 데이터셋 구성 columns
        self.core_num = core_num  # 멀티 프로세싱 시 cpu 개수

    def run(self):
        df = pd.read_csv(self.input, encoding='euc-kr')
        # 지번 주소 열 이름이 '소재지번주소' or '소재지지번주소' 구성 -> 이를 통일
        if '소재지지번주소' not in df.columns:
            df = df.rename(columns={'소재지번주소': '소재지지번주소'})
        df['isnull'] = df['소재지지번주소'].isnull()  # 주소값이 null 인지를 판단하는 열 추가
        mask = df['소재지지번주소'].str.contains(r'(특별시|광역시)', na=False)  # 광역시 데이터만 추출
        df = df[mask]
        preprocessed_df = parallelize_dataframe(df, data_preprocessing)  # 전처리
        if self.output_columns:  # 필요 column 리스트를 사용자가 명시한 경우
            preprocessed_df = preprocessed_df[self.output_columns]
        preprocessed_df.to_csv(self.output, encoding='utf-8')
