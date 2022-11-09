from PublicDataPreprocessor import PublicDataPreprocessor

if __name__ == '__main__':
    """
    - 주차장: input/주차장.csv
    - 어린이보호구역: input_data/어린이보호구역.csv
    - 렌터카업체: input_data/렌터카.csv
    """
    preprocessor = PublicDataPreprocessor(input_path='input_data/주차장.csv', output_path='output/parking.csv')
    preprocessor.run()
