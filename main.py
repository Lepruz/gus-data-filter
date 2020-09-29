import glob
import pandas as pd
import os
from data_parsers.FilterDictionary import FilterDictionary
from enums.filtered_factors_column_name_enum import FilteredFactorsColumnNameEnum
import numpy as np

data_parser = FilterDictionary()
factors = data_parser.load()


path = os.getcwd() + "/gus_csv/"
csv_extension = "*.csv"

city_filenames = [os.path.basename(x) for x in glob.glob(path + csv_extension)]

for city_filename in city_filenames:
    df = pd.read_csv(path + city_filename, sep = ';',error_bad_lines=False, encoding='utf-8')
    df.fillna('',inplace=True)
    filtered_data = []

    for index, row in factors.iterrows():
        temp_row = df[
            (df[FilteredFactorsColumnNameEnum.Kategoria] == row[FilteredFactorsColumnNameEnum.Kategoria]) &
            (df[FilteredFactorsColumnNameEnum.Grupa] == row[FilteredFactorsColumnNameEnum.Grupa]) &
            (df[FilteredFactorsColumnNameEnum.Podgrupa] == row[FilteredFactorsColumnNameEnum.Podgrupa]) &
            (df[FilteredFactorsColumnNameEnum.Wymiar1] == row[FilteredFactorsColumnNameEnum.Wymiar1]) &
            (df[FilteredFactorsColumnNameEnum.Wymiar2] == row[FilteredFactorsColumnNameEnum.Wymiar2]) &
            (df[FilteredFactorsColumnNameEnum.Wymiar3] == row[FilteredFactorsColumnNameEnum.Wymiar3]) &
            (df[FilteredFactorsColumnNameEnum.Wymiar4] == row[FilteredFactorsColumnNameEnum.Wymiar4]) &
            (df[FilteredFactorsColumnNameEnum.Miara] == row[FilteredFactorsColumnNameEnum.Miara])
            ]

        filtered_data.append(temp_row)

    # conversion from list to pd dataframe
    filtered_data_df = pd.concat(filtered_data)
    filtered_data_df.to_csv("gus_csv/filtered/" + city_filename , sep=';', encoding='utf-8')


