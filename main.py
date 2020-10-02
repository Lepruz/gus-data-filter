import glob
import os

import pandas as pd

from data_parsers.FilterDictionary import FilterDictionary
from enums.filtered_factors_column_name_enum import FilteredFactorsColumnNameEnum

data_parser = FilterDictionary()
factors = data_parser.load()

path = os.getcwd() + "/gus_csv/"
csv_extension = "*.csv"

city_filenames = [os.path.basename(x) for x in glob.glob(path + csv_extension)]
missing_rows = []


def find_row_by_columns(data_frame, r):
    return data_frame[
        (data_frame[FilteredFactorsColumnNameEnum.Kategoria] == r[FilteredFactorsColumnNameEnum.Kategoria]) &
        (data_frame[FilteredFactorsColumnNameEnum.Grupa] == r[FilteredFactorsColumnNameEnum.Grupa]) &
        (data_frame[FilteredFactorsColumnNameEnum.Podgrupa] == r[FilteredFactorsColumnNameEnum.Podgrupa]) &
        (data_frame[FilteredFactorsColumnNameEnum.Wymiar1] == r[FilteredFactorsColumnNameEnum.Wymiar1]) &
        (data_frame[FilteredFactorsColumnNameEnum.Wymiar2] == r[FilteredFactorsColumnNameEnum.Wymiar2]) &
        (data_frame[FilteredFactorsColumnNameEnum.Wymiar3] == r[FilteredFactorsColumnNameEnum.Wymiar3]) &
        (data_frame[FilteredFactorsColumnNameEnum.Wymiar4] == r[FilteredFactorsColumnNameEnum.Wymiar4]) &
        (data_frame[FilteredFactorsColumnNameEnum.Miara] == r[FilteredFactorsColumnNameEnum.Miara])
        ]


for city_filename in city_filenames:
    df = pd.read_csv(path + city_filename, sep=';', error_bad_lines=False, encoding='utf-8')
    df.fillna('', inplace=True)
    filtered_data = []

    for index, row in factors.iterrows():
        temp_row = find_row_by_columns(df, row)

        if temp_row.empty:
            temp_row = pd.DataFrame(
                data={FilteredFactorsColumnNameEnum.Kategoria: row[FilteredFactorsColumnNameEnum.Kategoria],
                      FilteredFactorsColumnNameEnum.Grupa: row[FilteredFactorsColumnNameEnum.Grupa],
                      FilteredFactorsColumnNameEnum.Podgrupa: row[FilteredFactorsColumnNameEnum.Podgrupa],
                      FilteredFactorsColumnNameEnum.Wymiar1: row[FilteredFactorsColumnNameEnum.Wymiar1],
                      FilteredFactorsColumnNameEnum.Wymiar2: row[FilteredFactorsColumnNameEnum.Wymiar2],
                      FilteredFactorsColumnNameEnum.Wymiar3: row[FilteredFactorsColumnNameEnum.Wymiar3],
                      FilteredFactorsColumnNameEnum.Wymiar4: row[FilteredFactorsColumnNameEnum.Wymiar4],
                      FilteredFactorsColumnNameEnum.Miara: row[FilteredFactorsColumnNameEnum.Miara]},
                index=[0])
            #filtrowanie po kolekcji bo nie wiem jak ją złączyć i wyszukać w niej raz
            # if not missing_rows or find_row_by_columns(pd.concat(missing_rows), temp_row).empty: -> coś takiego nie działa a szkoda czasu
            if len(list(filter(lambda r: not find_row_by_columns(r, temp_row).empty, missing_rows))) == 0:
                missing_rows.append(temp_row)
            print(temp_row)

        filtered_data.append(temp_row)

    # conversion from list to pd dataframe
    filtered_data_df = pd.concat(filtered_data)
    filtered_data_df.to_csv("gus_csv/filtered/" + city_filename, sep=';', encoding='utf-8', index=False)

missing_rows_df = pd.concat(missing_rows)
missing_rows_df.to_csv("gus_csv/filtered/missing_rows.csv", sep=';', encoding='utf-8', index=False)
