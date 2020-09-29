import os
import pandas as pd
import numpy as np

class FilterDictionary:

    def __init__(self):
        self.path = os.getcwd() + "/filtered_factors/factors.csv"


    def load(self):
        data = pd.read_csv(self.path, sep = ';',error_bad_lines=False, encoding='utf-8')
        data.fillna('', inplace=True)
        return data

