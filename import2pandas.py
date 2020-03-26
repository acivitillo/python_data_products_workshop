#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
created: 2020-03-22
updated:
author; Rafal
description: import data from lines of json to pandas
"""
import os
import pathlib
import json
import pandas as pd

ROOT_PATH = pathlib.Path().absolute()
FILES_PATH = os.path.join(ROOT_PATH, 'project_submissions/airly_sensors/files/datafiles')

airly_file = os.path.join(FILES_PATH, 'klobuck_szkolna_airly.json')


class Airly:
    debug = 0  # useful for development
    columns = ['from', 'to', 'pm1', 'pm25', 'pm10', 'press', 'humid', 'temp']  # list of measurements

    # df = pd.DataFrame(columns=['A'])

    def __init__(self, airly_file: str):
        self._airly_file = airly_file
        self._measurements_list = []
        if not Airly.debug:
            return None

        print(self._airly_file)

    def _24hours_history(self, json_string: str) -> list:
        one_day_measure = json.loads(json_string)
        history_list_group = one_day_measure['history']
        history_list = []
        for one_measure in history_list_group:
            values = one_measure['values']
            di_name_value = {}
            for pair in values:
                di_name_value[pair['name']] = pair['value']
            li = [
                pd.to_datetime(one_measure['fromDateTime'].replace(':00Z', ':00.000Z'), utc=True),
                pd.to_datetime(one_measure['tillDateTime'].replace(':00Z', ':00.000Z'), utc=True),
                di_name_value['PM1'],
                di_name_value['PM10'],
                di_name_value['PM25'],
                di_name_value['PRESSURE'],
                di_name_value['HUMIDITY'],
                di_name_value['TEMPERATURE'],
            ]
            history_list.append(li)
        return history_list

    def _json_rows2array(self):
        max_process = 10000000
        if Airly.debug:
            max_process = Airly.debug
        counter = 0
        error_counter = 0
        self._measurements_list = []
        with open(self._airly_file) as f:
            for line in f:
                counter += 1
                if counter > max_process:
                    break
                try:
                    self._measurements_list.extend(self._24hours_history(line))
                except Exception:
                    error_counter += 1
                    # print(f'error in line: {counter}')
                    # print(line)
        print(f'Erorr counter = {error_counter}')
        return self._measurements_list

    def _create_df(self) -> pd.DataFrame:
        # df = pd.DataFrame(columns=Airly.columns)
        df = pd.DataFrame()
        df = df.append(pd.DataFrame(self._measurements_list, columns=Airly.columns), ignore_index=True)
        return df

    def import2df(self):
        self._measurements_list = self._json_rows2array()
        result = self._create_df()
        return result


klobuck_file = Airly(airly_file)
df = klobuck_file.import2df()
start = '2019-01-02'
end = '2019-01-04'
df = df[(df['from'] > start) & (df['from'] < end)]
print(df)
