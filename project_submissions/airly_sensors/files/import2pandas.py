#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
created: 2020-03-22
updated: 2020-04-04
author; Rafal
description: import data from lines of json to pandas
"""
import os
import pathlib
import json
import pandas as pd
import psycopg2 as pg
import psycopg2.extras
import time
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
import urllib.parse as urlparse
import requests
from requests.exceptions import HTTPError

# from psycopg2.extras import execute_values
# import http.client
# import socket
# import random
# import math
# from urllib.parse import parse_qs

ROOT_PATH = pathlib.Path().absolute()
FILES_PATH = os.path.join(ROOT_PATH, 'project_submissions/airly_sensors/files/datafiles')


# airly_file = os.path.join(FILES_PATH, 'klobuck_szkolna_airly.json')
# airly_installed_sensors_file = os.path.join(FILES_PATH, 'installed_sensors.json')


class Airly:
    debug = 0  # useful for development
    columns = ['from', 'to', 'pm1', 'pm25', 'pm10', 'press', 'humid', 'temp']  # list of measurements

    root_url = 'https://airapi.airly.eu/v2'
    measure_install_url = root_url + '/measurements/installation'
    one_measure_url = root_url + '/measurements/installation'  # ?installationId=6512'

    reset_time = 60
    max_calls_per_reset_time = 50
    max_calls_per_day = 1000
    parallel = 5

    # df = pd.DataFrame(columns=['A'])

    def __init__(self, airly_file=None):
        if airly_file:
            self._airly_file = airly_file

        self._measurements_list = []

        if not Airly.debug:
            return

        print(self._airly_file)

    @classmethod
    def get_credentials(cls):
        return {
            'host': os.environ['POSTGRES_HOST'],
            'port': int(os.environ['POSTGRES_PORT']),
            'user': os.environ['POSTGRES_USER'],
            'password': os.environ['POSTGRES_PASS'],
            'database': os.environ['POSTGRES_DATABASE'],
            'airly_key': os.environ['AIRLY_KEY']
        }

    @staticmethod
    def a24hours_history_old(json_string: str) -> list:
        '''
        old version of the function
        noticed that some sensors store different data so dedided to store as jsonb
        :param json_string:
        :return:
        '''
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
                # some sensors produces wrong format
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

    @staticmethod
    def a24hours_history(json_string: str, installation_id: str) -> list:
        one_day_measure = json.loads(json_string)
        history_list_group = one_day_measure['history']
        history_list = []
        for one_measure in history_list_group:
            values = one_measure['values']
            di_name_value = {}
            for pair in values:
                di_name_value[pair['name'].lower()] = pair['value']

            # di_name_value['fromDateTime'.lower()] = pd.to_datetime(
            #     one_measure['fromDateTime'].replace(':00Z', ':00.000Z'),
            #     utc=True)
            # di_name_value['tillDateTime'.lower()] = pd.to_datetime(
            #     one_measure['tillDateTime'].replace(':00Z', ':00.000Z'),
            #     utc=True)

            di_name_value['fromDateTime'.lower()] = one_measure['fromDateTime'].replace(':00Z', ':00.000Z')
            di_name_value['tillDateTime'.lower()] = one_measure['tillDateTime'].replace(':00Z', ':00.000Z')
            di_name_value['installationId'] = installation_id

            history_list.append(di_name_value)
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
                    self._measurements_list.extend(self.a24hours_history(line))
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

    def airly_json2_list(self):
        self._measurements_list = self._json_rows2array()
        return self._measurements_list

    def import2df(self):
        self._measurements_list = self._json_rows2array()
        result = self._create_df()
        return result


class AirlyAllSensors(Airly):
    def __init__(self, all_sensors: str):
        super()
        self._airly_all_sensors = all_sensors
        self._airly_all_sensors_list = []

    def import_all_sensors_json2list(self):
        content = {}
        with open(self._airly_all_sensors) as f:
            content = f.read()
        all_sensors_json = json.loads(content)
        airly_all_sensors_list = []
        for e in all_sensors_json:
            one_sensor = {'id': e['id'],
                          'lat': e['location']['latitude'],
                          'lon': e['location']['longitude'],
                          'country': e['address']['country'],
                          'city': e['address']['city'],
                          'street': e['address']['street'],
                          'elevation': e['elevation'],
                          'airly': e['airly'],
                          'sponsor_id': e['sponsor']['id'],
                          'fulldata': json.dumps(e)
                          }

            airly_all_sensors_list.append(one_sensor)

        self._airly_all_sensors = airly_all_sensors_list
        return self._airly_all_sensors


class AirlyDatabase(Airly):
    def __init__(self):
        credentials = Airly.get_credentials()
        self.host = credentials['POSTGRES_HOST']
        self.port = credentials['POSTGRES_PORT']
        self.user = credentials['POSTGRES_USER']
        self.password = credentials['POSTGRES_PASS']
        self.database = credentials['POSTGRES_DATABASE']

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, port):
        self.__port = port

    def __str__(self):
        return f'host = {self.host}, port = {self.port}, database = {self.database}, ' \
               f'user = {self.user}, pass = ******'

    @staticmethod
    def import_measure_list_to_table(li):
        all_installations = [tuple(d) for d in li]

        all_credentials = Airly.get_credentials()
        credentials = {}

        for i in {'host', 'port', 'user', 'password', 'database'}:
            credentials[i] = all_credentials[i]

        with pg.connect(**credentials) as connection:
            cursor = connection.cursor()

            insert_string = 'insert into "measures" (from_time, to_time, measure) ' \
                            'values %s'
            pg.extras.execute_values(cursor, insert_string, all_installations)  # insert current data

    @staticmethod
    def import_installation_list_to_table(li):
        all_installations = [tuple(d.values()) for d in li]

        with pg.connect(**Airly.get_credentials()) as connection:
            cursor = connection.cursor()
            # TODO what if they remove a sensor but I will have measures for it?
            # only update - not delete
            cursor.execute('DELETE FROM instalations')  # will be replaced by current data

            insert_string = 'insert into "instalations" (id_loc,lat,lon,country,city,street,' \
                            'elevation,airly,sponsor_id,fulldata) ' \
                            'values %s'
            pg.extras.execute_values(cursor, insert_string, all_installations)  # insert current data


def slipper(index):
    sleep = 1500.0
    print(f'{index} slipping for {sleep} ms')
    time.sleep(sleep / 1000)


class AirlyRequest(Airly):
    max_cals_per_day = 1000
    reset_time = 20
    max_calls_per_reset_time = 100
    parallel = 100

    credentials = Airly.get_credentials()

    sleep_time = -1
    exec_time = -1
    counter = -1

    def __init__(self):
        super()

    def get_one_day_measurement(self):
        url = Airly.one_measure_url
        params = {'installationId': None}

    @classmethod
    def construct_dict_of_url_installation(cls, installations):
        url = Airly.one_measure_url
        params = {'installationId': None}

        li_url = []
        li_param = []
        headers = []
        credentials = Airly.get_credentials()
        for i in installations:
            li_url.append(url)
            params = {'installationId': i}
            li_param.append(params)
            headers.append({'Accept': 'application/json', 'apikey': credentials['airly_key']})

        return {'urls': li_url, 'params': li_param, 'headers': headers}

    def get_result(self, fun, li):
        di = fun(li)


def general_request(url, headers, params):
    try:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r  # r.content
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  # Python 3.6
    except Exception as err:
        print(f'Other error occurred: {err}')  # Python 3.6


def execute_parallel(source, param, main_fun):
    sleep_time = -1
    exec_time = -1
    counter = -1
    st = time.time()
    li_object_returned = []

    for i in range(0, len(param['urls']), source.parallel):
        if sleep_time > 0 and source.max_calls_per_reset_time - source.parallel < counter:
            print(f'must waut {sleep_time:.5f} sec due to exec time is {exec_time: .5f}')
            time.sleep(sleep_time)
            counter = 0
            st = time.time()

        pool_list = []
        headers = param['headers']
        params = param['params']
        j = 0
        for j in range(0, min(source.parallel, len(param['urls']) - j - i), 1):
            pool_list.append(param['urls'][i + j])

        with PoolExecutor(max_workers=source.parallel) as executor:
            for _ in executor.map(main_fun, pool_list, headers, params):
                counter += 1
                if _.status_code < 300:
                    li_object_returned.append(_)

        exec_time = time.time() - st
        sleep_time = source.reset_time - exec_time

    return li_object_returned


def airly_api_request(li_urls):
    source = Airly
    param = {'urls': li_urls['urls'], 'params': li_urls['params'], 'headers': li_urls['headers']}
    main_fun = general_request

    li_result = execute_parallel(source, param, main_fun)
    all_sensors = []
    for result in li_result:
        json_string = result.text
        parsed = urlparse.urlparse(result.url)
        installation_id = urlparse.parse_qs(parsed.query)['installationId'][0]
        one_day = Airly.a24hours_history(json_string, installation_id)
        all_sensors.append(one_day)
    return all_sensors


airly_request_obj = AirlyRequest()
installations_ids = ['984', '655', '97', '1088', '1035', '736', '987', '706', '992', '835', '346', '799', '90', '802', ]
# '800', '803', '805', '795', '599', '798', '908', '974', '749', '982', '700', '43', '909', '740', '983',
# '978', '1095', '756', '975', '1022', '957', '98', '972', '1083', '738', '815', '844', '870', '656',
# '747', '1113', '1116', '453', '690', '695', '1011', '659', '754', '96', '977', '560', '689', '95',
# '697', '448', '66', '398', '100', '99', '91', '836', '508', '42', '94', '101', '1102', '1123', '416',
# '809', '920', '923', '922', '913', '483', '284', '932', '937', '753', '938', '388', '883', '811',
# '931', '532', '521', '503', '514', '960', '477', '491', '637', '413', '555', '64', '605', '615', '397',
# '482', '674', '493', '642', '541', '620', '1033', '207', '1070', '549', '60', '1092', '247', '313',
# '1078', '17', '312', '590', '31', '492', '1096', '251', '842', '318', '1026', '18', '58', '244', '820',
# '627', '236', '317', '596', '245', '323', '327', '326', '614', '246', '239', '527', '241', '347',
# '237', '366', '600', '233', '512', '335', '238', '59', '401', '329', '1029', '473', '447', '354', '19',
# '727', '350', '353', '304', '320', '423', '349', '332', '403', '70', '683', '784', '879', '677', '290',
# '291', '681', '429', '685', '344', '336', '430', '877', '810', '341', '343', '342', '562', '669', '44',
# '286', '851', '371', '1012', '357', '699', '92', '372', '518', '1104', '93', '62', '373', '287', '611',
# '292', '631', '534', '639', '594', '595', '598', '629', '67', '68', '283', '295', '490', '702', '710',
# '704', '714', '321', '725', '863', '53', '536', '40', '380', '758', '897', '61', '297', '946', '459',
# '930', '465', '943', '944', '456', '924', '480', '926', '942', '902', '470', '479', '1101', '89', '65',
# '474', '676', '910', '88', '643', '914', '617', '854', '788', '849', '841', '1007', '845', '847',
# '504', '783', '1105', '52', '112', '41', '721', '106', '766', '1036', '684', '33', '1118', '1117',
# '569', '1131', '1023', '1054', '997', '962', '787', '7', '575', '776', '822', '105', '563', '566',
# '1126', '422', '51', '8', '355', '880', '709', '119', '888', '13', '881', '14', '874', '794', '884',
# '875', '885', '876', '889', '801', '46', '15', '111', '778', '75', '24', '21', '74', '107', '860',
# '945', '550', '554', '30', '6', '1087', '123', '581', '826', '573', '586', '871', '579', '582', '1121',
# '583', '364', '435', '785', '588', '572', '765', '377', '356', '36', '360', '370', '56', '22', '1072',
# '29', '405', '365', '26', '658', '337', '1074', '122', '817', '1091', '23', '12', '1106', '1120',
# '1132', '819', '814', '27', '496', '831', '703', '498', '607', '118', '20', '47', '928', '1062', '55',
# '104', '125', '310', '115', '16', '57', '653', '25', '117', '116', '11', '9', '906', '10', '1019',
# '124', '954', '948', '953', '113', '940', '54', '72', '45', '128', '84', '32', '71', '81', '38', '102',
# '48', '77', '78', '76', '37', '49', '79', '86', '82', '126', '80', '83', '85', '103', '34', '989',
# '792', '576']

# prepare urls
dict_of_url_calls = airly_request_obj.construct_dict_of_url_installation(installations_ids)  # from airly installations

# call api requests
results = airly_api_request(dict_of_url_calls)

# prepare list for database instrt
list_of_json_results = [[one_measure['fromdatetime'], one_measure['tilldatetime'], json.dumps(one_measure)] for
                        installation in results for one_measure in
                        installation]
# insert data into database
AirlyDatabase.import_measure_list_to_table(list_of_json_results)





# def results2json(li):
#     for one_result in li:
#         di = json.load(one_result.text)

# all_sensors = AirlyAllSensors(airly_installed_sensors_file)
# qq = all_sensors.import_all_sensors_json2list()
# airly = AirlyDatabase()
# airly.import_installation_list_to_table(qq)

# all_sensors = AirlyAllSensors(airly_installed_sensors_file)
# qq = all_sensors.import_all_sensors_json2list()
# print(len(qq))
# canada = [f'city = {c["city"]}, elevation = {c["elevation"]}' for c in qq if c['elevation'] < -5.7]
# canada2 = [c["elevation"] for c in qq if c['elevation'] < 0]
# print((canada))
# print(len(canada))
# print(min(canada2))

# klobuck_file = Airly(airly_file)
# df = klobuck_file.import2df()
# start = '2019-01-02'
# end = '2019-01-04'
# df = df[(df['from'] > start) & (df['from'] < end)]
# print(df)
