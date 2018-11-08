# datastudio.py
# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
import time
import requests
import logging
import logging.handlers
import mysql.connector
from mysql.connector import errorcode

def main():
    logger_name = 'datastudio'
    request_times = 10
    standby_time = 10  # seconds

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(filename)-20s %(lineno)-7d %(levelname)-8s %(message)s')

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)

    if sys.platform.startswith('linux'):
        file_handler = logging.handlers.WatchedFileHandler('/opt/www/log/purge_users.log')
    elif sys.platform.startswith('darwin'):
        file_handler = logging.handlers.WatchedFileHandler('purge_users.log')
    elif sys.platform.startswith('win32'):
        file_handler = logging.handlers.WatchedFileHandler('purge_users.log')
    else:
        file_handler = logging.handlers.WatchedFileHandler('purge_users.log')

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    logger.info('******************************************************************')
    logger.info('*****                  New datastudio started                *****')
    logger.info('******************************************************************')

    config = {
        'user': 'test.analytic',
        'password': 'XXXXXX',
        'host': 'XXXXXX',
        'database': 'analytics',
        'raise_on_warnings': True,
    }

    try:
        conn = mysql.connector.connect(**config)
    except mysql.connector.Error as e:
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif e.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(e)

    cur = conn.cursor(dictionary=True)

    sql = 'select * from TestTable'

    cur.execute(sql)

    for row in cur:
        logger.info(format(row['id'], 'd') + ' ' + row['Name'])

    cur.close()
    conn.close()

    r = None
    for i in range(0, request_times):
        r_url = 'https://api.appmetrica.yandex.ru/logs/v1/export/installations.json' +\
                '?application_id=728061' \
                '&date_since=2017-12-03 00:00:00' \
                '&date_until=2018-01-09 23:59:59' \
                '&date_dimension=default' \
                '&use_utf8_bom=true' \
                '&fields=tracker_name,publisher_name,install_datetime,os_name,appmetrica_device_id' \
                '&oauth_token=XXXXXX'
        r = requests.get(url=r_url)
        if not r.ok:
            logger.error('GET ERROR: url: %s, return code: %s, return text: %s.', r.url, r.status_code, r.text)
            time.sleep(standby_time)
            continue
        break

    if not r.ok:
        logger.error('We get ERROR for HTTP GET %s times. It''s enough: url: %s, return code: %s, return text: %s.',
                     request_times, r.url, r.status_code, r.text)
        return

    installation_data = r.json()['data']

    count = len(installation_data)

    for row in installation_data:
        a = row['appmetrica_device_id']
        b = row['install_datetime']
        c = row['os_name']
        d = row['publisher_name']
        e = row['tracker_name']

        logger.info('appmetrica_device_id = %-20s install_datetime = %-20s os_name' +\
                    ' = %-10s publisher_name = %-15s tracker_name = %s',
                    row['appmetrica_device_id'],
                    row['install_datetime'],
                    row['os_name'],
                    row['publisher_name'],
                    row['tracker_name'])



    r_url = 'https://api.appmetrica.yandex.ru/logs/v1/export/events.json' \
            '?application_id=728061' \
            '&date_since=2018-01-03 00:00:00' \
            '&date_until=2018-01-09 23:59:59' \
            '&date_dimension=default' \
            '&use_utf8_bom=true' \
            '&fields=event_name,event_datetime,appmetrica_device_id' \
            '&oauth_token=XXXXXX'

    r = requests.post(url=r_url, headers=r_headers)

    if not r.ok:
        logger.error('POST ERROR: url: %s, return code: %s, return text: %s.', r.url, r.status_code, r.text)
        return

    logger.info('******************************************************************')
    logger.info('*****                  datastudio finished                   *****')
    logger.info('******************************************************************')

    """
       Ссылка на объект в фрейме стека при возбуждении исключения (трассировка стека, хранимая в sys.exc_info()[2]
       (3.0 sys.exc_traceback) продлевает жизнь фрейма) — исправляется путём освобождения ссылки на объект трассировки,
       когда в нём более нет надобности (3.0 sys.exc_traceback = None)
    """
    sys.exc_traceback = None
    """
       В интерактивном режиме ссылка на объект в фрейме стека, где возбуждено необработанное исключение
       (трассировка стека, хранимая в sys.last_traceback продлевает жизнь фрейма) —
       исправляется путём sys.last_traceback = None.
    """
    sys.last_traceback = None


if __name__ == '__main__':
    sys.exit(main())
