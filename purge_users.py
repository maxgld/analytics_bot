# purge_users.py
# !/usr/bin/env python3
# -*- coding: utf-8 -*-

# Скрипт для очистки (удаления) пользователей R&G. Применялся для удаления старых пользователей, у которых
# в мыле есть приставка "_deleted", флаг is_active = false, имя и фамилия равны соответственно "Аккаунт" и "Удален".
# Всего таких товаров 625. Поэтому и был написан этот скрипт, так как быстрее написать скрипт, чем руками удалять.

import sys
import json
import requests
from dbworker import DBworker
import logging
import logging.handlers
import psycopg2


def main():
    logger_name = 'purge_users'
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
    logger.info('*****                 New purge_users started                *****')
    logger.info('******************************************************************')

    r_url = 'XXXXXX'

    r_data = {
        'email': 'XXXXXX',
        'password': 'XXXXXX',
        'scope': 'administrative'
    }

    r_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    r = requests.post(url=r_url, data=json.dumps(r_data), headers=r_headers)

    if not r.ok:
        logger.error('POST ERROR: url: %s, return code: %s, return text: %s.', r.url, r.status_code, r.text)
        return

    access_token = r.json()['token']

    db = DBworker(logger,
                  dbname='catalog',
                  user='test.analytic',
                  password='XXXXXX',
                  host='XXXXXX',
                  port=5432,
                  connect_timeout=15,
                  read_only=True)

    sql = '''
    select * from users_user as t1 inner join users_useremail as t2 on t1.id = t2.user_id
    where lower(t2.email) like '%_deleted' and t1.is_active = false;
    '''

    rows = db.execute_sql_rows(sql)

    i = 0
    for row in rows:
        i = i + 1
        try:
            user_id = row['user_id']
        except psycopg2.Error as e:
            logger.error('Error code: %s. Text code: %s', e.pgcode, e.pgerror)
            return

        r_url = 'XXXXXX' +\
                format(user_id, 'd') + '/purge/?access_token=' + access_token

        r = requests.post(url=r_url, headers=r_headers)

        if not r.ok:
            logger.error('POST ERROR: url: %s, return code: %s, return text: %s.', r.url, r.status_code, r.text)
            return

        logger.info('%-8s user_id = %s was purged!', i, user_id)

    logger.info('******************************************************************')
    logger.info('*****                  purge_users finished                  *****')
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
