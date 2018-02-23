# analytics_bot.py
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ниже приведена статистика за прошлую неделю в сравнении с показателями предыдущих трех месяцев
Новых пользователей: A (A1%)
Регистраций: B (B1%)
Сканирований: C (C1%)
Отзывов: D (D1%)
Фото: E (E1%)
Цен: F (F1%)
Активных пользователей: G (G1%)
"""

import locale
import sys
import datetime
import json
import requests
import logging
import logging.handlers
import logging.config
import yaml
import os
from dbworker import DBworker

# def split_at(w, n):
#     for i in range(0, len(w), n):
#         yield w[i:i + n]

def main():
    is_loaded_config = False
    logger = None
    logger_name = 'analytics_bot'

    config_path = 'analytics_bot.yaml'
    env_value = os.getenv('AB_CFG', None)
    if env_value:
        config_path = env_value
    if os.path.exists(config_path):
        with open(config_path, 'rt', encoding='utf8') as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config['logging'])
                logger = logging.getLogger(logger_name)
                is_loaded_config = True
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')

    if not is_loaded_config:
        print('Failed to load configuration file. Using default configs')
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s %(filename)-20s %(lineno)-7d %(levelname)-8s %(message)s')

        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.setFormatter(formatter)

        if sys.platform.startswith('linux'):
            file_handler = logging.handlers.WatchedFileHandler('/opt/www/log/analytics_bot.log')
        elif sys.platform.startswith('darwin'):
            file_handler = logging.handlers.WatchedFileHandler('analytics_bot.log')
        elif sys.platform.startswith('win32'):
            file_handler = logging.handlers.WatchedFileHandler('analytics_bot.log')
        else:
            file_handler = logging.handlers.WatchedFileHandler('analytics_bot.log')

        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        logger.addHandler(console)
        logger.addHandler(file_handler)

    logger.info('******************************************************************')
    logger.info('*****                New analytic_bot started                *****')
    logger.info('******************************************************************')

    db = None
    db_analytics = None
    if is_loaded_config:
        db = DBworker(logger,
                      dbname=config['database']['catalog_ratengoods']['dbname'],
                      user=config['database']['catalog_ratengoods']['user'],
                      password=config['database']['catalog_ratengoods']['password'],
                      host=config['database']['catalog_ratengoods']['host'],
                      port=config['database']['catalog_ratengoods']['port'],
                      connect_timeout=config['database']['catalog_ratengoods']['connect_timeout'],
                      read_only=config['database']['catalog_ratengoods']['read_only'])
        db_analytics = DBworker(logger,
                                dbname=config['database']['analytics']['dbname'],
                                user=config['database']['analytics']['user'],
                                password=config['database']['analytics']['password'],
                                host=config['database']['analytics']['host'],
                                port=config['database']['analytics']['port'],
                                connect_timeout=config['database']['analytics']['connect_timeout'],
                                read_only=config['database']['analytics']['read_only'])
    else:
        db = DBworker(logger,
                      dbname='catalog',
                      user='test.analytic',
                      password='XXXXXX',
                      host='XXXXXX',
                      port=5432,
                      connect_timeout=15,
                      read_only=True)
        db_analytics = DBworker(logger,
                                dbname='analytics',
                                user='test.analytic',
                                password='XXXXXX',
                                host='XXXXXX',
                                port=5432,
                                connect_timeout=15,
                                read_only=False)

    # current_date at time zone 'gmt'
    start_week_date = db.execute_sql_datetime(
        "select date_trunc('week', localtimestamp - interval '7' day)")
    logger.info('start_week_date = %s', start_week_date)
    finish_week_date = db.execute_sql_datetime(
        "select date_trunc('week', localtimestamp)")
    logger.info('finish_week_date = %s', finish_week_date)
    start_3months_date = db.execute_sql_datetime(
        "select date_trunc('month', localtimestamp - interval '3' month)")
    logger.info('start_3months_date = %s', start_3months_date)
    finish_3months_date = db.execute_sql_datetime(
        "select date_trunc('month', localtimestamp)")
    logger.info('finish_3months_date = %s', finish_3months_date)

    analytics_bot_id = db_analytics.write_time_periods(start_week_date, finish_week_date,
                                                       start_3months_date, finish_3months_date)

    # Новых пользователей: A(A1% количество новых запусков по сравнению с прошлыми 3 месяцами)
    a = db.execute_sql_int(f"""
        select count(*) from users_user
        where date_joined >= '{start_week_date}' and date_joined < '{finish_week_date}';
        """)

    a2 = db.execute_sql_int(f"""
        select count(*) from users_user
        where date_joined >= '{start_3months_date}' and date_joined < '{finish_3months_date}'
        """)

    a1 = db.calculate('a', a, a2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'a', a, a2, a1)

    # Регистраций: B(B1% количество регистраций по сравнению с прошлыми 3 месяцами)
    b = db.execute_sql_int(f"""
        select count(*) from catalog_userprofile
        where date_registered >= '{start_week_date}' and date_registered < '{finish_week_date}';
        """)

    b2 = db.execute_sql_int(f"""
        select count(*) from catalog_userprofile
        where date_registered >= '{start_3months_date}' and date_registered < '{finish_3months_date}';
        """)

    b1 = db.calculate('b', b, b2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'b', b, b2, b1)

    # Сканирований: C(C1% количество сканов  по сравнению с прошлыми 3 месяцами. По catalog_historyitem.)
    c = db.execute_sql_int(f"""
        select count(*) from catalog_historyitem
        where timestamp >= '{start_week_date}' and timestamp < '{finish_week_date}' and scanned = true;
        """)

    c2 = db.execute_sql_int(f"""
        select count(*) from catalog_historyitem
        where timestamp >= '{start_3months_date}' and timestamp < '{finish_3months_date}' and scanned = true;
        """)

    c1 = db.calculate('c', c, c2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'c', c, c2, c1)

    # Сканирований: C'(C'1% количество сканов  по сравнению с прошлыми 3 месяцами без учета Web.
    # По catalog_historyitem.)
    c_minus_web = db.execute_sql_int(f"""
        select count(*) from catalog_historyitem
        where timestamp >= '{start_week_date}' and timestamp < '{finish_week_date}' and scanned = true
        and user_id <> 1200021;
        """)

    c_minus_web2 = db.execute_sql_int(f"""
        select count(*) from catalog_historyitem
        where timestamp >= '{start_3months_date}' and timestamp < '{finish_3months_date}' and scanned = true
        and user_id <> 1200021;
        """)

    c_minus_web1 = db.calculate('c_minus_web', c_minus_web, c_minus_web2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'c_minus_web', c_minus_web, c_minus_web2, c_minus_web1)

    # Сканирований: C''(C''1% количество сканов  по сравнению с прошлыми 3 месяцами. По catalog_userevent.)
    # ProductScan = 15
    cc = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_week_date}' and created < '{finish_week_date}' and category = 15;
        """)

    cc2 = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_3months_date}' and created < '{finish_3months_date}' and category = 15;
        """)

    cc1 = db.calculate('cc', cc, cc2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'cc', cc, cc2, cc1)

    # Сканирований: C'''(C'''1% количество сканов  по сравнению с прошлыми 3 месяцами без учета Web.
    # По catalog_userevent.)
    # ProductScan = 15
    cc_minus_web = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_week_date}' and created < '{finish_week_date}' and category = 15
        and user_id <> 1200021;
        """)

    cc_minus_web2 = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_3months_date}' and created < '{finish_3months_date}' and category = 15
        and user_id <> 1200021;
        """)

    cc_minus_web1 = db.calculate('cc_minus_web', cc_minus_web, cc_minus_web2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'cc_minus_web', cc_minus_web, cc_minus_web2, cc_minus_web1)

    # Отзывов: D(D1% количество отзывов по сравнению с прошлыми 3 месяцами. По catalog_productreview.)
    d = db.execute_sql_int(f"""
        select count(*) from catalog_productreview
        where created >= '{start_week_date}' and created < '{finish_week_date}' and status = 1;
        """)

    d2 = db.execute_sql_int(f"""
        select count(*) from catalog_productreview
        where created >= '{start_3months_date}' and created < '{finish_3months_date}' and status = 1;
        """)

    d1 = db.calculate('d', d, d2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'd', d, d2, d1)

    # Отзывов: D'(D'1% количество отзывов по сравнению с прошлыми 3 месяцами. По catalog_userevent.)
    # ProductReviewAdd = 10
    dd = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_week_date}' and created < '{finish_week_date}' and category = 10;
        """)

    dd2 = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_3months_date}' and created < '{finish_3months_date}' and category = 10;
        """)

    dd1 = db.calculate('dd', dd, dd2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'dd', dd, dd2, dd1)

    # Фото: E(E1% количество одобренных фото по сравнению с прошлыми 3 месяцами. По catalog_productimage.)
    e = db.execute_sql_int(f"""
        select count(*) from catalog_productimage
        where created >='{start_week_date}' and created < '{finish_week_date}' and status = 1;
        """)

    e2 = db.execute_sql_int(f"""
        select count(*) from catalog_productimage
        where created >= '{start_3months_date}' and created < '{finish_3months_date}' and status = 1;
        """)

    e1 = db.calculate('e', e, e2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'e', e, e2, e1)

    # Фото: E'(E'1% количество одобренных фото по сравнению с прошлыми 3 месяцами. По catalog_userevent.)
    # ProductImageAdd = 7
    # ProductImageApprove = 8
    ee = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_week_date}' and created < '{finish_week_date}' and category in (7, 8);
        """)

    ee2 = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_3months_date}' and created < '{finish_3months_date}' and category in (7, 8);
        """)

    ee1 = db.calculate('ee', ee, ee2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'ee', ee, ee2, ee1)

    # Цен: F(F1% количество добавленных цен по сравнению с прошлыми 3 месяцами. По catalog_price.)
    f = db.execute_sql_int(f"""
        select count(*) from catalog_price
        where created >= '{start_week_date}' and created < '{finish_week_date}' and status = 1;
        """)

    f2 = db.execute_sql_int(f"""
        select count(*) from catalog_price
        where created >= '{start_3months_date}' and created < '{finish_3months_date}' and status = 1;
        """)

    f1 = db.calculate('f', f, f2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'f', f, f2, f1)

    # Цен: F'(F'1% количество добавленных цен по сравнению с прошлыми 3 месяцами. По catalog_userevent.)
    # PriceAdd = 4
    # PriceApprove = 5
    ff = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_week_date}' and created < '{finish_week_date}' and category in (4, 5);
        """)

    ff2 = db.execute_sql_int(f"""
        select count(*) from catalog_userevent
        where created >= '{start_3months_date}' and created < '{finish_3months_date}' and category in (4, 5);
        """)

    ff1 = db.calculate('ff', ff, ff2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'ff', ff, ff2, ff1)

    # Активных пользователей: G(G1% активные пользователи за месяц по любому действию
    #                          (сколько людей, зарегистрированных в текущем месяце, сделали хотя бы одно действие))
    # Без учета: ProfileBirthday = 17, LegacyRating = 1.
    g = db.execute_sql_int(f"""
        select count(*) from users_user
        where date_joined >= '{start_week_date}' and date_joined < '{finish_week_date}'
        and id in (select user_id from catalog_userevent
        where created >= '{start_week_date}' and created < '{finish_week_date}'
        and category in (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27));
        """)

    g2 = db.execute_sql_int(f"""
        select count(*) from users_user
        where date_joined >= '{start_3months_date}' and date_joined < '{finish_3months_date}'
        and id in (select user_id from catalog_userevent
        where created >= '{start_3months_date}' and created < '{finish_3months_date}'
        and category in (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27));
        """)

    g1 = db.calculate('g', g, g2, start_3months_date, finish_3months_date)

    db_analytics.write_parameter(analytics_bot_id, 'g', g, g2, g1)

    db.close()
    db_analytics.close()

    webhook_url = 'XXXXXX'

    one_day = datetime.timedelta(days=1)

    # Если будут проблемы с локалями, то
    # https://stackoverflow.com/questions/14547631/python-locale-error-unsupported-locale-setting
    # 'ru' and 'pl' for Windows, 'ru_RU' and 'pl_PL' for Mac OS
    loc_num = locale.getlocale(locale.LC_NUMERIC)

    if sys.platform.startswith('linux'):
        locale.setlocale(locale.LC_NUMERIC, 'pl_PL')
    elif sys.platform.startswith('darwin'):
        locale.setlocale(locale.LC_NUMERIC, 'pl_PL')
    elif sys.platform.startswith('win32'):
        locale.setlocale(locale.LC_NUMERIC, 'pl')
    else:
        locale.setlocale(locale.LC_NUMERIC, 'pl_PL')

    slack_text = """
*==============================*

*Cтатистика за прошлую неделю в сравнении с показателями предыдущих трех месяцев:*

Данные формируются за прошлую неделю и сравниваются со средним значением за прошлые 3 месяца по формуле:
_*x1*_ = (_*x*_ / _*7*_ - _*x2*_ / _*90*_) / (_*x2*_ / _*90*_) * _*100*_,
где
_*x*_ - количество пользователей/действий за предыдущую неделю,
_*x2*_ - количество пользователей/действий за предыдущие 3 месяца,
_*x1*_ - процент изменения пользователей/действий в среднем за один день предыдущей недели к среднему за один день 3-х предыдущих месяцев,
_*7*_ - количество дней в неделе,
_*90*_ - количество дней в предыдущих трёх месяцах - может быть равно 90, 91, 92 в зависимости от количества дней в месяцах.

Данные выводятся в формате:  _*x*_ (_*x1*_ %)

*==============================*

Прошлая неделя: _*{start_week_date}*_ - _*{finish_week_date}*_.
Предыдущие три месяца: _*{start_3months_date}*_ - _*{finish_3months_date}*_.

*==============================*

Новых пользователей:   _*{A}*_  (_*{A1}* %_)

Регистраций:   _*{B}*_  (_*{B1}*_ %)

Сканирований:   _*{CC_MINUS_WEB}*_  (_*{CC_MINUS_WEB1}*_ %)

Отзывов:   _*{DD}*_  (_*{DD1}*_ %)

Фотографий:   _*{EE}*_  (_*{EE1}*_ %)

Цен:   _*{F}*_  (_*{F1}*_ %)

Активных пользователей:   _*{G}*_  (_*{G1}*_ %)

*==============================*
""".format(start_week_date=start_week_date.strftime('%d.%m.%Y'),
           finish_week_date=(finish_week_date - one_day).strftime('%d.%m.%Y'),
           start_3months_date=start_3months_date.strftime('%d.%m.%Y'),
           finish_3months_date=(finish_3months_date - one_day).strftime('%d.%m.%Y'),
           A=format(a, 'n'), A1=format(a1, 'n'),
           B=format(b, 'n'), B1=format(b1, 'n'),
           C=format(c, 'n'), C1=format(c1, 'n'),
           C_MINUS_WEB=format(c_minus_web, 'n'), C_MINUS_WEB1=format(c_minus_web1, 'n'),
           CC=format(cc, 'n'), CC1=format(cc1, 'n'),
           CC_MINUS_WEB=format(cc_minus_web, 'n'), CC_MINUS_WEB1=format(cc_minus_web1, 'n'),
           D=format(d, 'n'), D1=format(d1, 'n'),
           DD=format(dd, 'n'), DD1=format(dd1, 'n'),
           E=format(e, 'n'), E1=format(e1, 'n'),
           EE=format(ee, 'n'), EE1=format(ee1, 'n'),
           F=format(f, 'n'), F1=format(f1, 'n'),
           FF=format(ff, 'n'), FF1=format(ff1, 'n'),
           G=format(g, 'n'), G1=format(g1, 'n'))

    locale.setlocale(locale.LC_NUMERIC, loc_num)

# """
# Сканирований:   _*{C}*_  (_*{C1}*_ %)  _по таблице catalog_historyitem_.
# Сканирований:   _*{C_MINUS_WEB}*_  (_*{C_MINUS_WEB1}*_ %)  _по таблице catalog_historyitem без учета сканирований через Web_.
# Сканирований:   _*{CC}*_  (_*{CC1}*_ %)  _по таблице catalog_userevent_.
# _*Сканирований:*_   _*{CC_MINUS_WEB}*_  (_*{CC_MINUS_WEB1}*_ %)  *_по таблице catalog_userevent без учета сканирований через Web_. Выбор редакции.*
#
# Отзывов:   _*{D}*_  (_*{D1}*_ %) _по таблице catalog_productreview_.
# _*Отзывов:*_   _*{DD}*_  (_*{DD1}*_ %) *_по таблице catalog_userevent_. Выбор редакции.*
#
# Фотографий:   _*{E}*_  (_*{E1}*_ %) _по таблице catalog_productimage_.
# _*Фотографий:*_   _*{EE}*_  (_*{EE1}*_ %) *_по таблице catalog_userevent_. Выбор редакции.*
#
# _*Цен:*_   _*{F}*_  (_*{F1}*_ %) *_по таблице catalog_price_. Выбор редакции.*
# Цен:   _*{FF}*_  (_*{FF1}*_ %) _по таблице catalog_userevent_.
# """


    slack_data = {
        'text': slack_text,
        'username': 'Analytic\'s bot',
        'icon_url': 'XXXXXX'#,
        #'channel': '#marketing'
    }

    response = requests.post(url=webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
    logger.info("Request is sent. Return code: %s. Return text: %s.", response.status_code, response.text)

    logger.info('******************************************************************')
    logger.info('*****                 analytic_bot finished                  *****')
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
