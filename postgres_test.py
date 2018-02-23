#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extensions
import pprint
import logging



try:
    conn = psycopg2.connect(dbname='catalog',
                            user='test.analytic',
                            password='XXXXXX',
                            host='XXXXXX',
                            port='5432',
                            connect_timeout=15) # seconds
except psycopg2.Error as e:
    print('Error code: %s\nText code: %s' % (e.pgcode, e.pgerror))

    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    cur = conn.cursor()

    cur.execute('SET statement_timeout TO \'2min\'')
    cur.execute('SHOW statement_timeout')
    print('statement_timeout =', cur.fetchone())

    cur.execute('SELECT count(id) FROM public.users_user;')

except psycopg2.Error as e:
    print('Error code: %s\nText code: %s' %(e.pgcode, e.pgerror))

#print(cur.fetchone())
x = cur.fetchone()[0]
print(x, type(x), repr(x))

# rows = cur.fetchall()
# for row in rows:
#     print("   ", row[1][1])
# pprint.pprint(rows)

cur.close()
conn.close()
