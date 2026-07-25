import os

from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import requests

HEADERS = {'Authorization': 'Bearer {}'.format(os.getenv('TRUSTED_TOKEN'))}

engine = create_engine('mysql+pymysql://localhost/')

row = list(engine.execute("select id from pynformatics.runs where ej_status=520 and create_time > '2022-10-14 20:44:23' and ej_contest_id = 1992;"))

for e in row:
    r = requests.post("http://localhost:12346/problem/run/{}/action/rejudge".format(e[0]),
                      headers=HEADERS)
    print(e[0], r.status_code) 
