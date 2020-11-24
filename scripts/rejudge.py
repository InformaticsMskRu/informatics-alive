from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import requests

engine = create_engine('mysql+pymysql://localhost/')

row = list(engine.execute("select id from pynformatics.runs where ej_status=377 and id > 24789179 order by id desc limit 1000"))

for e in row:
    r = requests.post("http://localhost:12346/problem/run/{}/action/rejudge".format(e[0]))
    print(e[0], r.status_code) 
