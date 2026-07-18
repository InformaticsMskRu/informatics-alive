FROM python:3.6

ENV FLASK_APP=wsgi:application
ENV PYTHONPATH=/code
# ENV PIP_INDEX_URL=https://registry.tcsbank.ru/repository/pypi-all/simple/

COPY . /code/rmatics
WORKDIR /code/rmatics

RUN pip3 install --requirement requirements.txt

CMD flask run
