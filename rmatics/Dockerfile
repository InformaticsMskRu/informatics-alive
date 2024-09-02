FROM python:3.6

ENV FLASK_APP=wsgi:application
ENV PYTHONPATH=/code

COPY . /code/rmatics
WORKDIR /code/rmatics

RUN pip3 install --requirement requirements.txt

CMD flask run
