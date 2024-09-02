FROM ubuntu:focal

ENV FLASK_ENV=production
ENV FLASK_APP=wsgi:application
ENV PYTHONPATH=/code
ENV DEBUG=False
ENV TESTING=False   

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt update && \
 apt install -y vim && \
 apt-get install -y curl iproute2 && \
 apt-get install -y python3-pip uwsgi-core uwsgi-plugin-python3 && \
 apt-get install -y python3-dev default-libmysqlclient-dev build-essential pkg-config
 
COPY . /code/rmatics
WORKDIR /code/rmatics

RUN pip3 install --requirement requirements.txt

CMD ["uwsgi", "--http", ":5000", "--processes", "1", "--threads", "2", "--wsgi-file", "wsgi.py", "--callable", "application", "-b", "32768"]
