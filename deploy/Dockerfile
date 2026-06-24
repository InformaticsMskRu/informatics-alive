FROM python:3.6

ENV PYTHONUNBUFFERED=1 \
    FLASK_APP=wsgi:application \
    PYTHONPATH=/code

WORKDIR /code/rmatics

COPY rmatics/requirements.txt ./requirements.txt
COPY deploy/requirements-extra.txt ./requirements-extra.txt

RUN sed -i \
      -e 's/^MarkupSafe==1\.0$/MarkupSafe==1.1.1/' \
      -e 's/^itsdangerous==0\.24$/itsdangerous==1.1.0/' \
      requirements.txt \
 && pip install --no-cache-dir -r requirements.txt -r requirements-extra.txt

COPY rmatics /code/rmatics
COPY deploy /code/deploy

CMD ["flask", "run", "--host", "0.0.0.0", "--port", "12345", "--no-reload"]
