import json
import os
import traceback

import requests
import telegram
from celery import Celery
from pymongo import MongoClient

BOT_AUTH_TOKEN = os.getenv('BOT_AUTH_TOKEN')
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL')
RMATICS_SERVICE_URL = os.getenv('RMATICS_SERVICE_URL', 'http://localhost:12346/')
# Токен, под которым ходят в trusted api rmatics
TRUSTED_TOKEN = os.getenv('TRUSTED_TOKEN')
RMATICS_HEADERS = {'Authorization': f'Bearer {TRUSTED_TOKEN}'}

app = Celery('tasks', broker=CELERY_BROKER_URL)
bot = telegram.Bot(BOT_AUTH_TOKEN)

PROBLEM_ID = 2936
USER_ID = 1
FILE = 'data.cpp'
LANG = 3


def send_submit():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file = open(os.path.join(current_dir, FILE), 'r')
    data = {
        'lang_id': LANG,
        'user_id': USER_ID,
    }
    url = f'{RMATICS_SERVICE_URL}problem/trusted/{PROBLEM_ID}/submit_v2'
    try:
        resp = requests.post(url, files={'file': file}, data=data, headers=RMATICS_HEADERS)
    except (requests.ConnectionError, requests.ConnectTimeout):
        return 'Выглядит как rmatics отвалился', None
    except requests.RequestException:
        return resp.text, None
    try:
        resp.raise_for_status()
    except:
        return json.dumps(resp.json()), None
    context = resp.json()
    if context['status'] != 'success':
        return json.dumps(context)
    run_id = context['data']['run_id']
    return f'Посылка отправлена, #{run_id}', run_id


def get_submission_status():
    args = {
        'lang_id': LANG,
        'count': 1,
        'user_id': USER_ID,
        'group_id': 0,
        'to_timestamp': -1,
        'from_timestamp': -1,
        'statement_id': 0,
        'page': 1,
    }
    args = '&'.join([f'{k}={v}' for k, v in args.items()])
    url = f'{RMATICS_SERVICE_URL}problem/{PROBLEM_ID}/submissions/?{args}'
    try:
        resp = requests.get(url, headers=RMATICS_HEADERS)
    except (requests.ConnectionError, requests.ConnectTimeout):
        return 'Выглядит как rmatics отвалился', None
    except requests.RequestException:
        return resp.text, None
    try:
        resp.raise_for_status()
    except:
        return json.dumps(resp.json()), None
    context = resp.json()
    if context['result'] != 'success':
        return json.dumps(context)
    ejudge_status_id = context['data'][0]['ejudge_status']
    return '', ejudge_status_id


@app.task(bind=True)
def send_submit_task(self, chat_id):
    try:
        response, run_id = send_submit()
    except Exception as e:
        response = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        run_id = None
        self.request.callbacks = None
    bot.send_message(chat_id, response)
    return chat_id, run_id


@app.task(bind=True)
def check_source_in_mongo_task(self, args):
    chat_id, run_id = tuple(args)
    try:
        mongo = MongoClient('mongodb://localhost/test')
        source = mongo.test.source.find_one({'run_id': run_id})
    except Exception as e:
        traceback_str = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        bot.send_message(chat_id, traceback_str)
        self.request.callbacks = None
        return
    if source is None:
        self.request.callbacks = None
        response = 'Исходник не найден в mongo!'
    else:
        response = 'Исходник положен в mongo'
    bot.send_message(chat_id, response)
    return chat_id, run_id


def on_check_for_status_failure(self, exc, task_id, args, kwargs, einfo):
    chat_id, run_id = tuple(args[0])
    bot.send_message(chat_id, f'Тест для #{run_id} провалился!')


@app.task(bind=True,
          default_retry_delay=5,
          retry_kwargs={'max_retries': 3},
          on_failure=on_check_for_status_failure)
def check_for_status_task(self, args):
    chat_id, run_id = tuple(args)
    try:
        response, status_id = get_submission_status()
    except Exception as e:
        response = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        bot.send_message(chat_id, response)
        raise self.retry()

    if status_id is None:
        bot.send_message(chat_id, response)

    if status_id != 0:
        bot.send_message(chat_id, f'Статус посылки пока что {status_id}, подождем ещё немного')
        raise self.retry()

    bot.send_message(chat_id, 'Статус посылки ОК')
    return chat_id, run_id


@app.task(bind=True)
def check_protocol_in_mongo_task(self, args):
    chat_id, run_id = tuple(args)
    try:
        mongo = MongoClient('mongodb://localhost/test')
    except Exception as e:
        traceback_str = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        bot.send_message(chat_id, traceback_str)
        self.request.callbacks = None
        return
    protocol = mongo.test.protocol.find_one({'run_id': run_id})
    if not protocol:
        self.request.callbacks = None
        response = 'Протокол не найден в mongo!'
    else:
        response = 'Протокол лежит в mongo, заканчиваем тест'
    bot.send_message(chat_id, response)
    return chat_id, run_id


full_submission_cycle_test = send_submit_task.s() | \
                             check_source_in_mongo_task.s() | \
                             check_for_status_task.s() | \
                             check_protocol_in_mongo_task.s()
