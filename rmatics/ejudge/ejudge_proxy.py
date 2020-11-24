import codecs
import requests
import re
import json
from flask import current_app

DEFAULT_ERROR_STR = 'Ошибка отправки задачи'

STATUS_REPR = {
  0 : 'Задача отправлена на проверку',  # NEW_SRV_ERR_NO_ERROR
120 : 'Отправка пустого файла',         # NEW_SRV_ERR_FILE_EMPTY
105 : 'Отправка бинарного файла',       # NEW_SRV_ERR_BINARY_FILE
 82 : 'Эта посылка является копией предыдущей',  # NEW_SRV_ERR_DUPLICATE_SUBMIT
 37 : 'Этот язык не может быть использован для этой задачи. Обратитесь к администраторам.',
 83 : 'Задача уже решена',         # NEW_SRV_ERR_PROB_ALREADY_SOLVED
 78 : 'Отправляемый файл превышает допустимый размер (64K) или превышена квота на число посылок (обратитесь к админимтратору)',  # NEW_SRV_ERR_RUN_QUOTA_EXCEEDED
113 : 'Отправляемый файл пустой',  # SUBMIT_EMPTY
1000: 'Отправляемый файл превышает допустимый размер. Требуется отправить исходный код или текстовый файл',
}


def report_error(code, login_data, submit_data, file, filename, user_id, addon = ''):
    t = str({'info' : addon, 'login_data' : login_data, 'submit_data' : submit_data, 'filename' : filename})
    log=codecs.open('/var/log/python.log', 'a', 'utf-8')
    log.write(t)
    log.write('\n---\n')
    log.close()


def submit(run_file, contest_id, prob_id, lang_id, login, password, filename, url):
    files = {'file' : (filename, run_file)}

    submit_data = {
        'lang_id' : lang_id,
        'action' : 'submit-run',
        'problem': prob_id,
        'prob_id': prob_id,
        'sender_user_id': 5,
        'contest_id': contest_id,
    }

    headers = {
        'Authorization': 'Bearer ' + current_app.config.get('EJUDGE_MASTER_TOKEN')
    }

    current_app.logger.info('Request {}'.format(submit_data))

    c = requests.put(url, data=submit_data, headers=headers, files=files)

    text_response = str(c.text)

    current_app.logger.info('Response {}'.format(text_response))
    try:
        resp = json.loads(text_response)
    except:
        current_app.logger.info('{} {}'.format(c.status_code, text_response))
        raise

    if resp['ok']:
        return {
            'code': 0,
            'message': STATUS_REPR[0],
            **resp['result']
        }

    current_app.logger.info('{} {}'.format(c.status_code, text_response))

    code = resp["error"]["num"]
    if code in STATUS_REPR:
        return {
            'code': code,
            'message': STATUS_REPR[code]
        }
    elif -code in STATUS_REPR:
        return {
            'code': -code,
            'message': STATUS_REPR[-code]
        }
    else:
        return {
            'code': code,
            'message': DEFAULT_ERROR_STR + " (" + str(code) + " " + resp["error"]["message"] +  ")",
        }

