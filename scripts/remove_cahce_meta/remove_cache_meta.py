import datetime

from rmatics import db, create_app
from rmatics.model import MonitorCacheMeta
from rmatics.config import CONFIG_MODULE

# Cron every hour
# 0 * * * * echo "прошёл один час"
def main():
    current_time = datetime.datetime.utcnow()
    print(f'Job running at {current_time}')
    app = create_app(config=CONFIG_MODULE)
    with app.app_context():
        db.session.query(MonitorCacheMeta). \
            filter(MonitorCacheMeta.when_expire < current_time) \
            .delete()

        db.session.commit()
    print('Success')


if __name__ == '__main__':
    main()

