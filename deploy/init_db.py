from rmatics import create_app
from rmatics.config import CONFIG_MODULE
from rmatics.model.base import db

import rmatics.model


def main():
    app = create_app(config=CONFIG_MODULE)
    with app.app_context():
        db.create_all()
        print('OK: all mapped tables created in moodle / ejudge / pynformatics')


if __name__ == '__main__':
    main()
