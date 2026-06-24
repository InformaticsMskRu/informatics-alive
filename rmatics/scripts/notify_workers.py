import click
from gevent import monkey

monkey.patch_all()

from gevent.pool import Group

from rmatics.wsgi import application
from rmatics.ejudge.notify.worker import NotifyWorker

from rmatics import create_app
from rmatics.config import CONFIG_MODULE

@application.cli.command()
@click.option('--workers', default=2)
def main(workers):
    create_app(config=CONFIG_MODULE, config_logger=False)
    worker_group = Group()
    for i in range(workers):
        worker_group.start(NotifyWorker(i + 1))
    worker_group.join()


if __name__ == '__main__':
    main()
