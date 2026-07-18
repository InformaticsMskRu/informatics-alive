from rmatics import create_app
from rmatics.model.base import celery as celery_app
from rmatics.config import CONFIG_MODULE

# Prevent app factory to configure own logger
# as it breaks celery task default logger
# TODO: worker_hijack_root_logger or @after_setup_task_logger
app = create_app(CONFIG_MODULE, config_logger=False)

celery = celery_app
