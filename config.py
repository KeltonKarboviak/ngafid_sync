# -*- coding: utf-8 -*-

import logging
import os
import pathlib
import time

from dotenv import load_dotenv
from slack_log_utils import SlackWebhookFormatter, SlackWebhookHandler


dotenv_path = pathlib.Path(pathlib.Path(__file__).parent, '.env')
load_dotenv(dotenv_path)

LOG_PATH = os.environ.get('LOG_PATH', 'logs/')
SERVER_PREFIX = os.environ.get('SERVER_PREFIX')

logging_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'slack': {
            '()': SlackWebhookFormatter,
        },
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'formatter': 'default',
            'level': logging.DEBUG,
            'filename': pathlib.Path(
                LOG_PATH, f'sync.log.{time.strftime("%Y-%m-%d")}'
            )
        },
        'slack': {
            '()': SlackWebhookHandler,
            'formatter': 'slack',
            'level': logging.WARNING,
            'url': os.environ.get('SLACK_WEBHOOK_URL'),
        },
    },
    'loggers': {
        'sync': {
            'level': logging.DEBUG,
            'handlers': ['file', 'slack'],
        },
    },
}
