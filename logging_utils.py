# -*- coding: utf-8 -*-

import json
import logging

import requests


class SlackWebhookHandler(logging.Handler):

    def __init__(self, url: str, level: int = logging.NOTSET):
        self._url = url

        super(SlackWebhookHandler, self).__init__(level)

    def emit(self, record: logging.LogRecord):
        log_entry = self.format(record)

        return requests.post(
            self._url,
            log_entry,
            headers={'Content-type': 'application/json'}
        ).content


class SlackWebhookFormatter(logging.Formatter):
    COLOR_GREEN = '#36a64f'
    COLOR_RED = '#ff0000'

    def __init__(self):
        super(SlackWebhookFormatter, self).__init__()

    def format(self, record: logging.LogRecord) -> str:
        """
        Transforms a LogRecord into a JSON string that complies with the
        Slack API (https://api.slack.com/incoming-webhooks).  First, call the
        logging.Formatter's format method to handle the basic formatting of
        the message. Then take the pieces out of the record that we want,
        put it into the JSON string, and return it.
        :param record: logging.LogRecord
        :return: str JSON string in format required by Slack API
        """
        formatted_msg = super(SlackWebhookFormatter, self).format(record)

        color = self.COLOR_GREEN \
            if record.levelno < logging.WARNING \
            else self.COLOR_RED

        attachment = {
            'author_name': record.name,
            'title': getattr(record, 'title', ''),
            'text': formatted_msg,
            'fallback': formatted_msg,
            'color': color,
        }

        return json.dumps({
            'attachments': [
                attachment
            ]
        })
