import requests
import json
from typing import Tuple


class Users:
    


class Channels:
    slack_url = 'https://hooks.slack.com/'
    urgent_alerts = slack_url + ''
    dl_dev = slack_url + ''
    dl_prod = slack_url + ''


def send_alert(alert_message, channel, mention_users: Tuple[str] = ()):
    slack_url = channel
    mention_string = ' '.join(mention_users) + ' pay attention to the following alert\n' if mention_users else ''
    report_string = mention_string + f'```{alert_message}```\n'
    requests.post(slack_url, data=json.dumps({'text': report_string}))
