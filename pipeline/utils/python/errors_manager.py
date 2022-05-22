import configparser
import datetime
import json
import os
import time
import traceback
from threading import Thread, Lock

import requests


class ErrorsManager:
    IN_MEMORY_CACHE_LIMIT = 1000
    IN_MEMORY_CACHE_DEALLOCATION_RATIO = 100
    ERRORS_BATCH_SIZE = 1000
    BATCH_COLLECTION_TIME_DELTA = 60
    ERRORS_STORAGE_PATH = 'errors/'

    def __init__(self):
        self.lock = Lock()
        self._slack_notification_thread = Thread(target=self.__scheduled_slack_notification_task)

        self._last_batch_size = 0
        self._in_memory_storage = []
        self._current_batch_file = None
        self.__load_errors(self.ERRORS_STORAGE_PATH)

        self._errors_by_sender = {}

        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(os.environ['CONF_PATH'] + '/error_reporting.ini')
        self.slack_url = parser.get('slack', 'url')

    def __scheduled_slack_notification_task(self):
        while True:
            try:
                with self.lock:
                    max_by_sender, messages_total = self.calculate_errors_number()
                    if max_by_sender == 0:
                        pass
                    else:
                        send_only_statistics = messages_total > 5
                        report_string = 'Data loading exceptions occurred\n'
                        filenames = []
                        attachments = []
                        for sender, errors in self._errors_by_sender.items():
                            attachment_text = f':clipboard: In service {sender}: \n'
                            if send_only_statistics:
                                attachment_text += f'{sender} - {len(errors)}'
                            else:
                                for i, error in enumerate(errors):
                                    attachment_text += f'```{error["text"]}```\n'
                            attachments.append({'text': attachment_text, 'color': 'BB2222'})
                            filenames.append(self._dump_errors_by_sender(errors, sender))
                        report_string += 'Check hive for more details...'

                        if len(filenames) > 0:
                            report_string += f'\nStacktraces and causes are available in files: {", ".join(filenames)}'

                        requests.post(self.slack_url, data=json.dumps({'text': report_string, 'attachments': attachments}),
                            headers={'Content-Type': 'application/json'})
                        self._errors_by_sender.clear()

                time.sleep(self.BATCH_COLLECTION_TIME_DELTA)
            except Exception as e:
                print(traceback.format_exc())

    def _dump_errors_by_sender(self, errors, sender):
        timestamp = str(datetime.datetime.now()).replace(':', '-').replace(' ', '_')
        filename = f'errors_{sender}_{timestamp}.json'
        with open(f'{self.ERRORS_STORAGE_PATH}/{filename}', 'w') as errors_file:
            errors_file.write('[\n')
            for i, error in enumerate(errors):
                errors_file.write(f'    {json.dumps(error)}{"," if i < len(error) - 1 else ""}\n')
            errors_file.write(']')
        return filename

    def calculate_errors_number(self):
        errors_numbers_by_sender = [len(x) for x in self._errors_by_sender.values()]
        max_by_sender = max([0] + errors_numbers_by_sender)
        messages_total = sum([0] + errors_numbers_by_sender)
        return max_by_sender, messages_total

    def publish_error(self, error):
        with self.lock:
            self._in_memory_storage.append(error)
            if len(self._in_memory_storage) < self.IN_MEMORY_CACHE_LIMIT:
                with open(self.ERRORS_STORAGE_PATH + 'cache.json', 'a') as cache_output_file:
                    cache_output_file.write(json.dumps(error) + '\n')
            else:
                del self._in_memory_storage[:int(self.IN_MEMORY_CACHE_LIMIT * self.IN_MEMORY_CACHE_DEALLOCATION_RATIO)]
                with open(self.ERRORS_STORAGE_PATH + 'cache.json', 'w') as cache_output_file:
                    for error_message in self._in_memory_storage:
                        cache_output_file.write(json.dumps(error_message) + '\n')

            max_by_sender, messages_total = self.calculate_errors_number()
            if messages_total > self.ERRORS_BATCH_SIZE:
                print('Error publishing rejected: too many error messages (> 1000)')

            sender = str(error['pipeline']) + '_' + str(error['service'])
            if sender not in self._errors_by_sender:
                self._errors_by_sender[sender] = []
            self._errors_by_sender[sender].append(error)

    def get_errors_report(self):
        result = []
        with self.lock:
            for error in self._in_memory_storage:
                timestamp_suffix = '' if error.get("timestamp") is None else f' at {error.get("timestamp")}'
                result.append((f'{error["pipeline"]}:{error["service"]}{timestamp_suffix}',
                str(error['text']), str(error['cause'])))
        return result

    def __load_errors(self, path: str):
        if not os.path.exists(path + 'cache.json'):
            return
        with open(path + 'cache.json') as cache_output_file:
            for error in cache_output_file.read().split('\n'):
                if len(error) > 0:
                    self._in_memory_storage.append(json.loads(error))

    def run(self):
        self._slack_notification_thread.start()


errors_manager = ErrorsManager()
