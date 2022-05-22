import configparser
import json
import os
import time
import traceback
from threading import Thread

from pika.adapters.utils.connection_workflow import AMQPConnectorException
from pika.exceptions import AMQPError

import pika_utils
from app_state import app_state
from errors_manager import errors_manager
from service_state import ServiceState


class RmqController:
    def __init__(self):
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(os.environ['CONF_PATH'] + '/rmq_connection_details.ini')
        self.rabbitmq_host = parser.get('global_rmq', 'host')
        self.rabbitmq_port = parser.get('global_rmq', 'port')
        self.rabbitmq_username = parser.get('global_rmq', 'username')
        self.rabbitmq_password = parser.get('global_rmq', 'password')

        self._connect_to_commands_queue()

        self.heartbeat_listener_thread = Thread(target=self.heartbeats_thread_target_function, daemon=False)
        self.errors_listener_thread = Thread(target=self.errors_thread_target_function, daemon=False)

    def _connect_to_commands_queue(self):
        self.commands_channel = pika_utils.Blocking.exchange('commands', pika_utils.TOPIC, host=self.rabbitmq_host,
            port=self.rabbitmq_port, username=self.rabbitmq_username, password=self.rabbitmq_password)

    @staticmethod
    def parse_rmq_message(body):
        message = json.loads(body.decode('utf8'))
        return message

    @staticmethod
    def heartbeat_callback(channel, method, properties, body):
        try:
            message = RmqController.parse_rmq_message(body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            pipeline = message.get('pipeline')
            service = message.get('service')
            state = message.get('state')
            last_received_message_datetime = message.get('last_received_message_datetime')
            last_sent_message_datetime = message.get('last_sent_message_datetime')
            last_heartbeat_datetime = message.get('last_heartbeat_datetime')
            with app_state.lock:
                if pipeline not in app_state.pipelines_state or service not in app_state.pipelines_state[pipeline]:
                    return
                service_state: ServiceState = app_state.pipelines_state[pipeline][service]
                service_state.last_received_message_datetime = last_received_message_datetime
                service_state.last_sent_message_datetime = last_sent_message_datetime
                service_state.last_heartbeat_datetime = last_heartbeat_datetime
                service_state.suspended = state == 'suspended'
        except Exception:
            print('Error on hearbeat processing:', traceback.format_exc())

    def heartbeats_thread_target_function(self):
        while True:
            try:
                self.heartbeats_channel = pika_utils.Blocking.queue('heartbeats',
                    callback=RmqController.heartbeat_callback, host=self.rabbitmq_host, port=self.rabbitmq_port,
                    username=self.rabbitmq_username, password=self.rabbitmq_password)
                self.heartbeats_channel.start_consuming()
            except (AMQPError, AMQPConnectorException):
                print('Unable to connect to heartbeats queue. Reconnecting in 10 seconds')
                time.sleep(10)

    @staticmethod
    def error_callback(channel, method, properties, body):
        try:
            message = RmqController.parse_rmq_message(body)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            errors_manager.publish_error(message)
        except Exception:
            print('Error on error report processing:', traceback.format_exc())

    def errors_thread_target_function(self):
        while True:
            try:
                self.errors_channel = pika_utils.Blocking.queue('errors', callback=RmqController.error_callback,
                    host=self.rabbitmq_host, port=self.rabbitmq_port, username=self.rabbitmq_username,
                    password=self.rabbitmq_password)
                self.errors_channel.start_consuming()
            except (AMQPError, AMQPConnectorException):
                print('Unable to connect to errors queue. Reconnecting in 10 seconds')
                time.sleep(10)

    def send_command(self, target, command, command_arguments):
        import pika
        try:
            self.commands_channel.basic_publish(exchange='commands', routing_key=target,
                body=json.dumps({'command': command, 'args': command_arguments}),
                properties=pika.BasicProperties(expiration='10000'))
            return True
        except (AMQPError, AMQPConnectorException):
            try:
                # reconnect and publish again
                self._connect_to_commands_queue()
                self.commands_channel.basic_publish(exchange='commands', routing_key=target,
                    body=json.dumps({'command': command, 'args': command_arguments}),
                    properties=pika.BasicProperties(expiration='10000'))
                return True
            except (AMQPError, AMQPConnectorException):
                print('Unable to connect to commands queue. Try again later.')
                return False

    def run(self):
        self.heartbeat_listener_thread.start()
        self.errors_listener_thread.start()


rmq_controller = RmqController()
