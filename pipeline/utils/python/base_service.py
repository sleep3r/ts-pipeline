import json
import os
import threading
import time
import traceback
from datetime import datetime
from typing import Union, List, Dict, Optional

from _socket import gaierror
from pika.exceptions import AMQPError
from pika.adapters.utils.connection_workflow import AMQPConnectorException
from pika import BasicProperties

import pika_utils
from config import Config
from state import State


class RmqInputInfo:
    def __init__(self, queue_name, exchange_name=None, prefetch_count: int = 100, durable=True):
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.prefetch_count = prefetch_count
        self.durable = durable

    def create_channel(self, profile, callback):
        if self.exchange_name is None:
            print(f'Creating input connection to queue {self.queue_name}')
            return pika_utils.Blocking.queue(queue_name=self.queue_name, callback=callback, host=profile['host'],
                port=profile['port'], username=profile['username'], password=profile['password'],
                prefetch_count=self.prefetch_count, durable=self.durable)
        else:
            print(f'Creating input connection to exchange {self.exchange_name} through {self.queue_name}')
            queue_name = self.exchange_name + '.' + self.queue_name
            return pika_utils.Blocking.queue(queue_name=queue_name, callback=callback, exchange_name=self.exchange_name,
                exchange_type=pika_utils.FANOUT, host=profile['host'], port=profile['port'],
                username=profile['username'], password=profile['password'],
                prefetch_count=self.prefetch_count, durable=self.durable)


class RmqOutputInfo:
    def __init__(self, name=None, is_exchange=False, durable=True):
        self.name = name
        self.is_exchange = is_exchange
        self.durable = durable
        self.properties = RmqOutputInfo.__create_publishing_properties(durable)

    @staticmethod
    def __create_publishing_properties(durable: bool):
        result = BasicProperties()
        if durable:
            result.delivery_mode = 2
        return result

    def create_channel(self, profile):
        if self.is_exchange:
            print(f'Creating output connection to exchange {self.name}')
            return pika_utils.Blocking.exchange(name=self.name, exchange_type=pika_utils.FANOUT, host=profile['host'],
                port=profile['port'], username=profile['username'], password=profile['password'], durable=self.durable)
        else:
            print(f'Creating output connection to queue {self.name}')
            return pika_utils.Blocking.queue(queue_name=self.name, host=profile['host'], port=profile['port'],
                username=profile['username'], password=profile['password'],
                durable=self.durable)


class BaseService:
    def __init__(self, input: Union[RmqInputInfo, str] = None,
            outputs: Union[RmqOutputInfo, List[str], List[RmqOutputInfo], str] = None,
            state_dump_interval = 1, subscription_tags: Union[str, List[str]] = None):
        self.flow_name = os.environ.get('FLOW_NAME')
        self.service_name = os.environ.get('SERVICE_NAME')
        self.input = None if input is None else RmqInputInfo(input) if type(input) == str else input
        self.outputs = [] if outputs is None \
            else [RmqOutputInfo(outputs)] if type(outputs) == str \
            else [outputs] if type(outputs) == RmqOutputInfo \
            else [RmqOutputInfo(output) if type(output) == str else output for output in outputs]
        self.state_dump_interval = state_dump_interval
        self.subscription_tags = [subscription_tags] if isinstance(subscription_tags, str) \
            else subscription_tags if isinstance(subscription_tags, list) \
            else []

        self.suspended = True
        self.terminated = False
        self.config = Config(None, None)

    def set_config(self, config):
        self.config = config

    def establish_rmq_connections(self):
        self.global_rmq_profile = dict(self.config.get_property_group('global_rmq'))
        self.local_rmq_profile = dict(self.config.get_property_group('local_rmq'))

        self._connect_to_input_queue()
        self._connect_to_output_queues()

        self.__connect_to_commands_queue()
        self.hive_commands_thread = threading.Thread(target=self.__listen_commands, daemon=True)
        self.__connect_to_heartbeats_queue()
        self.__connect_to_errors_queue()

    def load_state(self):
        self.state_path = 'state/' + self.service_name
        self.state = State(self.state_path, self.state_dump_interval)
        self._init_state()
        self.state.load()

    def _connect_to_input_queue(self):
        if self.input is not None:
            self.input_channel = self.input.create_channel(self.local_rmq_profile, self._handle_message_wrapper)
            print('Successfully set up input connection to local RabbitMQ')

    def _connect_to_output_queues(self):
        self.output_channels = []
        for output in self.outputs:
            self.output_channels.append(output.create_channel(self.local_rmq_profile))
            print('Successfully set up output connection to local RabbitMQ')

    def __connect_to_heartbeats_queue(self):
        self.hive_heartbeats_channel = pika_utils.Blocking.queue('heartbeats', host=self.global_rmq_profile['host'],
            port=self.global_rmq_profile['port'], username=self.global_rmq_profile['username'],
            password=self.global_rmq_profile['password'])
        self.hive_heartbeats_thread = threading.Thread(target=self.__send_heartbeats, daemon=True)

    def __connect_to_commands_queue(self):
        self.hive_commands_channel = pika_utils.Blocking.queue(self.flow_name + '.' + self.service_name,
            exchange_name='commands', routing_key=self.flow_name + '.' + self.service_name,
            exchange_type=pika_utils.TOPIC, callback=self.__handle_command_wrapper,
            host=self.global_rmq_profile['host'], port=self.global_rmq_profile['port'],
            username=self.global_rmq_profile['username'], password=self.global_rmq_profile['password'])

    def __connect_to_errors_queue(self):
        self.hive_errors_channel = pika_utils.Blocking.queue('errors', host=self.global_rmq_profile['host'],
            port=self.global_rmq_profile['port'], username=self.global_rmq_profile['username'],
            password=self.global_rmq_profile['password'])

    def _init_state(self):
        pass

    def _handle_message(self, message, headers):
        raise NotImplementedError()

    def _handle_message_wrapper(self, channel, method, properties, body):
        message = body.decode('utf8')
        channel.basic_ack(delivery_tag=method.delivery_tag)

        with self.state.write_lock:
            self.state._last_received_message_datetime = str(datetime.now())
            self.state._current_message = message
            self.state._current_headers = properties.headers
            self.state.dump(keys={'_last_received_message_datetime', '_current_message', '_current_headers'})
            try:
                if properties.headers and 'tag' in properties.headers \
                        and properties.headers['tag'] not in self.subscription_tags:
                    self._send(message, properties.headers)
                else:
                    self._handle_message(message, properties.headers)
            except Exception:
                self._report_error(traceback.format_exc(), cause=message)
            self.state._current_message = None
            self.state._current_headers = None
            self.state.dump(force=False)

    def _hard_shutdown(self, args):
        self.state.dump()
        self.terminated = True
        self._suspend({})

    def _suspend(self, args):
        self.suspended = True

    def _resume(self, args):
        self.suspended = False

    def _handle_predefined_command(self, command, args):
        if command.lower() == 'shutdown':
            print('Processing shutdown command')
            self._hard_shutdown(args)
        if command.lower() == 'suspend':
            print('Processing suspend command')
            self._suspend(args)
        if command.lower() == 'resume':
            print('Processing resume command')
            self._resume(args)

    def _handle_command(self, command, args):
        self._handle_predefined_command(command, args)

    def __handle_command_wrapper(self, channel, method, properties, body):
        message = json.loads(body)
        with self.state.write_lock:
            command = message.get('command')
            args = [] if message.get('args') is None else message.get('args')
            self._handle_command(command, args)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def _send(self, message, headers=None):
        while True:
            try:
                for output, output_channel in zip(self.outputs, self.output_channels):
                    properties = self.__merge_properties(output.properties, headers)
                    if output.is_exchange:
                        output_channel.basic_publish(exchange=output.name, routing_key='', body=message,
                                                     properties=properties)
                    else:
                        output_channel.basic_publish(exchange='', routing_key=output.name, body=message,
                                                     properties=properties)
                self.state._last_sent_message_datetime = str(datetime.now())
                break
            except (AMQPError, AMQPConnectorException):
                print('Sending failed due to local RMQ disconnect and is blocked until connection restored. '
                      'Reconnecting in 10 seconds')
                while True:
                    try:
                        time.sleep(10)
                        self._connect_to_output_queues()
                        break
                    except (AMQPError, AMQPConnectorException):
                        print('Unable to reconnect to output queues. Next attempt in 10 seconds')

    def __merge_properties(self, output_properties: BasicProperties,
            additional_headers: Optional[Dict[str, object]] = None):
        result = BasicProperties(
            delivery_mode=output_properties.delivery_mode,
            headers={},
        )

        result.headers.update(output_properties.headers or {})
        additional_headers = additional_headers or {}
        keys_intersection = set(result.headers.keys()) & set(additional_headers.keys())
        if len(keys_intersection) > 0:
            self._report_error(f'Warning: header key violation. Keys {keys_intersection} found in both '
                    f'{result.headers} and {additional_headers}')
        result.headers.update(additional_headers)

        return result

    def _run(self):
        self.hive_commands_thread.start()
        self.hive_heartbeats_thread.start()
        self.suspended = False

    def __listen_commands(self):
        while True:
            try:
                self.hive_commands_channel.start_consuming()
            except (AMQPError, AMQPConnectorException):
                print('Global RabbitMQ connection closed. Commands receiving interrupted. Reconnecting in 10 seconds')
                while True:
                    try:
                        time.sleep(10)
                        self.__connect_to_commands_queue()
                        print('Successfully reconnected to commands queue.')
                        break
                    except (AMQPError, AMQPConnectorException, gaierror):
                        print('Unable to reconnect to commands queue. Next attempt in 10 seconds')
            except Exception:
                print(traceback.format_exc())
                time.sleep(0.1)

    def __send_heartbeats(self):
        while True:
            try:
                heartbeat_message = {
                    'pipeline': self.flow_name,
                    'service': self.service_name,
                    'state': 'suspended' if self.suspended else 'ok',
                    'last_heartbeat_datetime': time.time(),
                    'last_received_message_datetime': self.state._last_received_message_datetime,
                    'last_sent_message_datetime': self.state._last_sent_message_datetime}
                self.hive_heartbeats_channel.basic_publish(exchange='', routing_key='heartbeats',
                    body=json.dumps(heartbeat_message))
            except (AMQPError, AMQPConnectorException):
                print('Global RabbitMQ connection closed. Unable to send heartbeat. Reconnecting in 10 seconds')
                while True:
                    try:
                        time.sleep(10)
                        self.__connect_to_heartbeats_queue()
                        break
                    except (AMQPError, AMQPConnectorException, gaierror):
                        print('Unable to reconnect to heartbeats queue. Next attempt in 10 seconds')
            except Exception:
                print(traceback.format_exc())
                time.sleep(0.1)
            # print('heartbeat sent')
            time.sleep(5)

    def _report_error(self, error_message, cause='Unknown'):
        try:
            print('ERROR', error_message)
            body = {'pipeline': self.flow_name, 'service': self.service_name, 'text': error_message, 'cause': cause,
                'timestamp': str(datetime.now())}
            self.hive_errors_channel.basic_publish(exchange='', routing_key='errors', body=json.dumps(body))
        except (AMQPError, AMQPConnectorException):
            print(f'Global RabbitMQ connection closed. Error message can\'t be reported: {error_message}. ' +
                  'Reconnecting in 10 seconds.')
            while True:
                try:
                    time.sleep(10)
                    self.__connect_to_errors_queue()
                    break
                except (AMQPError, AMQPConnectorException, gaierror):
                    print('Unable to reconnect to errors queue. Next attempt in 10 seconds')
        except Exception:
            print(traceback.format_exc())
            time.sleep(0.1)

    @staticmethod
    def format_exception(exc):
        return ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))


class ServiceUtils:
    @staticmethod
    def start_service(service: BaseService, name: str, config_files=('rmq_connection_details', 'common')):
        if name == "__main__":
            try:
                config = Config(os.environ['CONF_PATH'], config_files)
                service.set_config(config)
                service.establish_rmq_connections()
                service.load_state()
                service._run()
            except:
                print('Error occurred during startup:', traceback.format_exc())
