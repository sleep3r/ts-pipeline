DIRECT = 'direct'
FANOUT = 'fanout'
TOPIC = 'topic'
HEADERS = 'headers'


class Blocking:
    @staticmethod
    def queue(queue_name, callback=None, exchange_name=None, routing_key=None, exchange_type=DIRECT, host='localhost',
            port=None, username='guest', password='guest', prefetch_count=0, durable=False):
        from pika.adapters.blocking_connection import BlockingChannel
        connection = Blocking.create_connection(host, port, username, password)

        channel: BlockingChannel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=durable)
        if exchange_name is not None:
            channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)
            key_words = (routing_key or queue_name).split('.')
            for i in range(1, len(key_words) + 1):
                key = '.'.join(key_words[:i])
                channel.queue_bind(queue_name, exchange_name, routing_key=key)
        if callback is not None:
            if prefetch_count > 0:
                channel.basic_qos(prefetch_count=prefetch_count)
            channel.basic_consume(queue=queue_name, on_message_callback=callback)
        return channel

    @staticmethod
    def exchange(name, exchange_type=DIRECT, host='localhost', port=None, username='guest', password='guest',
            durable=False):
        from pika.adapters.blocking_connection import BlockingChannel
        connection = Blocking.create_connection(host, port, username, password)

        channel: BlockingChannel = connection.channel()
        channel.exchange_declare(exchange=name, exchange_type=exchange_type, durable=durable)
        return channel

    @staticmethod
    def create_connection(host, port, username, password):
        import pika
        credentials = pika.PlainCredentials(username, password)
        if port is None:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, heartbeat=0,
                credentials=credentials))
        else:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, heartbeat=0,
                credentials=credentials))
        return connection
