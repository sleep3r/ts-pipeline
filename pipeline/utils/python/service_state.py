from datetime import datetime


class ServiceState():
    def __init__(self, last_heartbeat_datetime, last_received_message_datetime, last_sent_message_datetime, suspended):
        self.last_heartbeat_datetime = last_heartbeat_datetime
        self.last_received_message_datetime = last_received_message_datetime
        self.last_sent_message_datetime = last_sent_message_datetime
        self.suspended = suspended

    def to_dict(self):
        state = 'down'
        if self.last_heartbeat_datetime is not None \
                and (datetime.now() - datetime.fromtimestamp(self.last_heartbeat_datetime)).seconds < 10:
            state = 'suspended' if self.suspended else 'up'
        return {'pipeline_state': state, 'last_heartbeat_datetime': self.last_heartbeat_datetime,
            'last_received_message_datetime': self.last_received_message_datetime,
            'last_sent_message_datetime': self.last_sent_message_datetime}
