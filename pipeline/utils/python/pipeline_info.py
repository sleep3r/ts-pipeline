import json
from typing import Dict

from service_info import ServiceInfo


class PipelineInfo:
    def __init__(self, name: str, services: Dict[str, ServiceInfo], rabbitmq_url: str, jenkins_url):
        self.name = name
        self.services = services
        self.rabbitmq_url = rabbitmq_url
        self.jenkins_url = jenkins_url

    def to_dict(self):
        return {'name': self.name, 'rabbitmq_url': self.rabbitmq_url, 'jenkins_url': self.jenkins_url,
            'services': {name: info.to_dict() for name, info in self.services.items()}}
