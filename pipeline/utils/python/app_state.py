import json
import os
from threading import RLock
from typing import Dict

from pipeline_info import PipelineInfo
from service_info import ServiceInfo
from service_state import ServiceState


class AppState:
    def __init__(self):
        self.pipelines_info = self._read_pipelines()
        self.pipelines_state = self._init_pipelines_state(self.pipelines_info)
        self.lock = RLock()

    @staticmethod
    def _parse_pipelines(pipelines_info: dict) -> Dict[str, PipelineInfo]:
        result = {}
        for pipeline_name, pipeline_info in pipelines_info.items():
            services = {}
            for service_name, service_info in pipeline_info['services'].items():
                services[service_name] = ServiceInfo(service_name, service_info['predecessors'],
                    service_info['supported_commands'], service_info['input_queue'])
            result[pipeline_name] = PipelineInfo(pipeline_name, services, pipeline_info['rabbitmq_url'],
                pipeline_info['jenkins_url'])
        return result

    def _read_pipelines(self) -> Dict[str, PipelineInfo]:
        conf_path = os.environ['CONF_PATH']
        if conf_path is None:
            raise Exception('CONF_PATH variable must be set')
        with open(conf_path + '/pipelines.json') as pipelines_file:
            return self._parse_pipelines(json.load(pipelines_file))

    def _init_pipelines_state(self, pipelines_configuration: Dict[str, PipelineInfo]) -> Dict[str, Dict]:
        return {pipeline.name: {service.name: ServiceState(None, None, None, False)
            for service in pipeline.services.values()}
            for pipeline in pipelines_configuration.values()}


app_state = AppState()
