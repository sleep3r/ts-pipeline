from typing import List


class ServiceInfo:
    def __init__(
        self, 
        name: str, 
        predecessors: List[str], 
        supported_commands: List[str], 
        input_queue: str
    ):
        self.name = name
        self.predecessors = predecessors
        self.supported_commands = supported_commands
        self.input_queue = input_queue

    def to_dict(self):
        return {
            'name': self.name, 
            'predecessors': self.predecessors, 
            'supported_commands': self.supported_commands,
            'input_queue': self.input_queue
        }
