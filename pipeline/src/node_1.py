import os
import json

from nn_node import NNNode
from base_service import ServiceUtils, RmqInputInfo, RmqOutputInfo


class NN1(NNNode):
    def _handle_message(self, message: str, headers: str):
        pass


nn1 = NN1(input=RmqInputInfo('init_queue'), outputs=[])
ServiceUtils.start_service(nn1, __name__)
