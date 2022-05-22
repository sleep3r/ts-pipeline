from base_service import *
from processor_service import ProcessorService


class FinalizerService(ProcessorService):
    def __init__(self, input: Union[RmqInputInfo, str], subscription_tags: Union[str, List[str]] = None):
        super().__init__(input=input, subscription_tags=subscription_tags)
