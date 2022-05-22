from initiator_service import *


initiator = InitiatorService('init_queue', default_delay=60, log_string='Initiated')
ServiceUtils.start_service(initiator, __name__)
