from base_service import *


class InitiatorService(BaseService):
    def _handle_message(self, message, headers):
        pass

    def __init__(self, outputs: Union[RmqOutputInfo, List[RmqOutputInfo], str], default_delay: float,
            default_message: str = 'ping', log_string='', initiate_on_start=True):
        self.default_delay = default_delay
        self.default_message = default_message
        super().__init__(outputs=outputs)
        self.log_string = log_string
        self.initiate_on_start = initiate_on_start

    def _init_state(self):
        self.state.message = self.default_message
        self.state.delay = self.default_delay

    def _suspend(self, args):
        if self.suspended:
            return
        super()._suspend(args)
        self.exit.set()

    def _run(self):
        super()._run()
        should_initiate = self.initiate_on_start
        while True:
            try:
                # TODO: add scheduler
                while self.suspended:
                    if self.terminated:
                        print('Shutting down')
                        return
                    time.sleep(1)
                with self.state.write_lock:
                    if should_initiate:
                        self._before_send()
                        self._send(self.state.message)
                        self._after_send()
                        self.state.dump()
                    else:
                        should_initiate = True
                self.exit = threading.Event()
                if self.state.delay <= 0:
                    self.exit.wait()
                else:
                    self.exit.wait(timeout=self.state.delay)
            except (AMQPError, AMQPConnectorException):
                print('RabbitMQ connection closed. Reconnecting in 10 seconds')
                time.sleep(10)
                self._connect_to_output_queues()
            except Exception:
                pass
                self._report_error(traceback.format_exc())

    def _before_send(self):
        pass

    def _after_send(self):
        if self.log_string != '':
            print(self.log_string)

    def _handle_set_delay_command(self, args):
        print('processing set_delay command')
        if len(args) == 0:
            print('Error: delay must not be empty')
            return
        try:
            new_delay = float(args[0])
            self.state.delay = new_delay
            if new_delay != 0:
                self.exit.set()
        except ValueError:
            print('Error: delay must be float')

    def _handle_set_message_command(self, args):
        print('processing set_message command')
        if len(args) == 0:
            print('Error: message must not be empty')
            return
        self.state.message = args[0]

    def _handle_trigger_command(self, args):
        self._before_send()
        self._send(self.state.message if len(args) == 0 else args[0])
        self._after_send()

    def _handle_command(self, command, args):
        super()._handle_command(command, args)

        if command == 'set_delay':
            self._handle_set_delay_command(args)

        if command == 'set_message':
            self._handle_set_message_command(args)

        if command == 'trigger':
            self._handle_trigger_command(args)

        self.state.dump()
