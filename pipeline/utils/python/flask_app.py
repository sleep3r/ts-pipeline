import copy
import json
from datetime import datetime

from flask import Flask, jsonify, request, render_template

from app_state import app_state
from errors_manager import errors_manager
from rmq_controller import rmq_controller


class FlaskApp:
    def __init__(self):
        self.app = Flask(__name__, template_folder='../templates', static_folder='../static')

        @self.app.route('/')
        @self.app.route('/service_monitor')
        def service_monitor():
            return render_template('service_monitor.html')

        @self.app.route('/errors')
        def errors():
            args = request.args
            type = 'html' if args.get('type') is None else 'json'
            errors = errors_manager.get_errors_report()[::-1]
            if type == 'json':
                return jsonify(errors)
            return render_template('errors.html', errors=errors)

        @self.app.route('/send_command', methods=['POST'])
        def send_command():
            args = json.loads(request.data)
            target = args.get('target')
            command = args.get('command')
            command_arguments = [] if args.get('command_arguments') is '' or args.get('command_arguments') is None \
                else args.get('command_arguments').split('\n')
            if target is None or command is None:
                return 'Parameters "target" and "command" are required', 400
            return 'Success' if rmq_controller.send_command(target, command, command_arguments) else ('Failure', 503)

        @self.app.route('/_pipelines', methods=['GET'])
        def _pipelines():
            with app_state.lock:
                pipelines_info_dict = {name: info.to_dict() for name, info in app_state.pipelines_info.items()}
                pipelines_state_dict = {pipeline_name: {name: state.to_dict() for name, state in pipeline_state.items()}
                    for pipeline_name, pipeline_state in app_state.pipelines_state.items()}
            return {'info': pipelines_info_dict, 'state': pipelines_state_dict}

    def run(self):
        self.app.run(host='0.0.0.0', port=8090)


flask_app = FlaskApp()
