import os
import pickle

from finalizer_service import *


class SavingMode:
    APPEND_SKIP_ON_REPEAT = 0
    APPEND = 1
    REWRITE = 2


class FileSaverService(FinalizerService):
    def __init__(self, input: Union[RmqInputInfo, str], path_format, file_format='',
            saving_mode=SavingMode.APPEND_SKIP_ON_REPEAT, create_paths=(),
            subscription_tags: Union[str, List[str]] = None):
        super().__init__(input, subscription_tags=subscription_tags)
        self.path_format = path_format
        self.file_format = file_format
        self.saving_mode = saving_mode
        self.paths_to_create = create_paths

    def serialize(self, target_object):
        if self.file_format.lower() == 'json':
            return json.dumps(target_object)
        elif self.file_format.lower() == 'pickle':
            return pickle.dumps(target_object)
        else:
            return str(target_object)

    def save(self, path_params=(), file_content=''):
        should_skip = False
        serialized_file_content = self.serialize(file_content)
        if self.saving_mode == SavingMode.APPEND_SKIP_ON_REPEAT:
            object_id = '_'.join(path_params)
            last_state_filename = f'{self.last_state_folder_path}/{object_id}.json'
            if os.path.exists(last_state_filename):
                with open(last_state_filename, encoding='utf8') as last_state_file:
                    should_skip = last_state_file.read() == serialized_file_content
            with open(last_state_filename, 'w', encoding='utf8') as last_state_file:
                last_state_file.write(serialized_file_content)

        if should_skip:
            return
        mode = 'w' if self.saving_mode == SavingMode.REWRITE else 'a'
        with open(self.path_format.format(*path_params), mode, encoding='utf8') as output_file:
            output_file.write(serialized_file_content)
            output_file.write('\n') #TODO: get rid of writing '\n' in text mode as it converts to a platform-specific representation

    def _run(self):
        self.last_state_folder_path = self.state_path + '/last'
        os.makedirs(self.last_state_folder_path, exist_ok=True)
        for path in self.paths_to_create:
            os.makedirs(path, exist_ok=True)
        super()._run()