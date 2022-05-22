import json
import os

from readerwriterlock import rwlock


class State:
    def __init__(self, path, dump_interval = 1):
        self.__dict__['state_internal'] = dict()
        self.__dict__['rwlock'] = rwlock.RWLockWrite()
        self.__dict__['path'] = path
        self.__dict__['dump_interval'] = dump_interval
        self.__dict__['dump_counter'] = 0

    def __setattr__(self, key, value):
        self.__dict__['state_internal'][str(key)] = value

    def __getattr__(self, item):
        if item == 'read_lock':
            return self.__dict__['rwlock'].gen_rlock()
        if item == 'write_lock':
            return self.__dict__['rwlock'].gen_wlock()
        return self.__dict__['state_internal'].get(str(item))

    def __str__(self):
        return str(self.__dict__['state_internal'])

    def load(self):
        path = self.__dict__['path']
        if not os.path.exists(path):
            return
        for f in os.listdir(path):
            if f.endswith('json'):
                file_path = path + '/' + f
                key = f.split('.')[0]
                with open(file_path) as input_file:
                    self.__dict__['state_internal'][key] = json.load(input_file)

    def dump(self, keys: set = None, force: bool = True):
        path = self.__dict__['path']

        if not force:
            dump_interval = self.__dict__['dump_interval']
            dump_counter = self.__dict__['dump_counter'] + 1
            if dump_counter < dump_interval:
                self.__dict__['dump_counter'] = dump_counter
                return
            self.__dict__['dump_counter'] = 0

        os.makedirs(self.__dict__['path'], exist_ok=True)
        for k, v in self.__dict__['state_internal'].items():
            if keys is None or k in keys:
                temp_file_path = path + '/tmp_' + str(k)
                file_path = path + '/' + str(k) + '.json'
                with open(temp_file_path, 'w') as output_file:
                    json.dump(v, output_file)
                os.replace(temp_file_path, file_path)
