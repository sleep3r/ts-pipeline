import copy
from typing import Union


class DataCollector:
    def __init__(self):
        self.variables_dict = {}
        self.topologies = []
        self.json_counter = 0

    @staticmethod
    def __get_topology(input_dict_data):
        def add_topology_slice(result_dict, input_dict):
            for keyword, value in input_dict.items():
                if type(value) == dict:
                    result_dict[keyword] = dict()
                    add_topology_slice(result_dict[keyword], input_dict[keyword])
                else:
                    result_dict[keyword] = set()

        result = dict()
        add_topology_slice(result, input_dict_data)

        return result

    @staticmethod
    def listless_dict(data: dict):
        def make_dict_from_list(input_list: list):
            dict_result = {}
            i = 0
            for item in input_list:
                dict_result[i] = item
                i += 1
            return dict_result

        def remake_dict(input_dict: dict):
            for keyword, value in input_dict.items():
                if type(value) == dict:
                    remake_dict(value)
                if type(value) == list:
                    input_dict[keyword] = make_dict_from_list(value)
                    remake_dict(input_dict[keyword])

        remake_dict(data)

    def __get_topology_id(self, input_dict: dict):
        self.listless_dict(input_dict)
        input_topology = self.__get_topology(input_dict)
        i = 0
        for topology in self.topologies:
            if input_topology == topology:
                return i
            i += 1
        self.topologies.append(input_topology)

        self.variables_dict[i] = copy.deepcopy(input_topology)
        return len(self.topologies) - 1

    def update_variables(self, data: dict):
        topology_id = self.__get_topology_id(data)
        self.json_counter += 1

        def update_slice(result_dict, input_dict):
            for keyword, value in input_dict.items():
                if type(value) == dict:
                    update_slice(result_dict[keyword], input_dict[keyword])
                elif type(value) == list:
                    result_dict[keyword].add('some_list')
                else:
                    result_dict[keyword].add(value)

        update_slice(self.variables_dict[topology_id], data)


def full_get(dictionary: dict, keywords: tuple, default=None):
    result = dictionary
    for keyword in keywords:
        if result != default:
            result = result.get(keyword, default)
    return result


def json_dump_(obj, file_name):
    import json
    fixed_file_name = file_name.split('.')[0] + '.json'
    with open(fixed_file_name, 'w') as f:
        json.dump(obj, f)


def json_load_(file_name):
    import json
    fixed_file_name = file_name.split('.')[0] + '.json'
    with open(fixed_file_name, 'r') as f:
        return json.load(f)


def load_or_do_and_pickle_dump(func, args: tuple, dump_path: str):
    import os
    import pickle
    if os.path.exists(dump_path):
        with open(dump_path, 'rb') as f:
            result = pickle.load(f)
    else:
        result = func(*args)
        with open(dump_path, 'wb') as f:
            pickle.dump(result, f)
    return result


def load_or_do_and_json_dump(func, args: tuple, dump_path: str):
    import os
    import json
    if os.path.exists(dump_path):
        with open(dump_path, 'r') as f:
            result = json.load(f)
    else:
        result = func(*args)
        with open(dump_path, 'w') as f:
            json.dump(result, f)
    return result
