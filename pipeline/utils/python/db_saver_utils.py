import json
from typing import List
from cast_utils import *
from db_base import *

'''
every outer function take named params and return sql and args for Postgres execute
=======================================
named params for every functions:
table_name - string, in which the table name is in {schema}.{table_name} format
full_dict - dict, where keys are the names of fields in the database
key_slice - list of strings, where strings are names of "key" or "static" fields in db. 
1) get_insert_on_conflict_sql_and_args, key_slice could be in constraint
returning - the string you will return
constraint_name - the name of constraint in get_insert_on_conflict_sql_and_args
do_nothing - bool for do_nothing case in get_insert_on_conflict_sql_and_args
=======================================
'''


def _prepare_dict_to_json(dictionary: dict):
    typing_result = {}
    for word, item in dictionary.items():
        if type(item) == dict:
            dictionary[word] = json.dumps(item)
        elif type(item) == list:
            new_list = []
            for element in item:
                if type(element) == dict:
                    new_list.append(json.dumps(element))
                    typing_result[word] = 'json[]'
                else:
                    new_list.append(element)
            dictionary[word] = new_list
    return typing_result


def _get_requirement_sql_and_args(full_dict, key_slice, typing_dict: dict):
    result_args = []
    sql_requirements = []
    for key in key_slice:
        element_type = f"::{typing_dict[key]}" if typing_dict and key in typing_dict else ''
        sql_requirements.append(f"{key} = %s{element_type}")
        result_args.append(full_dict[key])
    result_sql = f"WHERE {' AND '.join(sql_requirements)}"

    return result_sql, result_args


def _get_check_sql_and_args(table_name, full_dict, key_slice, dict_of_types):
    requirement_sql, requirement_args = _get_requirement_sql_and_args(full_dict, key_slice, dict_of_types)
    sql_result = f"SELECT * FROM {table_name} {requirement_sql}"
    arg_result = requirement_args

    return sql_result, tuple(arg_result)


def get_check_sql_and_args(table_name, full_dict, key_slice):
    typing_ = _prepare_dict_to_json(full_dict)

    _get_check_sql_and_args(table_name, full_dict, key_slice, typing_)


def _get_insert_sql_and_args(table_name: str, full_dict: dict, dict_of_types: dict, returning: str = None):
    # add columns name and values
    arg_result = []
    value_sql_parts = []
    column_sql_parts = []
    for key in full_dict:
        column_sql_parts.append(f"{key}")
        element_type = f"::{dict_of_types[key]}" if key in dict_of_types else ''
        value_sql_parts.append(f"%s{element_type}")
        arg_result.append(full_dict[key])
    sql_result = f"INSERT INTO {table_name}({', '.join(column_sql_parts)}) "
    sql_result += f"VALUES({', '.join(value_sql_parts)}) "

    # add returning
    if returning:
        sql_result += f'RETURNING {returning} '

    return sql_result, tuple(arg_result)


def get_insert_sql_and_args(table_name: str, full_dict: dict, returning: str = None):
    """
    Function generate an insert sql sting and args in tuple for PostgresConnection
    :param table_name: name of the table for inserting the data
    :param full_dict: dict containing all the data to be uploaded to the db
    :param returning: string with the name of the returned field
    :return: sql sting and args in tuple
    """
    # prepare dict
    typing_ = _prepare_dict_to_json(full_dict)

    return _get_insert_sql_and_args(table_name, full_dict, typing_, returning)


def insert_on_new_data(table_name: str, full_dict: dict, entity_ids: List, excluded_keys: List, dict_of_types=None) -> tuple:
    """
    Function for inserting new data for existing entities (in case we need a historical data)
    If entity with a specific id is already present in the db, data be inserted in case new row is the same entity but with different data in other fields.
    If data contains new entity which is absent in the db it will be also inserted.
    It is also possible to exclude some fields from comparison
    :param table_name: name of the table for inserting the data
    :param full_dict: dict containing all the data to be uploaded to the db
    :param entity_ids: list of fields which identify the entity to search for in the database
    :param excluded_keys: keys excluded from comparison
    :param dict_of_types: dict of specific types if needed
    :return: returns two strings containing sql query and data for it
    """

    def _get_update_set_sql_and_args(data_dict, excluded_keys, dict_of_types: dict) -> tuple:
        """
        An overridden function originally invoked for creating conditional parts in sql queries. This custom version allows to create conditional queries with WHERE operator
        :param full_dict: dict containing all the data to be uploaded to the db
        :param excluded_keys: keys to ignore while comparing the data
        :param dict_of_types: dict of specific types if needed
        :return: returns two strings containing sql query and data for it (conditional part)
        """
        key_words = [key for key in data_dict.keys() if key not in excluded_keys]
        result_sql = []
        result_args = []
        for key in key_words:
            element_type = f"::{dict_of_types[key]}" if dict_of_types and key in dict_of_types else ''
            result_sql.append(f'{key} = %s{element_type}')
            result_args.append(data_dict[key])
        result_sql = ' AND '.join(result_sql)
        return result_sql, result_args

    if dict_of_types is None:
        dict_of_types = {}
    sql_query = f"INSERT INTO {table_name} "
    sql_args = []
    typing_ = _prepare_dict_to_json(full_dict)

    columns_sql_parts = []
    values_sql_parts = []
    for key in full_dict:
        columns_sql_parts.append(str(key))
        element_type = f"::{dict_of_types[key]}" if key in dict_of_types else ''
        values_sql_parts.append(f'%s{element_type}')
        sql_args.append(full_dict[key])
    sql_query += f"({', '.join(columns_sql_parts)}) "
    sql_query += f"SELECT {', '.join(values_sql_parts)} "
    condition_sql = f'WHERE NOT EXISTS(SELECT {",".join(entity_ids)} FROM {table_name} WHERE '
    update_set_sql, update_set_args = _get_update_set_sql_and_args(full_dict, excluded_keys, typing_)
    sql_query += condition_sql
    sql_query += update_set_sql + ')'
    sql_args.extend(update_set_args)
    return sql_query, tuple(sql_args)


def _get_update_set_sql_and_args(full_dict, key_slice, typing_dict: dict):
    dynamic_fields = full_dict.keys() - key_slice if len(full_dict.keys() - key_slice) > 0 else full_dict.keys()
    sorted_dynamic_fields = sorted(list(dynamic_fields))

    result_args = []
    sql_parts = []
    for field in sorted_dynamic_fields:
        element_type = f"::{typing_dict[field]}" if typing_dict and field in typing_dict else ''
        sql_parts.append(f'{field} = %s{element_type}')
        result_args.append(full_dict[field])
    result_sql = ', '.join(sql_parts) + ' '
    return result_sql, result_args


def get_update_sql_and_args(table_name, full_dict, key_slice):
    """
    Function generate an update sql sting and args in tuple for PostgresConnection
    :param table_name: name of the table for inserting the data
    :param full_dict: dict containing all the data to be uploaded to the db
    :param key_slice: list of strings, where strings are names of "key" or "static" fields in db.
    :return: sql sting and args in tuple
    """
    # prepare dict
    typing_ = _prepare_dict_to_json(full_dict)

    # first preparations
    sql_result = f'UPDATE {table_name} SET '
    arg_result = []

    # add update set
    update_set_sql, update_set_args = _get_update_set_sql_and_args(full_dict, key_slice, typing_)
    sql_result += update_set_sql
    arg_result.extend(update_set_args)

    # add requirements
    requirement_sql, requirement_args = _get_requirement_sql_and_args(full_dict, key_slice, typing_)
    sql_result += requirement_sql
    arg_result.extend(requirement_args)

    return sql_result, tuple(arg_result)


def _assert_not_none_values_in_key_fields(full_dict, key_slice):
    for keyword in key_slice:
        clean_keyword = __clear_field_name(keyword)
        if full_dict[clean_keyword] is None:
            raise ValueError(f"field {clean_keyword} must not be None")


def __clear_field_name(word):
    return re.findall(r"\w+", word)[0]


def get_insert_on_conflict_sql_and_args(
        table_name, full_dict, key_slice: list, constraint_name=None, returning=None, do_nothing=False):
    """
    Function generate an insert_on_conflict sql sting and args in tuple for PostgresConnection
    :param table_name: name of the table for inserting the data
    :param full_dict: dict containing all the data to be uploaded to the db
    :param key_slice: list of strings, where strings are names of "key" or "static" fields in db.
    :param constraint_name: you can also specify the constraint name
    :param returning: string with the name of the returned field
    :param do_nothing: could be True if you won't make update on conflict
    :return: sql sting and args in tuple
    """
    # check dict
    _assert_not_none_values_in_key_fields(full_dict, key_slice)

    # prepare dict
    typing_ = _prepare_dict_to_json(full_dict)

    # add insert
    result_sql, result_args = _get_insert_sql_and_args(table_name, full_dict, typing_)

    # add conflict_target
    result_sql += 'ON CONFLICT '
    if constraint_name:
        result_sql += f'ON CONSTRAINT {constraint_name} '
    else:
        result_sql += f"({', '.join([str(word) for word in key_slice])}) "

    # add conflict_action
    if do_nothing:
        result_sql += 'DO UPDATE SET '
        update_set_sql, update_set_args = _get_update_set_sql_and_args(full_dict, full_dict.keys() - key_slice, typing_)
        result_args = list(result_args)
        result_sql += update_set_sql
        result_args.extend(update_set_args)

    else:
        # this for insert or update
        result_sql += 'DO UPDATE SET '
        update_set_sql, update_set_args = _get_update_set_sql_and_args(full_dict, key_slice, typing_)
        result_args = list(result_args)
        result_sql += update_set_sql
        result_args.extend(update_set_args)

    # add returning value
    if returning:
        result_sql += f'RETURNING {returning} '

    return result_sql, tuple(result_args)
