import re
from datetime import datetime
from typing import Tuple


def get_copy_of_dict(original_dict, with_same_keys: list = None, with_different_keys: dict = None, without_keys: list = None):
    if with_same_keys and without_keys:
        print('WARNING in this function you should use only one of two methods with_same_keys or without_keys')

    result = {}

    if with_same_keys:
        for key in with_same_keys:
            result[key] = original_dict[key] if key in original_dict else None
    else:
        result.update(original_dict)

    if without_keys:
        for key in without_keys:
            if key in result:
                result.pop(key, None)

    if with_different_keys:
        for key, new_key in with_different_keys.items():
            if key in result:
                result.pop(key, None)
            result[new_key] = original_dict[key]

    return result


def get_datetime_from_specific_string(string: str):
    date = re.findall("\d{4}-\d\d-\d\d", string)
    time = re.findall("\d\d:\d\d:\d\d", string)
    if len(date) == 1 and len(time) == 1:
        return datetime.fromisoformat(f"{date[0]} {time[0]}")


def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return default


def parse_date_range(dates_str: str) -> Tuple[datetime, datetime]:
    dates_str_splitted = dates_str.split(',')
    if len(dates_str_splitted) > 2:
        long_range = dates_str.split('-')
        return parse_date_range(long_range[0])[0], parse_date_range(long_range[1])[1]

    dates, year = dates_str_splitted
    date_range_splitted = dates.split('-')
    parse = lambda x: datetime.strptime(f'{x.lstrip(" ")} {int(year)}', '%b %d %Y')

    start_dttm = parse(date_range_splitted[0])
    if len(date_range_splitted) == 1:
        return start_dttm, start_dttm

    finish_str = date_range_splitted[1]
    finish_dttm = parse(finish_str) if len(finish_str) > 3 \
        else datetime(start_dttm.year, start_dttm.month, int(finish_str))

    return start_dttm, finish_dttm