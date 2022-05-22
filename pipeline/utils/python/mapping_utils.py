from datetime import datetime

from db_base import PostgresDbConnection


def get_dota_series_id(db_service: PostgresDbConnection, team_1_nm: str, team_2_nm: str, dttm: datetime):
    sql = "select * from dota.find_liq_series(%s, %s, %s)"
    response = db_service.get_list_of_dict_from_query(sql, (team_1_nm, team_2_nm, dttm))
    response.sort(key=lambda x: abs(x['o_series_dttm'] - dttm))
    if len(response) == 0:
        return None

    best_match = response[0]
    if best_match['swap']:
        return best_match['o_series_liq_id'], best_match['team_2_id'], best_match['team_1_id']

    return best_match['o_series_liq_id'], best_match['team_1_id'], best_match['team_2_id']


def get_lol_match_id(db_service: PostgresDbConnection, team_1_nm: str, team_2_nm: str, dttm: datetime, source=None):
    sql = "select * from esport.find_matches(%s, %s, %s, %s)"
    response = db_service.get_list_of_dict_from_query(sql, (team_1_nm, team_2_nm, dttm, source))
    response.sort(key=lambda x: abs(x['o_match_dttm'] - dttm))
    if len(response) == 0:
        return None, None, None

    best_match = response[0]
    if best_match['swap']:
        return best_match['o_match_id'], best_match['o_team_2_id'], best_match['o_team_1_id']

    return best_match['o_match_id'], best_match['o_team_1_id'], best_match['o_team_2_id']


# region SQL QUERIES
SQL_CHECK_LINK_EXISTS = '''
SELECT tl.team_id, tl.team_link_id
FROM {scheme}.team_link as tl
WHERE tl.team_gamepedia_link = %s
'''

# prev_links, team_gamepedia_link, team_nm_cln, region_cln, region_cln, team_nm_cln, region_cln, team_nm_cln
SQL_FIND_TEAM = '''
SELECT DISTINCT (team_id) 
FROM {scheme}.team_link
WHERE ((team_gamepedia_link = any(%s)) or ((team_nm_cln is not null) and
      (((team_gamepedia_link like '{vendor}/index.php?title=%%')
        or (%s like '{vendor}/index.php?title=%%'))
        and (team_nm_cln=%s)) or
      ((region_cln is not null) and (%s is not null) and
       (region_cln=%s) and (team_nm_cln=%s)) or
      ((region_cln is null) and (%s is null) and (team_nm_cln=%s)))) and team_id is not null;
'''

SQL_INSERT_TEAM = '''
INSERT INTO {scheme}.team(team_nm, org_location, region, created_dttm)
VALUES(%s, %s, %s, %s)
RETURNING team_id
'''

SQL_CHECK_PLAYER_LINK_EXISTS = '''
SELECT tl.player_id
FROM {scheme}.player_link as tl
WHERE tl.player_gamepedia_link = %s
'''

# prev_links, player_gamepedia_link, player_nm_cln, real_name_cln, real_name_cln, player_nm_cln, birthday
SQL_FIND_PLAYER = '''
SELECT DISTINCT (player_id)
FROM {scheme}.player_link
WHERE ((player_gamepedia_link = any(%s)) or ((player_nm_cln is not null) and
      ((player_gamepedia_link like '{vendor}/index.php?title=%%')
        or (%s like '{vendor}/index.php?title=%%'))
        and (player_nm_cln=%s)) or
      ((real_name_cln is not null) and (%s is not null) and
       (real_name_cln=%s) and ((player_nm_cln=%s) or (birthday is not null and birthday=%s)))) and player_id is not null;
'''

SQL_INSERT_PLAYER = '''
INSERT INTO {scheme}.player(player_nm, real_name, country, birthday, residency, role)
VALUES(%s, %s, %s, %s, %s, %s)
RETURNING player_id
'''

SQL_INSERT_TEAM_IMAGE = '''
INSERT INTO esport.team_link_image(team_link_id, team_gamepedia_image, last_update_dttm) 
VALUES(%s, %s, now()) ON CONFLICT (team_link_id) DO NOTHING '''
# endregion


class MappingException(Exception):
    pass
