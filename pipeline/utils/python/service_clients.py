import os
import traceback
from time import sleep, time

import requests

from config import Config


class ClientBase:
    def __init__(self, service_addr, auth):
        self.service_addr = service_addr
        self.auth = auth

    def _get(self, endpoint, binary=False, **args):
        url = f'{self.service_addr}/{endpoint}'
        res = requests.get(url, params=args, auth=self.auth)
        if res.status_code != 200:
            return res.text, False

        result = res.content if binary else res.text
        return result, True

    def _post(self, endpoint, **args):
        # data = json.dumps(content)
        url = f'{self.service_addr}/{endpoint}'
        res = requests.post(url, params=args, auth=self.auth)
        if res.status_code != 200:
            return res.text, False

        return res.content, True


class MappingServiceClient(ClientBase):
    retry_count = 2
    with_debug_logs = True

    def get_player_by_link(self, link: str):
        response, success = self._get('get_player', link=link)
        if success:
            return int(response)
        print(f'Player by link {link} mapping error: ', response)
        return None

    def get_team_by_link(self, link: str):
        response, success = self._get('get_team', link=link)
        if success:
            return int(response)
        print(f'Team by link {link} mapping error: ', response)
        return None

    def get_team_by_name(self, team_name: str, source: str, league: str = None, check_level: str = None):
        for _ in range(self.retry_count):
            try:
                if self.with_debug_logs:
                    print(f'Trying to map team {team_name} from {source}')
                    start = time()

                response, success = self._get('get_team', name=team_name, source=source, league=league, check_level=check_level)
                if self.with_debug_logs:
                    finish = time() - start
                    print(f'Mapping of team {team_name} finished in {finish}s with success={success}')

                if success:
                    return int(response)
                print(f'Team by name "{team_name}" mapping error: ', response)
                return None
            except:
                if self.with_debug_logs:
                    finish = time() - start
                    print(f'Error occurred in mapping team {team_name}. Response was given in {finish}s')
                traceback.print_exc()
                print(f'Retrying mapping service call with team {team_name} after 1s cd...')
                sleep(1)

        return None

    def get_match(self, team_1, team_2, dttm, score_1=None, score_2=None):
        for _ in range(self.retry_count):
            try:
                if self.with_debug_logs:
                    print(f'Trying to map match with teams {team_1},{team_2}')
                    start = time()

                response, success = self._get('get_match', team_1=team_1, team_2=team_2, dttm=dttm,
                                              score_1=score_1, score_2=score_2)
                if self.with_debug_logs:
                    finish = time() - start
                    print(f'Mapping of match {team_1},{team_2} finished in {finish}s with success={success}, response = {response}')

                return response if success else None
            except:
                if self.with_debug_logs:
                    finish = time() - start
                    print(f'Error occurred in mapping match with teams {team_1},{team_2}. Response was given in {finish}s')
                traceback.print_exc()
                print('Retrying service call after 1s cd...')
                sleep(1)

        return None

    def get_team_link_id(self, link: str):
        response, success = self._get('get_team_link_id', link=link)
        if success:
            return int(response)
        print(f'Team_link_id by link {link} mapping error: ', response)
        return None

    def get_player_link_id(self, link: str):
        response, success = self._get('get_player_link_id', link=link)
        if success:
            return int(response)
        print(f'Player_link_id by link {link} mapping error: ', response)
        return None


class TeacherServiceClient(ClientBase):
    def game_updated(self, game_id: int):
        response, success = self._get('game_updated', game_id=game_id)
        if not success:
            print('Error in teacher game_updated call: ' + response)

    def new_game_added(self, game_id: int, game_dttm: str):
        response, success = self._get('new_game_added', game_id=game_id, game_dttm=game_dttm)
        if not success:
            print('Error in teacher new_game_added call: ' + response)

    def match_updated(self, match_id: int):
        response, success = self._get('match_updated', match_id=match_id)
        if not success:
            print('Error in teacher match_updated call: ' + response)

    def qq_game_updated(self, game_qq_id: int):
        response, success = self._get('qq_game_updated', game_qq_id=game_qq_id)
        if not success:
            print('Error in teacher qq_game_updated call: ' + response)

    def new_timeline_added(self, game_x_stat_id: int):
        response, success = self._get('timeline_added', game_x_stat_id=game_x_stat_id)
        if not success:
            print('Error in teacher new_timeline_added call: ' + response)


class DbCalcServiceClient(ClientBase):
    def recalculate_rosters(self):
        response, success = self._post('trigger_task', name='t_calc_rosters')
        if not success:
            print('Error in DbCalcServiceClient.recalculate_rosters call: ' + response)


def get_teacher_client() -> TeacherServiceClient:
    config = Config(os.environ['CONF_PATH'], config_files=('rmq_connection_details', 'common'))
    auth = (
        config.get_property('teacher', 'user'),
        config.get_property('teacher', 'password'))
    service_url = config.get_property('teacher', 'url')
    return TeacherServiceClient(service_url, auth)


def get_db_calc_client() -> DbCalcServiceClient:
    config = Config(os.environ['CONF_PATH'], config_files=('rmq_connection_details', 'common'))
    auth = (
        config.get_property('dbcalc', 'user'),
        config.get_property('dbcalc', 'password'))
    service_url = config.get_property('dbcalc', 'url')
    return DbCalcServiceClient(service_url, auth)
