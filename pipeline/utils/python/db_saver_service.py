from copy import deepcopy

from db_base import *
from finalizer_service import *


class ObjectGeneration:
    def __init__(self, retry_limit, delay):
        super().__init__()
        self.delay = delay
        self.obj_limit = retry_limit
        self.objects = {}
        self.timer_started = False
        self.timer = None
        self.canceled = False

    def update(self, data_id, data):
        if data_id in self.objects:
            self.objects[data_id]['data'] = data
            return

        self.objects[data_id] = {'data': data, 'counter': 0, 'processed': False}

    def set_processed(self, data_id):
        del self.objects[data_id]

    def check_errors(self):
        return not self.canceled and not self.timer_started and len(self.objects) > 0

    def cancel(self):
        if self.timer_started and self.timer is not None:
            self.timer.cancel()
            self.canceled = True
            self.timer = None


class GenerationList:
    def __init__(self, generations: List[ObjectGeneration], db_save_method: classmethod, report_error: classmethod):
        self.report_error = report_error
        self.generations = generations
        self.db_save_method = db_save_method
        self.locker = threading.Lock()

    def update(self, data_id, data):
        for generation in self.generations:
            if data_id in generation.objects:
                generation.update(data_id, data)
                return True

        self.generations[0].update(data_id, data)
        return False

    def set_processed(self, data_id):
        for generation in self.generations:
            if data_id in generation.objects:
                generation.set_processed(data_id)
                return

    def check_errors(self):
        with self.locker:
            for generation_num in range(len(self.generations)):
                generation = self.generations[generation_num]
                if generation.check_errors():
                    generation.timer_started = True
                    generation.timer = threading.Timer(generation.delay, self.process_errors, args=(generation_num,))
                    generation.timer.start()

    def process_errors(self, generation_num):
        generation = self.generations[generation_num]
        for data_id, data in reversed(list(generation.objects.items())):
            status, exc = self.db_save_method(data_id, data['data'])
            if status:
                data['processed'] = True
            else:
                data['counter'] += 1
                if data['counter'] >= generation.obj_limit and generation_num < len(self.generations) - 1:
                    error_msg = f'Error occurred {data["counter"]} times in saving {data_id}:\n' \
                                f'{BaseService.format_exception(exc)}'
                    self.report_error(error_msg, cause=json.dumps(data['data']))
                    data['processed'] = None

        for data_id in list(generation.objects.keys()):
            if generation.objects[data_id]['processed']:
                generation.set_processed(data_id)
            elif generation.objects[data_id]['processed'] is None:
                self.generations[generation_num + 1].update(data_id, generation.objects[data_id]['data'])
                generation.set_processed(data_id)

        generation.timer_started = False
        generation.timer = None
        self.check_errors()

    def stop(self):
        for generation in self.generations:
            generation.cancel()


class DbSaverService(FinalizerService):
    def __init__(self, *args, db_property_group='db', close_after_save=False, **kwargs):
        self.db_property_group = db_property_group
        self.close_after_query = close_after_save
        super().__init__(*args, **kwargs)

    def _run(self):
        self._db_service = PostgresDbConnection(config=self.config, db_property_group=self.db_property_group)
        self.generation_list = GenerationList([
            ObjectGeneration(retry_limit=10, delay=30 * 60),
            ObjectGeneration(retry_limit=10, delay=3 * 60 * 60),
            ObjectGeneration(retry_limit=0, delay=24 * 60 * 60)
        ],
            self.__save_to_db_locked,
            self._report_error)

        self._locker = threading.Lock()
        self._after_run()
        super()._run()

    def _after_run(self):
        pass

    def _init_state(self):
        self.state.unprocessed_data = {}

    def _handle_message(self, message, headers):
        if 'tag' in headers:
            print(f'Handling tagged message with headers {headers}')
            self._handle_tagged_message(message, headers)
            return

        data = json.loads(message)
        data_id = self._get_id(data)
        print(f'Received data with ID {data_id}')
        exist = self.generation_list.update(data_id, data)
        if exist:
            print(f'Message with data_id={data_id} updated in errors_list')
            return

        status, exc = self.__save_to_db_locked(data_id, data)
        if status:
            self.generation_list.set_processed(data_id)
        else:
            self.generation_list.check_errors()

    def _handle_tagged_message(self, message, headers):
        pass

    def __save_to_db_locked(self, data_id, data):
        with self._locker:
            try:
                data_1 = deepcopy(data)
                self._save_to_db(data_1)
                print(f'Data with ID={data_id} has been saved to DB')
                return True, None
            except Exception as ex:
                print({f'==== ERROR in saving {data_id} ===='})
                print(BaseService.format_exception(ex))
                if hasattr(ex, 'pgerror'):
                    print(ex.pgerror)
                return False, ex
            finally:
                if self.close_after_query:
                    self._db_service.close()

    def insert_or_update_data(self, check_sql, insert_sql, update_sql, check_args, insert_args, update_args,
            update_status=None):
        exists = self._execute_and_fetch(check_sql, check_args)
        if len(exists) > 0:
            if update_sql:
                self._execute_query(update_sql, update_args)
            result = exists[0][0]
            if update_status:
                update_status[0] = True
        else:
            result = self._execute_and_fetch(insert_sql, insert_args, with_commit=True)[0][0]
        return result

    def _hard_shutdown(self, args):
        self.generation_list.stop()
        super()._hard_shutdown(args)

    def _save_to_db(self, data):
        raise NotImplementedError()

    def _get_id(self, data) -> str:
        raise NotImplementedError()

    def _execute_query(self, sql: str, args: tuple):
        """
        Use it for INSERT/UPDATE/DELETE statements when there is no need for return
        :param sql: SQL query with %s on args places
        :param args: tuple of arguments that would be placed into %s
        """
        self._db_service.execute(sql, args)

    def _execute_and_fetch(self, sql: str, args: tuple = None, with_columns: bool = False, with_commit: bool = False):
        """
        Use it for SELECT or INSERT with RETURNING
        :param sql: SQL query with %s on args places
        :param args: tuple of arguments that would be placed into %s
        :param with_commit: True if need commit after execute (for INSERT)
        """
        if not args:
            args = tuple()
        return self._db_service.execute_and_fetch(sql, args, with_columns, with_commit)

    def _execute_many(self, sql_list, args_list):
        return self._db_service.execute_many(sql_list, args_list)

    def _insert_from_df(self, table_name: str, df: DataFrame):
        """
        Insert data from pandas DataFrame
        :param table_name: [schema_name].[table_name]
        :param df: Columns names must be same as in DB
        """
        self._db_service.insert_data_from_df(table_name, df)

    def _get_df_from_query(self, sql, args=None):
        if not args:
            args = tuple()
        return self._db_service.get_df_from_query(sql, args)

    def _get_list_of_dict_from_query(self, sql, args=None):
        if not args:
            args = tuple()
        return self._db_service.get_list_of_dict_from_query(sql, args)
