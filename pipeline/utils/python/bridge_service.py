from db_base import *
from processor_service import *


class BridgeService(ProcessorService):
    def __init__(self, input: Union[RmqInputInfo, str], outputs: Union[RmqOutputInfo, List[RmqOutputInfo], str],
                 input_path: str, subscription_tags: Union[str, List[str]] = None):
        super().__init__(input, outputs, subscription_tags=subscription_tags)
        self.input_path = input_path + '/'


class DbBridgeService(ProcessorService):
    def __init__(self, input: Union[RmqInputInfo, str],
                 outputs: Union[RmqOutputInfo, List[str], List[RmqOutputInfo], str],
                 subscription_tags: Union[str, List[str]] = None):
        super().__init__(input, outputs, subscription_tags=subscription_tags)

    def _run(self):
        self._db_service = PostgresDbConnection(config=self.config)
        super()._run()

    def _execute_query(self, sql: str, args: tuple):
        """
        Use it for INSERT/UPDATE/DELETE statements when there is no need for return
        :param sql: SQL query with %s on args places
        :param args: tuple of arguments that would be placed into %s
        """
        self._db_service.execute(sql, args)

    def _execute_and_fetch(self, sql: str, args: tuple, with_columns: bool = False, with_commit: bool = False):
        """
        Use it for SELECT or INSERT with RETURNING
        :param sql: SQL query with %s on args places
        :param args: tuple of arguments that would be placed into %s
        :param with_commit: True if need commit after execute (for INSERT)
        """
        return self._db_service.execute_and_fetch(sql, args, with_columns, with_commit)

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
