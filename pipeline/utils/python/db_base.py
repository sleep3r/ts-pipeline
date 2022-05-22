import sys
import time
from io import StringIO
from os import environ
from typing import Iterable, Union, List

import psycopg2
import psycopg2.pool
from pandas import DataFrame

from config import Config


class PostgresDbConnection:
    def __init__(self, conn_str: str = None, config: Config = None, dbname: str = None, user: str = None,
            password: str = None, host: str = None, port: Union[str, int] = None, rc_times=5, with_pool=True,
            db_property_group: str = 'db', service_name=None):
        if conn_str is not None:
            self.connection_string = conn_str
        elif config is not None:
            db_conf = config.get_property_group(db_property_group)
            self.connection_string = self.get_conn_str(db_conf.get('dbname'), db_conf.get('user'),
                db_conf.get('password'), db_conf.get('host'), db_conf.get('port'))
        elif all([value is not None for value in [dbname, user, password, host, port]]):
            self.connection_string = self.get_conn_str(dbname, user, password, host, port)
        else:
            raise ValueError('Cannot connect to database - none of conn string, config or (dbname, user, password, '
                             'host, port) provided')

        if not service_name and 'FLOW_NAME' in environ:
            service_name = f"{environ.get('FLOW_NAME', 'undefined')}_{environ.get('SERVICE_NAME', 'undefined')}"
        elif not service_name:
            print('WARNING: Service_name param in PostgresDbConnection init is undefined')
            try:
                service_name = f"dl_{sys.modules['__main__'].__file__}]"[-64:]
            except:
                service_name = f'Undefined_[data_load]'

        self.connection_string += f' application_name={service_name}'
        self.rc_times = rc_times
        self.with_pool = with_pool
        self.conn_pool = psycopg2.pool.ThreadedConnectionPool(1, 3, self.connection_string)

    def __reload_connection(self):
        if self.with_pool:
            self.conn_pool = psycopg2.pool.ThreadedConnectionPool(1, 3, self.connection_string)

    def _get_connection(self):
        if not self.with_pool:
            return psycopg2.connect(self.connection_string)
        for _ in range(5):
            try:
                conn = self.conn_pool.getconn()
                return conn
            except psycopg2.pool.PoolError:
                print('Connection pool is exhausted, trying to reconnect')
                time.sleep(1)
                self.__reload_connection()

    def _put_connection(self, conn):
        if self.with_pool and self.conn_pool:
            self.conn_pool.putconn(conn)

    def get_list_of_dict_from_query(self, sql: str, args: tuple = None) -> list:
        data, columns = self.execute_and_fetch(sql, args, with_columns=True, with_commit=False)
        result = []
        for row in data:
            row_result = {}
            for i in range(len(row)):
                row_result[columns[i]] = row[i]
            result.append(row_result)
        return result

    def get_df_from_query(self, sql: str, args: tuple = None) -> DataFrame:
        data, columns = self.execute_and_fetch(sql, args, with_columns=True, with_commit=False)
        return DataFrame(data=data, columns=columns)

    def get_from_query(self, sql: str, args: tuple = None, with_commit=False) -> []:
        data = self.execute_and_fetch(sql, args, with_columns=False, with_commit=with_commit)
        if len(data) > 0 and len(data[0]) == 1:
            return [*map(lambda x: x[0], data)]

        return data

    def execute_and_fetch(self, sql: str, args: tuple, with_columns, with_commit):
        for _ in range(self.rc_times):
            try:
                conn = self._get_connection()
                curs = conn.cursor()
                if args:
                    curs.execute(sql, args)
                else:
                    curs.execute(sql)
                # print(curs.query)
                result = curs.fetchall()
                if with_commit:
                    conn.commit()
                columns = [desc[0] for desc in curs.description] if with_columns else None
                curs.close()
                self._put_connection(conn)
                if with_columns:
                    return result, columns
                return result

            except psycopg2.OperationalError as ex:
                print('Connection lost, trying to reconnect..')
                time.sleep(1)
                self.__reload_connection()

        raise ReconnectError(f'Reconnect in {sql} failed {self.rc_times} times')

    def execute_many(self, sql_list: List[str], args_list: List[tuple]):
        if len(sql_list) != len(args_list):
            raise ValueError('len of lists must be equal')

        for _ in range(self.rc_times):
            try:
                conn = self._get_connection()
                curs = conn.cursor()
                result = []
                for sql, args in zip(sql_list, args_list):
                    curs.execute(sql, args)
                    result.append(curs.fetchall() if curs.description else None)
                conn.commit()
                curs.close()
                self._put_connection(conn)
                return result

            except psycopg2.OperationalError as ex:
                print('Connection lost, trying to reconnect..')
                time.sleep(1)
                self.__reload_connection()

        raise ReconnectError(f'Reconnect in {sql_list} failed {self.rc_times} times')

    def execute(self, sql: str, args: tuple):
        for _ in range(self.rc_times):
            try:
                conn = self._get_connection()
                curs = conn.cursor()
                if args:
                    curs.execute(sql, args)
                else:
                    curs.execute(sql)
                conn.commit()
                curs.close()
                self._put_connection(conn)
                return

            except psycopg2.OperationalError:
                print('Connection lost, trying to reconnect..')
                time.sleep(1)
                self.__reload_connection()

        raise ReconnectError(f'Reconnect in {sql} failed {self.rc_times} times')

    def insert_batch(self, table_name: str, args_template: str, args_list: Iterable[tuple]):
        for _ in range(self.rc_times):
            try:
                conn = self._get_connection()
                curs = conn.cursor()
                args_str = ','.join(curs.mogrify(f"({args_template})", args).decode("utf-8") for args in args_list)
                curs.execute(f"INSERT INTO {table_name} VALUES " + args_str)
                conn.commit()
                curs.close()
                self._put_connection(conn)
                return

            except psycopg2.OperationalError:
                print('Connection lost, trying to reconnect..')
                time.sleep(1)
                self.__reload_connection()

        raise ReconnectError(f'Reconnect in insert_batch to {table_name} failed {self.rc_times} times')

    def insert_data_from_df(self, table_name, data: DataFrame):
        sio = StringIO()
        sio.write(data.to_csv(index=None, header=None, sep=';', escapechar='\\', doublequote=False))
        sio.seek(0)

        for _ in range(self.rc_times):
            try:
                conn = self._get_connection()
                with conn.cursor() as c:
                    c.copy_from(sio, table_name, columns=data.columns, sep=';', null="")
                    conn.commit()
                return

            except psycopg2.OperationalError:
                self.__reload_connection()
            except psycopg2.pool.PoolError:
                self.__reload_connection()

        raise ReconnectError(f'Reconnect in insert DF to {table_name} failed {self.rc_times} times')

    def close(self):
        try:
            if self.with_pool and self.conn_pool:
                self.conn_pool.closeall()
        except Exception as ex:
            print(f'Exception while closing connection: {ex}')

    @staticmethod
    def get_conn_str(dbname, user, password, host, port):
        return f'dbname={dbname} user={user} password={password} host={host} port={port}'


class ReconnectError(Exception):
    pass
