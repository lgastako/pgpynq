import threading
import os
import sys
import re

from ConfigParser import ConfigParser
from collections import namedtuple

from pynq.providers import IPynqProvider
from pynq.enums import Actions

import psycopg2
import psycopg2.pool

DEFAULT_CONFIG_FILENAME = "pgpynq.cfg"
CONFIG_FILE_ENV_VAR = "PGPYNQ_CONFIG_FILE"
THREAD_STORAGE = threading.local()
MINIMUM_CONNECTIONS = 0
MAXIMUM_CONNECTIONS = sys.maxint
SAFE_NAME_EQ_RE = re.compile(r"=")
SAFE_NAME_RE = re.compile(r"([^a-zA-Z0-9])")


def _get_default_connection_string():
    if CONFIG_FILE_ENV_VAR in os.environ:
        path = os.environ[CONFIG_FILE_ENV_VAR]
    else:
        path = os.path.join(os.getcwd(), DEFAULT_CONFIG_FILENAME)
    if not os.path.exists(path):
        raise Exception("Default connection requested but could not find"
                        " config file: {path}.\nEither pass in a connection "
                        "or create a config file.".format(path=path))
    config = ConfigParser()
    config.read(path)
    return config.get("connection", "connection_string")


def get_pool():
    if not hasattr(THREAD_STORAGE, "pool"):
        connection_string = _get_default_connection_string()
        pool = psycopg2.pool.PersistentConnectionPool(MINIMUM_CONNECTIONS,
                                                      MAXIMUM_CONNECTIONS,
                                                      connection_string)
        THREAD_STORAGE.pool = pool
    pool = THREAD_STORAGE.pool
    return pool


class Psycopg2RelationProvider(IPynqProvider):

    @property
    def from_clause(self):
        raise NotImplementedError

    def parse(self, query, action=None, cols=None, column=None):
        if action == Actions.SelectMany:
            return self.parse_select_many(query)
        elif action == Actions.Select:
            return self.parse_select(query, cols)
        elif action == Actions.Count:
            return self.parse_count(query)
        elif action == Actions.Max:
            return self.parse_max(query, column)
        elif action == Actions.Min:
            return self.parse_min(query, column)
        elif action == Actions.Sum:
            return self.parse_sum(query, column)
        elif action == Actions.Avg:
            return self.parse_avg(query, column)
        raise ValueError("Invalid action exception.  %s is unknown." % action)

    def get_single_val(self, rows):
        return rows[0][0]

    def do_single_val(self, sql, query):
        sql = self.append_clauses(sql, query)
        return self.get_single_val(self.execute(sql))

    def do_package(self, sql, query):
        sql = self.append_clauses(sql, query)
        return self.package(self.execute(sql))

    def parse_count(self, query):
        sql = ("SELECT COUNT(*)"
               " FROM %s" % self.from_clause)
        return self.do_single_val(sql, query)

    def parse_max(self, query, column):
        sql = ("SELECT MAX(%s)"
               " FROM %s" % (column, self.from_clause))
        return self.do_single_val(sql, query)

    def parse_min(self, query, column):
        sql = ("SELECT MIN(%s)"
               " FROM %s" % (column, self.from_clause))
        return self.do_single_val(sql, query)

    def parse_sum(self, query, column):
        sql = ("SELECT SUM(%s)"
               " FROM %s" % (column, self.from_clause))
        return self.do_single_val(sql, query)

    def parse_avg(self, query, column):
        sql = ("SELECT AVG(%s)"
               " FROM %s" % (column, self.from_clause))
        return self.do_single_val(sql, query)

    def parse_select(self, query, cols):
        sql = ("SELECT %s "
               "FROM %s" % (",".join(cols), self.from_clause))
        return self.do_package(sql, query)

    def parse_select_many(self, query):
        sql = ("SELECT *"
               " FROM %s" % self.from_clause)
        return self.do_package(sql, query)

    def append_clauses(self, sql, query):
        sql = self.add_where(sql, query)
        sql = self.do_group(sql, query)
        return sql

    def add_where(self, sql, query):
        if query:
            for index, expression in enumerate(query.expressions):
                #                import ipdb; ipdb.set_trace()
                clause = "WHERE" if index == 0 else "AND"
                if expression.node_type == "Equal":
                    sql += " %s %s = %s" % (clause,
                                            expression.lhs,
                                            expression.rhs)
                else:
                    sql += " %s %s" % (clause, expression)
        return sql

    def do_group(self, sql, query):
        if query.group_expression:
            import ipdb; ipdb.set_trace()
        return sql

    def package(self, rows):
        fields = self._fields
        TupleType = namedtuple(self.tuple_name.split(" ")[0], fields)
        return [TupleType(*row) for row in rows]

    def _execute_with_connection(self, connection, sql):
        cursor = connection.cursor()
        cursor.execute(sql)
        # ghetto but for now it'll do.
        self._fields = self.extract_fields_from_cursor(cursor)
        results = cursor.fetchall()
        return results

    def execute(self, sql):
        need_to_return_conn_to_pool = False
        connection = self.connection
        if not connection:
            connection = self.get_connection_from_pool()
            need_to_return_conn_to_pool = True
        try:
            return self._execute_with_connection(connection, sql)
        finally:
            if need_to_return_conn_to_pool:
                connection.rollback()
                self.return_connection_to_pool(connection)

    def get_connection_from_pool(self):
        pool = get_pool()
        return pool.getconn()

    def return_connection_to_pool(self, connection):
        pool = get_pool()
        pool.putconn(connection)


class Psycopg2TableProvider(Psycopg2RelationProvider):

    def __init__(self, table_name=None, connection=None):
        self.table_name = table_name
        self.connection = connection

    @property
    def from_clause(self):
        return self.table_name

    @property
    def tuple_name(self):
        return self.table_name.split(".")[-1]

    def extract_fields_from_cursor(self, cursor):
        return ", ".join(t[0] for t in cursor.description)


class Psycopg2JoinProvider(Psycopg2RelationProvider):

    def __init__(self, relation1, relation2, on_clause, connection=None):
        self.relation1 = relation1
        self.relation2 = relation2
        self.on_clause = on_clause
        self.connection = connection

    @property
    def from_clause(self):
        return "%s INNER JOIN %s ON %s" % (self.relation1,
                                           self.relation2,
                                           self.on_clause)

    def safe_name(self, name):
        name = SAFE_NAME_EQ_RE.sub("_eq_", name)
        name = SAFE_NAME_RE.sub("_", name)
        return name

    @property
    def tuple_name(self):
        return "join_%s_to_%s_on_%s" % (self.safe_name(self.relation1),
                                        self.safe_name(self.relation2),
                                        self.safe_name(self.on_clause))

    def extract_fields_from_cursor(self, cursor):
        return ", ".join(t[0] for t in cursor.description)


def table(table_name, connection=None):
    return Psycopg2TableProvider(table_name, connection)


# TODO: inner/outer/etc.
# TODO: multi-joins?
# TODO: I don't like this whole approach to doing joins... eventually needs to
# be replaced with a .join clause
def join(relation1, relation2, on_clause, connection=None):
    return Psycopg2JoinProvider(relation1, relation2, on_clause, connection)
