import re

from collections import namedtuple

from pynq.providers import IPynqProvider
from pynq.enums import Actions

import psycopg2


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

    def parse_count(self, query):
        sql = ("SELECT COUNT(*)"
               " FROM %s" % self.from_clause)
        # TODO: Refactor to DRY up all the parse_* methods.
        sql = self.add_where(sql, query)
        sql = self.do_group(sql)
        return self.package(self.execute(sql))

    def parse_max(self, query, column):
        sql = ("SELECT MAX(%s)"
               " FROM %s" % (column, self.from_clause))
        # TODO: Refactor to DRY up all the parse_* methods.
        sql = self.add_where(sql, query)
        sql = self.do_group(sql)
        return self.package(self.execute(sql))

    def parse_min(self, query, column):
        sql = ("SELECT MIN(%s)"
               " FROM %s" % (column, self.from_clause))
        # TODO: Refactor to DRY up all the parse_* methods.
        sql = self.add_where(sql, query)
        sql = self.do_group(sql)
        return self.package(self.execute(sql))

    def parse_sum(self, query, column):
        sql = ("SELECT SUM(%s)"
               " FROM %s" % (column, self.from_clause))
        # TODO: Refactor to DRY up all the parse_* methods.
        sql = self.add_where(sql, query)
        sql = self.do_group(sql)
        return self.package(self.execute(sql))

    def parse_avg(self, query, column):
        sql = ("SELECT AVG(%s)"
               " FROM %s" % (column, self.from_clause))
        # TODO: Refactor to DRY up all the parse_* methods.
        sql = self.add_where(sql, query)
        sql = self.do_group(sql)
        return self.package(self.execute(sql))

    def parse_select(self, query, cols):
        sql = ("SELECT %s "
               "FROM %s" % (",".join(cols), self.from_clause))
        sql = self.add_where(sql, query)
        sql = self.do_group(sql)
        return self.package(self.execute(sql))

    def parse_select_many(self, query):
        sql = ("SELECT *"
               " FROM %s" % self.from_clause)
        sql = self.add_where(sql, query)
        sql = self.do_group(sql)
        return self.package(self.execute(sql))

    def add_where(self, sql, query):
        if query:
            for index, expression in enumerate(query.expressions):
                #                import ipdb; ipdb.set_trace()
                clause = "WHERE" if index == 0 else "AND"
                sql += " %s %s" % (clause, expression)
            sql = re.sub("==", "=", sql) # TODO: For reals.
        return sql

    def do_group(self, sql):
        # TODO: Do.
        return sql

    def package(self, rows):
        fields = self._fields
        print "FIELDS: %s" % self._fields
        TupleType = namedtuple(self.tuple_name.split(" ")[0], fields)
        return [TupleType(*row) for row in rows]

    def extract_fields_from_cursor(self, cursor):
        # TODO: make the names unique eventually... for now we assume
        # they are.
        return ", ".join(t[0] for t in cursor.description)

    def execute(self, sql):
        # TODO: Allow for passing in existing connection, etc. but for now:
        connection = psycopg2.connect("dbname=milodb user=milo")
        cursor = connection.cursor()
        print "SQL: %s" % sql
        cursor.execute(sql)
        # ghetto but for now it'll do.
        self._fields = self.extract_fields_from_cursor(cursor)
        print "FIELDS(1): %s" % self._fields
        results = cursor.fetchall()

        # only do this if we created the conn, also contextmanager, etc
        connection.rollback()
        connection.close()
        return results


class Psycopg2TableProvider(Psycopg2RelationProvider):

    def __init__(self, table_name=None):
        self.table_name = table_name

    @property
    def from_clause(self):
        return self.table_name

    @property
    def tuple_name(self):
        return self.table_name.split(".")[-1]


class Psycopg2JoinProvider(Psycopg2RelationProvider):

    def __init__(self, relation1, relation2, on_clause):
        self.relation1 = relation1
        self.relation2 = relation2
        self.on_clause = on_clause

    @property
    def from_clause(self):
        return "%s INNER JOIN %s ON %s" % (self.relation1,
                                           self.relation2,
                                           self.on_clause)

    @property
    def tuple_name(self):
        return "todo_finish_me"


def table(table_name):
    return Psycopg2TableProvider(table_name)


# TODO: inner/outer/etc.
# TODO: multi-joins?
def join(relation1, relation2, on_clause):
    return Psycopg2JoinProvider(relation1, relation2, on_clause)
