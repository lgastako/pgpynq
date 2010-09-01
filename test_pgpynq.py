"""These tests assume you have created a database and user something like
   this:

   $ createuser pgpynq
   Shall the new role by a superuser? (y/n) y
   $ createdb pgpynq_tests -O pgpynq

"""

import unittest

from pynq import From
from pgpynq import table
from pgpynq import join

from pgpynq import get_pool

EXAMPLE_USERS = [("Amy", "Amy Wong"),
                 ("Bender", "Bender Bending Rodriguez"),
                 ("Hermes", "Hermes Conrad"),
                 ("Professor", "Hubert J. Farnsworth"),
                 ("Zoidberg", "John A. Zoidberg"),
                 ("Fry", "Philip J. Fry"),
                 ("Leela", "Turanga Leela")]

EXAMPLE_SINGERS = [("Amy", "Grant"),
                   ("Leela", "James"),
                   ("Mad", "Professor")]


class PgpynqTests(unittest.TestCase):

    def setUp(self):
        pool = get_pool()
        conn = pool.getconn()
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS users")
        cursor.execute("DROP TABLE IF EXISTS singers")
        cursor.execute("""CREATE TABLE users (
                             id SERIAL,
                             name TEXT,
                             fullname TEXT
                          )""")
        cursor.execute("""CREATE TABLE singers (
                              firstname TEXT,
                              lastname TEXT
                          )""")
        for name, fullname in EXAMPLE_USERS:
            cursor.execute("INSERT INTO users (name, fullname)"
                           " VALUES (%s, %s)", (name, fullname))
        for firstname, lastname in EXAMPLE_SINGERS:
            cursor.execute("INSERT INTO singers (firstname, lastname)"
                           " VALUES (%s, %s)", (firstname, lastname))

    def test_simple_select_many(self):
        users = From(table("users")).select_many()
        self.assertEquals(len(EXAMPLE_USERS), len(users))
        self.assert_("Fry" in [u.name for u in users])
        self.assert_("Amy Wong" in [u.fullname for u in users])

    def test_simple_select(self):
        users = From(table("users")).select("id, name")
        self.assertEquals(len(EXAMPLE_USERS), len(users))
        self.assert_("Fry" in [u.name for u in users])
        u = users[0]
        self.assert_(not hasattr(u, "fullname"))

    def test_single_simple_where(self):
        users = From(table("users")).where("name == 'Leela'").select_many()
        self.assertEquals(1, len(users))
        self.assert_(users[0].fullname.startswith("Turanga"))

    def test_composite_where(self):
        users = (From(table("users"))
                 .where("id < 5")
                 .where("id > 2")
                 .select_many())
        self.assertEquals(2, len(users))
        ids = [u.id for u in users]
        self.assert_(3 in ids)
        self.assert_(4 in ids)

    def test_count(self):
        self.assertEquals(7, From(table("users")).count())

    def test_min(self):
        self.assertEquals(1, From(table("users")).min("id"))

    def test_max(self):
        self.assertEquals(7, From(table("users")).max("id"))

    def test_sum(self):
        self.assertEquals(28, From(table("users")).sum("id"))

    def test_avg(self):
        self.assertAlmostEquals(4.0, float(From(table("users")).avg("id")))

    def test_simple_join(self):
        user_singers = From(join("users AS u",
                                 "singers AS s",
                                 "u.name = s.firstname")).select_many()
        self.assertEquals(2, len(user_singers))
        # NOTE: name is from the users table, lastname is from singers
        self.assert_("Amy Grant" in ["%s %s" % (uxs.name,
                                                uxs.lastname)
                                     for uxs in user_singers])


if __name__ == '__main__':
    unittest.main()
