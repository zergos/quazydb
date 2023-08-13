import unittest

from quazy import DBFactory

db: DBFactory | None = None


def setupModule():
    db = DBFactory.postgres(conninfo="postgresql://quazy:quazy@localhost/quazy")
    db.clear()


class MigrationTests(unittest.TestCase):

    def test_initial_migration(self):
        ...
