from quazy import DBTable

class SomeTable(DBTable):
    name: str
    one_more_field: int

class ExtraTable(DBTable):
    value1: int
    value2_renamed: int
