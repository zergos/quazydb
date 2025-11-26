class QuazyError(Exception):
    """Common error base class"""


class QuazyFieldTypeError(QuazyError):
    """Field type definition error. For example, type is not supported"""


class QuazyFieldNameError(QuazyError):
    """Field name definition error. Probably, naming collision occurs"""


class QuazyMissedField(QuazyError):
    """Field name missed in a table while access to a data"""


class QuazyTranslatorException(QuazyError):
    """SQL translator has run into something trying to prepare a query"""


class QuazyNotSupported(QuazyError):
    """You are trying to perform something not supported by QuazyDB yet"""


class QuazyWrongOperation(QuazyError):
    """You are trying to use something the wrong way"""


class QuazyFrozen(QuazyError):
    """Trying to modify the frozen query"""
    def __init__(self, msg: str):
        super().__init__(f"{msg}. Query is frozen")
