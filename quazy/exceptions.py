class QuazyError(Exception):
    pass


class QuazyFieldTypeError(QuazyError):
    pass


class QuazySubclassError(QuazyError):
    pass


class QuazyFieldNameError(QuazyError):
    pass


class QuazyMissedField(QuazyError):
    pass


class QuazyTranslatorException(QuazyError):
    pass


class QuazyNotSupported(QuazyError):
    pass


class QuazyWrongOperation(QuazyError):
    pass

