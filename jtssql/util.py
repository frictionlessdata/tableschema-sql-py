import datetime
import decimal
import json

from sqlalchemy.types import Unicode, BigInteger, Boolean, Float

JTS_TYPES = {
    'string': Unicode,
    'integer': BigInteger,
    'boolean': Boolean,
    'number': Float,
    # FIXME: add proper support for dates
    # 'date': Date
    'date': Unicode
}


def json_default(obj):
    if isinstance(obj, datetime.datetime):
        obj = obj.date()
    if isinstance(obj, datetime.date):
        obj = obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        obj = obj.to_eng_string()
    return obj
