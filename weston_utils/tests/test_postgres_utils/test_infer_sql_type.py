# pytest for weston_utils.postgres_utils.infer_sql_type()

import pandas as pd
from weston_utils.postgres_utils import infer_sql_type

def test_integer_series():
    s = pd.Series([1, 2, 3])
    assert infer_sql_type(s) == "INT"

def test_float_series():
    s = pd.Series([1.1, 2.2, 3.3])
    assert infer_sql_type(s) == "FLOAT"

def test_boolean_series():
    s = pd.Series([True, False])
    assert infer_sql_type(s) == "BOOLEAN"

def test_datetime_series():
    s = pd.Series(pd.date_range("2023-01-01", periods=3))
    assert infer_sql_type(s) == "TIMESTAMP"

def test_timedelta_series():
    s = pd.Series([pd.Timedelta("1 day"), pd.Timedelta("2 days")])
    assert infer_sql_type(s) == "INTERVAL"

def test_categorical_text():
    s = pd.Series(["low", "high", "medium"], dtype="category")
    assert infer_sql_type(s) == "TEXT"

def test_categorical_varchar():
    s = pd.Series(["x", "y", "z"], dtype="category")
    assert infer_sql_type(s, prefer_varchar=True) == "VARCHAR"

def test_string_text_default():
    s = pd.Series(["apple", "banana", "cherry"])
    assert infer_sql_type(s) == "TEXT"

def test_string_prefer_varchar():
    s = pd.Series(["foo", "bar", "baz"])
    assert infer_sql_type(s, prefer_varchar=True) == "VARCHAR"

def test_object_mixed_type():
    s = pd.Series([1, "a", 3.14])
    assert infer_sql_type(s) == "TEXT"

def test_fallback_object():
    s = pd.Series([{"a": 1}, {"b": 2}])
    assert infer_sql_type(s) == "TEXT"

def test_mostly_datetime_strings_warns(caplog):
    s = pd.Series(["2023-01-01", "2023-01-02", "not a date", "2023-01-03", "still not"])
    with caplog.at_level("WARNING"):
        result = infer_sql_type(s)
        assert result == "TEXT"
        assert "parse as datetimes" in caplog.text

def test_empty_series_returns_text():
    s = pd.Series([], dtype=object)
    assert infer_sql_type(s) == "TEXT"