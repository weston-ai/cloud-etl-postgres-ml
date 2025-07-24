# pytest for weston_utils.postgres_database.clean_column()

import pytest
from weston_utils.postgres_utils import clean_column

def test_basic_cleanup():
    assert clean_column("  Total Revenue (%) ") == "total_revenue"

def test_hyphens_and_spaces():
    assert clean_column("gross-profit margin") == "gross_profit_margin"

def test_multiple_special_characters():
    assert clean_column("Q1:Revenue/Net%") == "q1_revenue_net"

def test_leading_non_letter():
    assert clean_column("123column name") == "col_123column_name"

def test_multiple_underscores_collapse():
    assert clean_column("Value---Added__Tax") == "value_added_tax"

def test_blank_string():
    assert clean_column("   ") == "col"

def test_only_special_chars():
    assert clean_column("!@#$%^&*()") == "col"

def test_trailing_underscore_removed():
    assert clean_column("field_name__") == "field_name"

def test_preserve_valid_name():
    assert clean_column("valid_column_name") == "valid_column_name"

def test_truncation():
    long_name = "x" * 100
    assert clean_column(long_name) == "x" * 63

def test_custom_max_length():
    assert clean_column("This is a long name with many parts", max_length=10) == "this_is_a"

def test_numeric_only():
    assert clean_column("123456") == "col_123456"

def test_starts_with_underscore_ok():
    assert clean_column("_internal_field") == "_internal_field"

def test_python_keyword():
    assert clean_column("class") == "class_col"

def test_postgres_reserved_keywords():
    assert clean_column("select") == "select_col"
    assert clean_column("from") == "from_col"
    assert clean_column("user") == "user_col"

def test_uppercase_reserved():
    assert clean_column("GROUP") == "group_col"

def test_name_ending_in_underscore():
    assert clean_column("column_name_") == "column_name"

def test_name_with_double_underscores_inside():
    assert clean_column("gross__profit") == "gross_profit"

def test_reserved_suffix_preserved():
    assert clean_column("select_col") == "select_col"

def test_none_input_raises():
    with pytest.raises(TypeError):
        clean_column(None)

def test_numeric_and_special_start():
    assert clean_column("123!bad header") == "col_123_bad_header"

def test_input_with_newlines_tabs():
    assert clean_column("\tHeader with \n breaks ") == "header_with_breaks"

def test_already_sql_friendly():
    assert clean_column("already_clean_1") == "already_clean_1"

# Test camelCase and PascalCase
def test_simple_camel_case():
    assert clean_column("userID") == "user_id"
    assert clean_column("totalRevenue") == "total_revenue"

def test_pascal_case():
    assert clean_column("TotalRevenue") == "total_revenue"
    assert clean_column("UserAccountID") == "user_account_id"

def test_mixed_upper_case_transitions():
    assert clean_column("HTTPServer") == "http_server"
    assert clean_column("JSONResponseCode") == "json_response_code"

def test_single_uppercase_transition_at_end():
    assert clean_column("WhoAmI") == "who_am_i"
    assert clean_column("SaveToDB") == "save_to_db"

def test_acronym_to_word_transition():
    assert clean_column("APIEndpoint") == "api_endpoint"
    assert clean_column("UIDGenerator") == "uid_generator"

def test_already_snake_case():
    assert clean_column("user_id") == "user_id"
    assert clean_column("total_revenue") == "total_revenue"

def test_lowercase_to_uppercase_edge():
    assert clean_column("aB") == "a_b"
    assert clean_column("abC") == "ab_c"

def test_uppercase_then_lower():
    assert clean_column("ABc") == "a_bc"  # common in PascalCase like 'IDCard'

def test_preserve_short_words():
    assert clean_column("Am") == "am"
    assert clean_column("USA") == "usa"
    assert clean_column("aPPLE") == "a_pple"

def test_complex_mixed_format():
    assert clean_column("getHTTPStatusCode200") == "get_http_status_code200"
    assert clean_column("MySQLDataLoader") == "my_sql_data_loader"