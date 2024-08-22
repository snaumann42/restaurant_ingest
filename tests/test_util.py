import pandas
from pandas import StringDtype
import pytest

from ingest_lib import util


class TestUtil:
    df1 = pandas.DataFrame({
        "name": ["steve", "tom", "jack"],
        "address": ["abc", "abc", "abc"],
        "city": ["denver", "denver", "denver"],
        "zip": ["12345", "12345", "12345"]
        }, dtype=StringDtype())

    @pytest.mark.parametrize("test_data", [df1])
    def test_deduplicate_restuarant(test_data):
        count_column = "test_count"
        df_dedup = util.deduplicate_restuarant(test_data.copy,
                                               ["address", "city", "zip"],
                                               count_column)

        assert len(df_dedup.index) == 1, (
            "dedup method should remove duplicates based on columns provided.")

        assert df_dedup[count_column].iloc[0] == 3, (
            "dedup method should add column to count duplicate rows.")

    def test_create_hash_column():
        value = "hello"
        encoded_value = util.hash_string(value)

        for _ in range(30):
            assert encoded_value == util.hash_string(value), (
                "hash method should consistency produce the same value")

    @pytest.mark.parametrize("test_data", [df1])
    def test_hash_column(test_data):
        column = "hash_column"

        hash1 = util.create_hash_column(test_data.copy, ["address"], column)
        hash2 = util.create_hash_column(
            test_data.copy, ["address", "city"], column)
        assert hash1[column] != hash2(column), (
            "hash string parameter and be created from the list of columns",
            "provided.")

    def failure():
        assert 1 == 3, "This should fail."
