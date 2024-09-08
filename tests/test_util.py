from ingest_lib import util


class TestUtil:

    def test_create_hash_column(self):
        value = "hello"
        encoded_value = util.hash_string(value)

        for _ in range(30):
            assert encoded_value == util.hash_string(value), (
                "hash method should consistency produce the same value")

    def test_hash_column(self):
        column = "hash_column"

        hash1 = util.create_hash_column(self.df1.copy(), ["address"], column)
        hash2 = util.create_hash_column(
            self.df1.copy(), ["address", "city"], column)
        count_joined = len(hash1.join(hash2.set_index(column), on=column,
                                      how="inner", rsuffix="_another"))
        assert count_joined == 0, (
            "hash string parameter and be created from the list of columns",
            "provided.")
