import numpy as np
import os

import pandas
from pandas import StringDtype

from restaurantIngest import RestaurantIngest
from ingest_lib import util


class TestUtil:
    file1 = "is_from_file1"
    file2 = "is_from_file2"
    cafe_hash = "b'--Jg1ePrm1y9D5sqJRv5ygkbOYH9CFblnylHWqSUEu9HvTfrtY3X-paK7Iua22eT9b4DYy_Wd_Qe5_96PFYLTQ=='"
    df1 = pandas.DataFrame({
        "name": ["TBS CORNER DINER AND GRILL", "TBS CORNER DINER AND GRILL",
                 "ANOTHER"],
        "address": ["3085 JUPITER BLVD STE 19", "123 Different",
                    "142 Another"],
        "city": ["PALM BAY", "PALM BAY", "PALM BAY"],
        "zip": ["32909", "32909", "32909"]
        }, dtype=StringDtype())
    df1[file2] = True

    test_data = (f""""name","address","city","zip","{file1}","{RestaurantIngest.join_field}","total_count"
"TBS CORNER DINER AND GRILL","3085 JUPITER BLVD STE 19","PALM BAY","32909",True,"b'--9ZuKyEz-kai1S9zENcsbwzRmLdoIx0N6qO-PtUv3UWkX21FnpKj7wuxtfoq87-wV_QWcfAoSNY-Od5QZ4hEA=='",1
"COURTYARD CAFE","200 SE 2 AVE","MIAMI","33131",True,"{cafe_hash}",2""")

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

    def test_restaurant_ingest(self):
        # write out temp to allow ingest to join
        with open(RestaurantIngest.temp_destination, "w") as temp_file:
            temp_file.write(self.test_data)
            temp_file.close()
        result = RestaurantIngest.transform(self.df1)
        # delete temp file (ensures normal run doesn't encounter error)
        os.remove(RestaurantIngest.temp_destination)

        # all fields, including shouldn't be null
        df_null_check = result.isnull()
        df_check = df_null_check.apply(np.logical_or.reduce, axis=0)
        for column in result.columns:
            assert not df_check[column], ("No data should be null in the",
                                          " returned result data frame.",
                                          f" failed for {column}")

        # test file fields are filled correctly
        df_check = result[
            result[RestaurantIngest.join_field] == self.cafe_hash]
        result = df_check.apply(np.logical_or.reduce, axis=0)
        assert result[self.file1], ("All cafe rows are from file1")
        assert not result[self.file2], ("No cafe rows are from file2")

        # ensure all duplicates are removed
        # test count is accurate
        # name, address, city, zip are all not null?
        # check both file names exist
