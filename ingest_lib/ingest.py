import csv

import pandas
from pandas import StringDtype


class Ingest:
    """A class to define the basic functionality of our ingests."""

    ingest_path: str
    destination: str
    ingest_schema: dict
    file_field_prefix: str

    @classmethod
    def evaluate(cls, file_handle, file_name):
        # retrieve field names from schema
        field_names = list(cls.ingest_schema.keys())
        # add additional null values to remove
        missing_values = ["", "NAN", "*"]
        df_data = pandas.read_csv(
            file_handle, na_values=missing_values, dtype=StringDtype())

        # set proper column names
        df_data.columns = field_names
        # create ingest file boolean field
        column_name = cls.file_field_prefix + file_name.split(".")[0]
        df_data = df_data.assign(**{column_name: True})

        return df_data

    @classmethod
    def transform(cls):
        pass

    @classmethod
    def load(cls, df_data):
        df_data.to_csv(path_or_buf=cls.temp_destination, index=False,
                       quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")


if __name__ == '__main__':
    print("Ingest is a abstract class and not meant to run."
          " Run any ingest from the ingests folder.")
