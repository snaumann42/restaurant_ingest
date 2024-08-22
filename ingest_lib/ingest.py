import os
import csv
import dataclasses
import pandas
from pandas import StringDtype
from collections import defaultdict


class Ingest:
    """A class to define the basic functionality of our ingests."""

    ingest_path: str
    destination: str
    ingest_schema: dict
    file_field_prefix: str

    @classmethod
    def evaluate(cls):
        # Read all files from the ingest path
        ingest_files = filter(lambda file:
                              os.path.isfile(cls.ingest_path + file),
                              os.listdir(cls.ingest_path))
        for file_name in ingest_files:
            with open(cls.ingest_path + file_name) as file_handle:
                # retrieve field names from schema
                field_names = list(cls.ingest_schema.keys())

                df_data = pandas.read_csv(
                    file_handle, dtype=StringDtype())
                # set proper column names
                df_data.columns = field_names
                # create ingest file boolean field
                column_name = cls.file_field_prefix + file_name.split(".")[0]
                df_data = df_data.assign(**{column_name: True})
            # sequencially process the files
            cls.transform(df_data)

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
