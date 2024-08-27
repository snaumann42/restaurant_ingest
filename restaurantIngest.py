from pathlib import Path
import os
from collections import defaultdict
import numpy as np

import pandas
from pandas import StringDtype, BooleanDtype, DataFrame

from ingest_lib.ingest import Ingest
from ingest_lib import util


class RestaurantIngest(Ingest):
    """A class to define the basic functionality of our ingests."""

    ingest_path: str = "sample_data/"
    temp_destination: Path = Path("temp_restaurant.csv")
    destination: str = "final_restaurant.csv"
    total_count: str = "total_count"
    join_field: str = "unique_id"
    drop_suffix: str = "_redundant"
    file_field_prefix: str = "is_from_"
    redundant_columns = ["address", "name", "city", "zip"]
    unique_columns = ["address", "name", "city", "zip"]
    ingest_schema = {"name": StringDtype(), "address": StringDtype(),
                     "city": StringDtype(), "zip": StringDtype()}

    output_schema = {join_field: StringDtype(),
                     total_count: np.int64}
    output_schema.update(ingest_schema)

    @classmethod
    def transform(cls, df_raw):
        """Deduplicated and clean up data and enrich with additional fields."""
        # drop rows with null values
        df_no_na = df_raw.dropna()
        # drop address rows with zips not equal to 5 in length
        df_no_na = df_no_na[df_no_na.zip.str.len() == 5]
        # cleanup address data to allow for better deduplication
        df_no_na["address"] = df_no_na["address"].apply(
            util.clean_address_data)
        # cleanup name data to allow for better deduplication
        df_no_na["name"] = df_no_na["name"].apply(util.clean_name_data)

        # Create unique field based on row's unique fields
        df_with_hash = util.create_hash_column(df_raw,
                                               cls.unique_columns,
                                               cls.join_field)
        # get a count of all duplicates in this file
        df_dup_count = df_with_hash.groupby(
            cls.join_field)[cls.join_field].count().reset_index(
                name=cls.total_count)
        # remove all duplicates
        df_with_hash.drop_duplicates(subset=cls.join_field, inplace=True)

        df_current_data = df_with_hash.join(
            df_dup_count.set_index(cls.join_field),
            on=cls.join_field, how="right")
        print(f"current ingest row count - {len(df_current_data)}")

        if (cls.temp_destination.is_file()):
            df_previous_data: DataFrame

            try:
                with open(cls.temp_destination) as temp_file:
                    df_previous_data = pandas.read_csv(
                        temp_file, dtype=defaultdict(BooleanDtype,
                                                     **cls.output_schema))
            except Exception as ex:
                print(f'Failure occurred while processing temp file, {ex=}')
                raise
            # outer join on unique id, suffix duplicate fields with know value
            df_current_data = df_current_data.set_index(cls.join_field).join(
                df_previous_data.set_index(cls.join_field),
                how="outer", rsuffix=cls.drop_suffix).reset_index()
            print("current joined with previous ingest row count - ",
                  f"{len(df_current_data)}")
            # count total duplicated rows (total_count + total_count_redundant)
            df_current_data[cls.total_count] = df_current_data[
                cls.total_count].fillna(0) + df_current_data[
                    cls.total_count + cls.drop_suffix].fillna(0)

            # coalesce redundant columns
            for column in cls.redundant_columns:
                df_current_data[column] = df_current_data[
                    column].combine_first(df_current_data[column
                                                          + cls.drop_suffix])

            # Drop redundant columns
            for column in df_current_data.columns:
                if (cls.drop_suffix in column):
                    df_current_data.pop(column)

            # Ensure file fields have nulls filled in with False
            with pandas.option_context("future.no_silent_downcasting", True):
                for column in df_current_data.columns:
                    if (cls.file_field_prefix in column):
                        df_current_data[column] = df_current_data[
                            column].fillna(False).infer_objects(copy=False)

            # Ensure total count remains an int
            df_current_data[cls.total_count] = df_current_data[
                cls.total_count].fillna(0).astype(np.int64)

            df_current_data.drop_duplicates(subset=cls.join_field,
                                            inplace=True)
            print(f"final row count - {len(df_current_data)}")

        return df_current_data


if __name__ == '__main__':
    import time
    start_time = time.time()

    # Read all files from the ingest path
    ingest_files = filter(lambda file:
                          os.path.isfile(RestaurantIngest.ingest_path + file),
                          os.listdir(RestaurantIngest.ingest_path))
    for file_name in ["file1.csv", "file2.csv"]:
        print(f"Attempting to ingest {file_name}")
        with open(RestaurantIngest.ingest_path + file_name) as file_handle:
            df_processed = RestaurantIngest.evaluate(file_handle, file_name)
            RestaurantIngest.load(df_processed)
            print(f"Completed ingesting {file_name}\n")

    print("Finalizing run.")
    if (RestaurantIngest.temp_destination.is_file()):
        os.rename(RestaurantIngest.temp_destination,
                  RestaurantIngest.destination)

    duration = (time.time() - start_time)
    row_count = sum(1 for _ in open('final_restaurant.csv'))

    print(f"--- {duration} seconds ---\n--- {row_count} rows ---")
