from pathlib import Path
import numpy as np

import pandas
import csv
from pandas import StringDtype, BooleanDtype

from ingest_lib.ingest import Ingest
from ingest_lib import util


class RestaurantFuzzy(Ingest):
    ingest_source: str = "clean_restaurant.csv"
    destination: str = "fuzzy_restaurant.csv"
    temp_destination: Path = Path("temp_fuzzy_restaurant.csv")
    total_count: str = "total_count"
    join_field: str = "unique_id"
    fuzzy_index_column: str = "fuzzy_index"
    fuzzy_match_column: str = "name"
    debug_column: str = "name_dupl"
    drop_suffix: str = "_redundant"
    file_field_prefix: str = "is_from_"
    unique_columns = ["address", "city", "zip"]
    ingest_schema = {"unique_id": StringDtype(), "name": StringDtype(),
                     "address": StringDtype(), "city": StringDtype(),
                     "zip": StringDtype(), "is_from_file1": BooleanDtype(),
                     "total_count": np.int64, "is_from_file2": BooleanDtype()}

    @classmethod
    def handle_fuzzy_matches(cls, df_raw, debug=False):
        """ Used as a dataframe apply/lambda to process fuzzy matches. This
        method marks fuzzy matches via the util method, sums the 'total_counts'
        of all duplicates and combines the file boolean fields. Then removes
        all duplicates after the original fields are updates.
        """
        df_fuzzy = util.mark_fuzzy_matches(
            df_raw,
            cls.fuzzy_match_column,
            groupby_vals=cls.unique_columns,
            dupl_index_column=cls.fuzzy_index_column)

        file_columns = []
        for column in df_raw.columns:
            if cls.file_field_prefix in column:
                file_columns.append(column)

        df_dupl = df_fuzzy.loc[df_fuzzy[cls.fuzzy_index_column] != -1]
        non_dupl = df_fuzzy.loc[df_fuzzy[cls.fuzzy_index_column] == -1].drop(
            cls.fuzzy_index_column, axis=1)

        field_select = [cls.fuzzy_index_column, cls.total_count, *file_columns]
        if debug:
            field_select.append[cls.name]
        df_dupl = df_dupl[field_select]

        # builds aggregation, then runs it against our duplicate rows.
        aggregation = {}
        for field in file_columns:
            aggregation[field] = "any"
        aggregation[cls.total_count] = "sum"
        if debug:
            aggregation[cls.name] = "first"
        df_dupl.groupby(cls.fuzzy_index_column).agg(aggregation)

        # outer join on unique id, suffix duplicate fields with know value
        non_dupl = non_dupl.join(
            df_dupl, how="left", rsuffix=cls.drop_suffix).reset_index()

        # count total duplicated rows (total_count + total_count_redundant)
        non_dupl[cls.total_count] = non_dupl[cls.total_count] + non_dupl[
                cls.total_count + cls.drop_suffix].fillna(0)

        # ensure to combine file booleans
        with pandas.option_context("future.no_silent_downcasting", True):
            for file_boolean in file_columns:
                non_dupl[file_boolean] = non_dupl[file_boolean] | non_dupl[
                    file_boolean + cls.drop_suffix
                    ].fillna(False).infer_objects(copy=False)

        if debug:
            non_dupl[cls.debug_column] = non_dupl[cls.name + cls.drop_suffix]

        for column in non_dupl.columns:
            if (cls.drop_suffix in column):
                non_dupl.drop(column, axis=1, inplace=True)

        return non_dupl

    @classmethod
    def transform(cls, df_raw):
        # convert appropriate fields
        for column in df_raw.columns:
            if cls.file_field_prefix in column:
                df_raw[column] = df_raw[column].astype(bool)
        df_raw[cls.total_count] = df_raw[cls.total_count].astype(np.int64)

        result = util.multi_processing(df_raw, cls.handle_fuzzy_matches,
                                       groupby="city")

        return result
