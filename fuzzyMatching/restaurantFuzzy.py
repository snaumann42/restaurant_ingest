from pathlib import Path
import numpy as np

import pandas
from pandas import StringDtype, BooleanDtype, DataFrame

from ingest_lib.ingest import Ingest
from ingest_lib import util


class RestaurantFuzzy(Ingest):
    ingest_source: str = "clean_restaurant.csv"
    destination: str = "fuzzy_restaurant.csv"
    temp_destination: Path = Path("temp_fuzzy_restaurant.csv")
    total_count: str = "total_count"
    join_field: str = "unique_id"
    drop_suffix: str = "_redundant"
    file_field_prefix: str = "is_from_"
    unique_columns = ["address", "city", "zip"]
    ingest_schema = {"unique_id": StringDtype(), "name": StringDtype(),
                     "address": StringDtype(), "city": StringDtype(),
                     "zip": StringDtype(), "is_from_file1": BooleanDtype(),
                     "total_count": np.int64, "is_from_file2": BooleanDtype()
                     }
    
    @classmethod
    def fuzzy_matches(cls, df_raw):
        return util.remove_fuzzy_matches(
            df_raw, groupby_vals=cls.unique_columns, match_field="name")

    @classmethod
    def transform(cls, df_raw):
        result = util.multi_processing(df_raw, cls.fuzzy_matches,
                                       groupby="city")
        return result
