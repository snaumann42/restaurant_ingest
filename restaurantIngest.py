import os

from dataCleaning.restaurantCleaning import RestaurantCleaning
from fuzzyMatching.restaurantFuzzy import RestaurantFuzzy


if __name__ == '__main__':
    import time
    start_time = time.time()

    # Read all files from the ingest path
    ingest_files = filter(lambda file:
                          os.path.isfile(
                              RestaurantCleaning.ingest_path + file),
                          os.listdir(RestaurantCleaning.ingest_path))
    initial_count = 0
    for file_name in ["file1.csv", "file2.csv"]:
        print(f"Attempting to ingest {file_name}")
        with open(RestaurantCleaning.ingest_path + file_name) as file_handle:
            df_ingested = RestaurantCleaning.evaluate(file_handle)
            initial_count += len(df_ingested)
            # create ingest file boolean field
            column_name = (
                RestaurantCleaning.file_field_prefix + file_name.split(".")[0])
            df_ingested = df_ingested.assign(**{column_name: True})

            df_processed = RestaurantCleaning.transform(df_ingested)
            RestaurantCleaning.load(df_processed)
            print(f"Completed clean for {file_name}\n")

    print("Finalizing clean.")
    RestaurantCleaning.finalize()

    print("Starting fuzzy deduplication\n")
    with open(RestaurantFuzzy.ingest_source) as file_handle:
        df_cleaned = RestaurantFuzzy.evaluate(file_handle)
        df_fuzzy_dedup = RestaurantFuzzy.transform(df_cleaned)
        RestaurantFuzzy.load(df_fuzzy_dedup)

    print("Finalizing fuzzy.")
    RestaurantFuzzy.finalize()

    duration = (time.time() - start_time)
    cleaned_count = sum(1 for _ in open(RestaurantCleaning.destination))
    fuzzy_count = sum(1 for _ in open(RestaurantFuzzy.destination))

    print(f"--- {duration} seconds ---")
    print(f"--- initial row count: {initial_count} ---")
    print(f"--- cleaned row count: {cleaned_count} ---")
    print(f"--- fuzzy row count: {fuzzy_count} ---")
