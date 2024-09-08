This project will ingest multiple restaurant data sources, clean up the data and remove duplicates. It will output a CSV containing all the restaurants, the count of duplicates and which file they originated from.

Since this is meant to be a production data pipeline, each ingest in our ingests folder will be considered a main/runnable ingest. The task description which lead to this project is added below.

Current statistics, I'll profile the project to get a better idea of the multiprocessing time improvements.
--- 20.029337882995605 seconds ---
--- initial row count: 417559 ---
--- cleaned row count: 345788 ---
--- fuzzy row count: 315578 ---

## Task Description
You are provided with two datasets named file1.csv and file2.csv, these files contain the name
of a restaurant and its address.

You are asked to write code in Python/Java/Scala to read both CSV files, match the entries
between the two files, and harmonize column names. Try to structure your code as a
production-ready pipeline.

In addition to the code in your project, please generate a CSV file and a report (1-2 pages).
The output CSV file should contain:
 - The full list of restaurants and addresses present in any of the two files without
duplicates
 - A unique identifier for each restaurant
 - The total number of occurrences of each entry
 - Two boolean flags indicating whether the entry is present in file1 or file2
The report should briefly explain your approach to cleaning and matching the data, and possible
future enhancements.

The report should also discuss high level architecture and how these use cases can be
implemented at scale to accommodate higher volume and velocity of the data.

Hints:
 - Input files might contain duplicates
 - Restaurant names and address formats are not standardized, the same address might
show up in different formats.