This project will ingest multiple restaurant data sources, clean up the data and remove duplicates. It will output a CSV containing all the restaurants, the count of duplicates and which file they originated from.

Since this is meant to be a production data pipeline, each ingest in our ingests folder will be considered a main/runnable ingest.

without data cleanup
--- 10.621857166290283 seconds ---
--- 10.790529012680054 seconds ---
--- 10.73390793800354 seconds ---
--- 11.025524377822876 seconds ---
--- 407036 rows ---

with data cleanup
--- 27.63713788986206 seconds ---
--- 27.290466785430908 seconds ---
--- 27.607961893081665 seconds ---
--- 27.578247785568237 seconds ---
--- 345875 rows ---

with Multi Processing
