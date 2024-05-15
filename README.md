# Betfair Historic Data Loader
Contains spark jobs for loading Betfair data and applying transformations. 

## Introduction
This project is designed to load Betfair data into Iceberg tables for exploration. Some notes on the data:
- Betfair historic data can be purchased from [here](https://historicdata.betfair.com/#/home)
- The data is provided per event in JSON files which are bzipped.
- File sizes vary greatly based on the number of recorded changes per event (events that are popular will have many more changes as there is much more activity). 
- The structure of said files is complex containing nested arrays and objects to several levels.

## Methodology
Typical 3 layer architecture:
- Raw: data is loaded here with very minimal change
- Clean: data is normalised as I intend to enrich the data later with other data sources. Some unnecessary columns are removed, some new columns are created.
- Visualisation: data is re-aggregated for visualising.