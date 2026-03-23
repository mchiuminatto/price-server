# Step 2

# Objective

The objective is to produce a parquet file with normalized structure so the rest of the process will rely on a validated known structure.

# How

- The process recives the payload from step one
- Opens the referenced in the payload in source_path
- Normalize the columns names, depending on a strcuture mapper defined for the price provider.

For example for Dukascopy the columns are:

Time (<time-zone>),
Ask,
Bid,
AskVolume,
BidVolume

The output columns should be 

timestamp
ask
bid
ask_volume
bid_volume.


- Extract the instrument from the file name

instrument

- Extrat information from the data istelf:
date-from
date-to
records (rows)

Finally saves the file in the step output folder in parquet format.

# Notes

There could be one or more workers depending on the configuration file (pipeline-config.yaml).

For each step, the number of workers can be specified as follows:

workesrs-number: 
-1: Use all cores available
0: Do not run the step 
n: Use n-2 workers. In this case will control not to run more workers than the number of cpus minus 2.







