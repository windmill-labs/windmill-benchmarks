Windmill for Data Pipelines
===========================

This repo contains instructions and pieces of code that were useful to benchmark Windmill as a data integration platform.
The goal of this experimentation was to compare the performance, both in terms of memory footprint and speed, of using
Windmill with Polars or DuckDB to process data VS Spark, which is commonly used for this purpose.

The results are available in [this blog post](https://www.windmill.dev/blog/launch-week-1/data-pipeline-orchestrator#in-memory-data-processing-performance)

## Generate TPC-H datasets

We benchmarked Polars, DuckDB and Spark using the [TPC-H datasets](https://www.tpc.org/tpch/). The scripts necessary to generate the data in various sizes
can be downloaded freely on [this page](https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.1&mode=CURRENT-ONLY).
The full specifications on the generated generated datasets is available as a PDF on [this page](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).

Note: The above scripts won't work on OSX, it needs a few tweaks. [This repo](https://github.com/gregrahn/tpch-kit) among others, can be used instead to generate the datasets.
As indicated on the README, the generation is made simple:
```bash
export SCALE_FACTOR='1'

export DSS_CONFIG=$(pwd)/dbgen
export DSS_QUERY=$DSS_CONFIG/queries
export DSS_PATH=$(pwd)/output_data/$SCALE_FACTOR

# then from the ./dbgen/ folder:
make MACHINE=MACOS DATABASE=POSTGRESQL # to compile the binary
./qgen -v -c -d -s $SCALE_FACTOR > tpch-stream.sql # to generate the queries
./dbgen -vf -s $SCALE_FACTOR # to generate the datasets into $DSS_PATH
```

The datasets will be generated as .tbl files, which is equivalent to CSVs with `|` as a separator. You can rename them to CSVs:
```bash
# from the folder containing the .tbl files
for file in $(ls *.tbl); do mv $file  $(basename $file .tbl).csv; done
```

Then you will need to upload those files to an S3 bucket. If you're using AWS, this can be done with the CLI:
```bash
# to setup your AWS key
aws configure
# to upload the files to a bucket named 'windmill' at '/tpc-h/1g/input/'
export SCALE_FACTOR='1g'
for file in $(ls *.csv); do echo $file; aws s3 cp $file s3://windmill/tpc-h/$SCALE_FACTOR/input/; done
```

## Queries

We select 9 queries from the generated queries in `tpch-stream.sql` and we converted them to DuckDB, Polars and Spark
The resulting code in in the 3 files [duckdb](duckdb/tpc_h.py), [polars](polars/tpc_h.py), [spark](spark/tpc_h.py).

Of course the code in duckdb and polars folders should be executed on Windmill in various scripts and flows.

## Running the queries on Spark

The spark queries were run on a single node equivalent to the Windmill worker executing the queries for DuckDB and Polars.
Installing Spark can be done by downloading it [here](https://spark.apache.org/downloads.html). Then the script in the
spark folder can be executed using `spark-submit`:
```bash
spark-submit spark/tpc_h.py --remote true --scale '1g' --query 1 # for the first queru
```

## Running the queries on Airflow

We also implemented and Airflow DAG to run all the queries on airflow using Polar (The same can be done with DuckDB). The 
DAG code is available [here](airflow/tpc_h.py)
