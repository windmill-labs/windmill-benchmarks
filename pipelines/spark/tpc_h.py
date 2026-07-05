"""TPC-H-derived benchmark — Spark (single node, local[*]).

Runs 9 TPC-H queries (spec Q1, Q3, Q5, Q6, Q10, Q12, Q14, Q16, Q18, exposed
here as query_1..query_9) sequentially in one SparkSession against Parquet
inputs read from S3 (MinIO via s3a). Adaptive Query Execution is enabled and
this is tuned as a single-node ("no cluster") deployment, not a strawman.

This is NOT an audited TPC-H benchmark. See pipelines/README.md.

Config via environment:
  SCALE_FACTOR / S3_BUCKET / S3_ENDPOINT / S3_USE_SSL
  AWS_REGION / AWS_ACCESS_KEY / AWS_SECRET_KEY
  BENCH_OUT     path to write per-query timing JSON
  WRITE_OUTPUT  "true" to write query results back to S3 (default "false")
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, avg, count, when, countDistinct, lit
from datetime import date, timedelta
import builtins
import json
import os
import time

BUCKET = os.environ.get("S3_BUCKET", "windmill")
SF = os.environ.get("SCALE_FACTOR", "1")
WRITE_OUTPUT = os.environ.get("WRITE_OUTPUT", "false").lower() == "true"


def build_session():
    b = (SparkSession.builder.appName("tpch")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true"))
    endpoint = os.environ.get("S3_ENDPOINT")
    if endpoint:
        scheme = "https" if os.environ.get("S3_USE_SSL", "true").lower() == "true" else "http"
        b = (b.config("spark.hadoop.fs.s3a.endpoint", f"{scheme}://{endpoint}")
              .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY", ""))
              .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_KEY", ""))
              .config("spark.hadoop.fs.s3a.path.style.access", "true")
              .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                      os.environ.get("S3_USE_SSL", "true").lower())
              .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))
    return b.getOrCreate()


def load(spark, name):
    return spark.read.parquet(f"s3a://{BUCKET}/tpc-h/{SF}/raw/{name}.parquet")


def query_1(t):  # TPC-H Q1
    return (t["lineitem"]
        .filter(col("L_SHIPDATE") <= lit(date(1998, 12, 1) - timedelta(days=90)))
        .groupBy("L_RETURNFLAG", "L_LINESTATUS")
        .agg(
            sum("L_QUANTITY").alias("SUM_QTY"),
            sum("L_EXTENDEDPRICE").alias("SUM_BASE_PRICE"),
            sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("SUM_DISC_PRICE"),
            sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT")) * (1 + col("L_TAX"))).alias("SUM_CHARGE"),
            avg("L_QUANTITY").alias("AVG_QTY"),
            avg("L_EXTENDEDPRICE").alias("AVG_PRICE"),
            avg("L_DISCOUNT").alias("AVG_DISC"),
            count(lit(1)).alias("COUNT_ORDER"),
        ).sort("L_RETURNFLAG", "L_LINESTATUS"))


def query_2(t):  # TPC-H Q3
    return (t["lineitem"]
        .join(t["orders"], col("L_ORDERKEY") == col("O_ORDERKEY"))
        .join(t["customer"], col("O_CUSTKEY") == col("C_CUSTKEY"))
        .filter(col("C_MKTSEGMENT") == "BUILDING")
        .filter(col("O_ORDERDATE") < lit(date(1995, 3, 15)))
        .filter(col("L_SHIPDATE") > lit(date(1995, 3, 15)))
        .groupBy("L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY")
        .agg(sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("REVENUE"))
        .sort(col("REVENUE").desc(), col("O_ORDERDATE")).limit(10))


def query_3(t):  # TPC-H Q5
    return (t["customer"]
        .join(t["orders"], col("C_CUSTKEY") == col("O_CUSTKEY"))
        .join(t["lineitem"], col("O_ORDERKEY") == col("L_ORDERKEY"))
        .join(t["supplier"], col("L_SUPPKEY") == col("S_SUPPKEY"))
        .join(t["nation"], col("S_NATIONKEY") == col("N_NATIONKEY"))
        .join(t["region"], col("N_REGIONKEY") == col("R_REGIONKEY"))
        .filter(col("R_NAME") == "ASIA")
        .filter(col("C_NATIONKEY") == col("S_NATIONKEY"))  # spec join, missing in original
        .filter(col("O_ORDERDATE") >= lit(date(1994, 1, 1)))
        .filter(col("O_ORDERDATE") < lit(date(1995, 1, 1)))
        .groupBy("N_NAME")
        .agg(sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("REVENUE"))
        .sort(col("REVENUE").desc()))


def query_4(t):  # TPC-H Q6
    return (t["lineitem"]
        .filter(col("L_SHIPDATE") >= lit(date(1994, 1, 1)))
        .filter(col("L_SHIPDATE") < lit(date(1995, 1, 1)))
        .filter(col("L_DISCOUNT").between(round(0.06 - 0.01, 2), round(0.06 + 0.01, 2)))
        .filter(col("L_QUANTITY") < 24)
        .agg(sum(col("L_EXTENDEDPRICE") * col("L_DISCOUNT")).alias("REVENUE")))


def query_5(t):  # TPC-H Q10
    return (t["customer"]
        .join(t["orders"], col("C_CUSTKEY") == col("O_CUSTKEY"))
        .join(t["lineitem"], col("O_ORDERKEY") == col("L_ORDERKEY"))
        .join(t["nation"], col("C_NATIONKEY") == col("N_NATIONKEY"))
        .filter(col("O_ORDERDATE") >= lit(date(1993, 10, 1)))
        .filter(col("O_ORDERDATE") < lit(date(1994, 1, 1)))
        .filter(col("L_RETURNFLAG") == "R")
        .groupBy("C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "N_NAME", "C_ADDRESS", "C_COMMENT")
        .agg(sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("REVENUE"))
        .sort(col("REVENUE").desc()).limit(20))


def query_6(t):  # TPC-H Q12
    return (t["orders"]
        .join(t["lineitem"], col("O_ORDERKEY") == col("L_ORDERKEY"))
        .filter(col("L_SHIPMODE").isin(["MAIL", "SHIP"]))
        .filter(col("L_COMMITDATE") < col("L_RECEIPTDATE"))
        .filter(col("L_SHIPDATE") < col("L_COMMITDATE"))
        .filter(col("L_RECEIPTDATE") >= lit(date(1994, 1, 1)))
        .filter(col("L_RECEIPTDATE") < lit(date(1995, 1, 1)))
        .groupBy("L_SHIPMODE")
        .agg(
            sum(when(col("O_ORDERPRIORITY").isin(["1-URGENT", "2-HIGH"]), 1).otherwise(0)).alias("HIGH_LINE_COUNT"),
            sum(when(~col("O_ORDERPRIORITY").isin(["1-URGENT", "2-HIGH"]), 1).otherwise(0)).alias("LOW_LINE_COUNT"),
        ).sort("L_SHIPMODE"))


def query_7(t):  # TPC-H Q14
    return (t["lineitem"]
        .join(t["part"], col("L_PARTKEY") == col("P_PARTKEY"))
        .filter(col("L_SHIPDATE") >= lit(date(1995, 9, 1)))
        .filter(col("L_SHIPDATE") < lit(date(1995, 10, 1)))
        .select((100.0 * sum(when(col("P_TYPE").startswith("PROMO"),
                                  col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).otherwise(0))
                 / sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT")))).alias("PROMO_REVENUE")))


def query_8(t):  # TPC-H Q16
    # original used contains("Customer") only; spec is LIKE '%Customer%Complaints%'
    bad = t["supplier"].filter(col("S_COMMENT").rlike("Customer.*Complaints")).select("S_SUPPKEY")
    return (t["partsupp"]
        .join(t["part"], col("PS_PARTKEY") == col("P_PARTKEY"))
        .filter(col("P_BRAND") != "Brand#45")
        .filter(~col("P_TYPE").startswith("MEDIUM POLISHED"))
        .filter(col("P_SIZE").isin([49, 14, 23, 45, 19, 3, 36, 9]))
        .join(bad, col("PS_SUPPKEY") == col("S_SUPPKEY"), how="left_anti")
        .groupBy("P_BRAND", "P_TYPE", "P_SIZE")
        .agg(countDistinct("PS_SUPPKEY").alias("SUPPLIER_CNT"))
        .sort(col("SUPPLIER_CNT").desc(), col("P_BRAND"), col("P_TYPE"), col("P_SIZE")))


def query_9(t):  # TPC-H Q18
    big = (t["lineitem"].groupBy("L_ORDERKEY").agg(sum("L_QUANTITY").alias("SUM_QTY"))
           .filter(col("SUM_QTY") > 300).select(col("L_ORDERKEY").alias("BIG_ORDERKEY")))
    return (t["customer"]
        .join(t["orders"], col("C_CUSTKEY") == col("O_CUSTKEY"))
        .join(t["lineitem"], col("O_ORDERKEY") == col("L_ORDERKEY"))
        .join(big, col("O_ORDERKEY") == col("BIG_ORDERKEY"), how="left_semi")
        .groupBy("C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE")
        .agg(sum("L_QUANTITY").alias("SUM_QTY"))
        .sort(col("O_TOTALPRICE").desc(), col("O_ORDERDATE")).limit(100))


QUERIES = [query_1, query_2, query_3, query_4, query_5, query_6, query_7, query_8, query_9]
NEEDS = {  # tables each query touches (to load only what's needed)
    1: ["lineitem"], 2: ["customer", "orders", "lineitem"],
    3: ["customer", "orders", "lineitem", "supplier", "nation", "region"],
    4: ["lineitem"], 5: ["customer", "orders", "lineitem", "nation"],
    6: ["orders", "lineitem"], 7: ["lineitem", "part"],
    8: ["partsupp", "part", "supplier"], 9: ["customer", "orders", "lineitem"],
}


def main():
    spark = build_session()
    spark.sparkContext.setLogLevel("ERROR")
    timings = {}
    t0 = time.time()
    tables = {name: load(spark, name)
              for name in {n for names in NEEDS.values() for n in names}}
    timings["load"] = time.time() - t0
    validate_dir = os.environ.get("SPARK_VALIDATE_DIR")
    results = {}
    for i, qf in enumerate(QUERIES, 1):
        s = time.time()
        res = qf(tables)
        if validate_dir:
            results[i] = [list(r) for r in res.collect()]
        elif WRITE_OUTPUT:
            res.write.parquet(f"s3a://{BUCKET}/tpc-h/{SF}/output-spark/query_{i}.parquet", mode="overwrite")
        else:
            res.count()  # force full execution
        timings[f"query_{i}"] = time.time() - s
    if validate_dir:
        os.makedirs(validate_dir, exist_ok=True)
        with open(os.path.join(validate_dir, f"spark_sf{SF}.json"), "w") as f:
            json.dump(results, f, default=str)
    timings["total_queries"] = builtins.sum(v for k, v in timings.items() if k.startswith("query_"))
    timings["total"] = time.time() - t0
    ver = spark.version
    spark.stop()
    out = os.environ.get("BENCH_OUT")
    if out:
        with open(out, "w") as f:
            json.dump({"engine": "spark", "variant": "aqe", "sf": SF,
                       "spark_version": ver, "timings": timings}, f)
    print(json.dumps(timings, indent=2))


if __name__ == "__main__":
    main()
