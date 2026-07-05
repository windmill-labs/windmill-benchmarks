"""TPC-H-derived benchmark — Polars.

Runs 9 TPC-H queries (spec Q1, Q3, Q5, Q6, Q10, Q12, Q14, Q16, Q18, exposed
here as query_1..query_9) sequentially against Parquet inputs read from S3.

Three variants exercise the Polars 1.x execution paths:
  eager     - pl.read_parquet loads every table fully into RAM, then the
              in-memory engine runs the query plan (highest memory).
  lazy      - pl.scan_parquet + in-memory engine (projection/predicate
              pushdown, but intermediates still materialize in RAM).
  streaming - pl.scan_parquet + streaming engine (out-of-core, survives
              larger-than-memory inputs).

This is NOT an audited TPC-H benchmark. See pipelines/README.md.

Config via environment (see duckdb/tpc_h.py for the shared S3 vars):
  POLARS_VARIANT  "eager" | "lazy" | "streaming"  (default "lazy")
  BENCH_OUT       path to write per-query timing JSON
  WRITE_OUTPUT    "true" to write query results back to S3 (default "false")
"""
import datetime
import json
import os
import time
import polars as pl

BUCKET = os.environ.get("S3_BUCKET", "windmill")
SF = os.environ.get("SCALE_FACTOR", "1")
VARIANT = os.environ.get("POLARS_VARIANT", "lazy")
WRITE_OUTPUT = os.environ.get("WRITE_OUTPUT", "false").lower() == "true"

TABLES = ["customer", "orders", "lineitem", "supplier",
          "part", "partsupp", "nation", "region"]


def storage_options():
    endpoint = os.environ.get("S3_ENDPOINT")
    opts = {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY", ""),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_KEY", ""),
        "aws_region": os.environ.get("AWS_REGION", "us-east-1"),
    }
    if endpoint:
        scheme = "https" if os.environ.get("S3_USE_SSL", "true").lower() == "true" else "http"
        opts["aws_endpoint_url"] = f"{scheme}://{endpoint}"
        opts["aws_allow_http"] = "true"
        opts["aws_virtual_hosted_style_request"] = "false"
    return opts


def load_tables():
    """Return {name: LazyFrame}. Eager fully materializes first."""
    opts = storage_options()
    frames = {}
    for t in TABLES:
        uri = f"s3://{BUCKET}/tpc-h/{SF}/raw/{t}.parquet"
        if VARIANT == "eager":
            frames[t] = pl.read_parquet(uri, storage_options=opts).lazy()
        else:
            frames[t] = pl.scan_parquet(uri, storage_options=opts)
    return frames


def collect(lf):
    if VARIANT == "streaming":
        return lf.collect(engine="streaming")
    return lf.collect(engine="in-memory")


# ---- queries: each returns a LazyFrame ----

def query_1(t):  # TPC-H Q1
    lineitem = t["lineitem"]
    return (lineitem
        .filter(pl.col("L_SHIPDATE") <= datetime.date(1998, 12, 1) - datetime.timedelta(days=90))
        .group_by(["L_RETURNFLAG", "L_LINESTATUS"])
        .agg([
            pl.col("L_QUANTITY").sum().alias("SUM_QTY"),
            pl.col("L_EXTENDEDPRICE").sum().alias("SUM_BASE_PRICE"),
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).sum().alias("SUM_DISC_PRICE"),
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")) * (1 + pl.col("L_TAX"))).sum().alias("SUM_CHARGE"),
            pl.col("L_QUANTITY").mean().alias("AVG_QTY"),
            pl.col("L_EXTENDEDPRICE").mean().alias("AVG_PRICE"),
            pl.col("L_DISCOUNT").mean().alias("AVG_DISC"),
            pl.len().alias("COUNT_ORDER"),
        ])
        .sort(["L_RETURNFLAG", "L_LINESTATUS"]))


def query_2(t):  # TPC-H Q3
    customer, orders, lineitem = t["customer"], t["orders"], t["lineitem"]
    return (lineitem
        .join(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        .join(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
        .filter(pl.col("C_MKTSEGMENT") == "BUILDING")
        .filter(pl.col("O_ORDERDATE") < datetime.date(1995, 3, 15))
        .filter(pl.col("L_SHIPDATE") > datetime.date(1995, 3, 15))
        .group_by(["L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"])
        .agg([(pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).sum().alias("REVENUE")])
        .sort(["REVENUE", "O_ORDERDATE"], descending=[True, False])
        .limit(10))


def query_3(t):  # TPC-H Q5
    customer, orders, lineitem = t["customer"], t["orders"], t["lineitem"]
    supplier, nation, region = t["supplier"], t["nation"], t["region"]
    return (customer
        .join(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(supplier, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .join(region, left_on="N_REGIONKEY", right_on="R_REGIONKEY")
        .filter(pl.col("R_NAME") == "ASIA")
        .filter(pl.col("C_NATIONKEY") == pl.col("S_NATIONKEY"))
        .filter(pl.col("O_ORDERDATE") >= datetime.date(1994, 1, 1))
        .filter(pl.col("O_ORDERDATE") < datetime.date(1995, 1, 1))
        .group_by(["N_NAME"])
        .agg([(pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).sum().alias("REVENUE")])
        .sort(["REVENUE"], descending=True))


def query_4(t):  # TPC-H Q6
    lineitem = t["lineitem"]
    return (lineitem
        .filter(pl.col("L_SHIPDATE") >= datetime.date(1994, 1, 1))
        .filter(pl.col("L_SHIPDATE") < datetime.date(1995, 1, 1))
        .filter(pl.col("L_DISCOUNT").is_between(round(0.06 - 0.01, 2), round(0.06 + 0.01, 2)))
        .filter(pl.col("L_QUANTITY") < 24)
        .select([(pl.col("L_EXTENDEDPRICE") * pl.col("L_DISCOUNT")).sum().alias("REVENUE")]))


def query_5(t):  # TPC-H Q10
    customer, orders, lineitem, nation = t["customer"], t["orders"], t["lineitem"], t["nation"]
    return (customer
        .join(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(nation, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        .filter(pl.col("O_ORDERDATE") >= datetime.date(1993, 10, 1))
        .filter(pl.col("O_ORDERDATE") < datetime.date(1994, 1, 1))
        .filter(pl.col("L_RETURNFLAG") == "R")
        .group_by(["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "N_NAME", "C_ADDRESS", "C_COMMENT"])
        .agg([(pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).sum().alias("REVENUE")])
        .sort(["REVENUE"], descending=True)
        .limit(20))


def query_6(t):  # TPC-H Q12
    orders, lineitem = t["orders"], t["lineitem"]
    return (orders
        .join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .filter(pl.col("L_SHIPMODE").is_in(["MAIL", "SHIP"]))
        .filter(pl.col("L_COMMITDATE") < pl.col("L_RECEIPTDATE"))
        .filter(pl.col("L_SHIPDATE") < pl.col("L_COMMITDATE"))
        .filter(pl.col("L_RECEIPTDATE") >= datetime.date(1994, 1, 1))
        .filter(pl.col("L_RECEIPTDATE") < datetime.date(1995, 1, 1))
        .group_by(["L_SHIPMODE"])
        .agg([
            pl.when(pl.col("O_ORDERPRIORITY").is_in(["1-URGENT", "2-HIGH"])).then(1).otherwise(0).sum().alias("HIGH_LINE_COUNT"),
            pl.when(pl.col("O_ORDERPRIORITY").is_in(["1-URGENT", "2-HIGH"]).not_()).then(1).otherwise(0).sum().alias("LOW_LINE_COUNT"),
        ])
        .sort("L_SHIPMODE"))


def query_7(t):  # TPC-H Q14
    lineitem, part = t["lineitem"], t["part"]
    return (lineitem
        .join(part, left_on="L_PARTKEY", right_on="P_PARTKEY")
        .filter(pl.col("L_SHIPDATE") >= datetime.date(1995, 9, 1))
        .filter(pl.col("L_SHIPDATE") < datetime.date(1995, 10, 1))
        .select([(100.0 * (
            pl.when(pl.col("P_TYPE").str.starts_with("PROMO"))
              .then(pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
              .otherwise(0)).sum()
            / (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).sum()
        ).alias("PROMO_REVENUE")]))


def query_8(t):  # TPC-H Q16
    partsupp, part, supplier = t["partsupp"], t["part"], t["supplier"]
    # NOTE: original code used contains("Customer") OR contains("Complaints"),
    # which does not match the spec's LIKE '%Customer%Complaints%' (ordered).
    # Fixed to an ordered-substring regex so results match DuckDB/Spark.
    bad_suppliers = supplier.filter(
        pl.col("S_COMMENT").str.contains("Customer.*Complaints")
    ).select("S_SUPPKEY")
    return (partsupp
        .join(part, left_on="PS_PARTKEY", right_on="P_PARTKEY")
        .filter(pl.col("P_BRAND") != "Brand#45")
        .filter(pl.col("P_TYPE").str.starts_with("MEDIUM POLISHED").not_())
        .filter(pl.col("P_SIZE").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .join(bad_suppliers, left_on="PS_SUPPKEY", right_on="S_SUPPKEY", how="anti")
        .group_by(["P_BRAND", "P_TYPE", "P_SIZE"])
        .agg([pl.col("PS_SUPPKEY").n_unique().alias("SUPPLIER_CNT")])
        .sort(["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"], descending=[True, False, False, False]))


def query_9(t):  # TPC-H Q18
    customer, orders, lineitem = t["customer"], t["orders"], t["lineitem"]
    big_orders = (lineitem.group_by("L_ORDERKEY")
        .agg([pl.col("L_QUANTITY").sum().alias("SUM_QTY")])
        .filter(pl.col("SUM_QTY") > 300).select("L_ORDERKEY"))
    return (customer
        .join(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(big_orders, left_on="O_ORDERKEY", right_on="L_ORDERKEY", how="semi")
        .group_by(["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"])
        .agg([pl.col("L_QUANTITY").sum().alias("SUM_QTY")])
        .sort(["O_TOTALPRICE", "O_ORDERDATE"], descending=[True, False])
        .limit(100))


QUERIES = [query_1, query_2, query_3, query_4, query_5, query_6, query_7, query_8, query_9]


def main():
    timings = {}
    t0 = time.time()
    t = load_tables()
    if VARIANT == "eager":
        # force materialization cost into "load"
        for name, lf in t.items():
            t[name] = lf.collect(engine="in-memory").lazy()
    timings["load"] = time.time() - t0
    opts = storage_options()
    for i, qf in enumerate(QUERIES, 1):
        s = time.time()
        res = collect(qf(t))
        if WRITE_OUTPUT:
            out = f"s3://{BUCKET}/tpc-h/{SF}/output-polars/query_{i}.parquet"
            res.write_parquet(out, storage_options=opts)
        timings[f"query_{i}"] = time.time() - s
    timings["total_queries"] = sum(v for k, v in timings.items() if k.startswith("query_"))
    timings["total"] = time.time() - t0
    out = os.environ.get("BENCH_OUT")
    if out:
        with open(out, "w") as f:
            json.dump({"engine": "polars", "variant": VARIANT, "sf": SF,
                       "polars_version": pl.__version__, "timings": timings}, f)
    print(json.dumps(timings, indent=2))


if __name__ == "__main__":
    main()
