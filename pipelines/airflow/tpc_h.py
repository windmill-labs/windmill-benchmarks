from datetime import datetime

import os
import polars as pl
import s3fs
import datetime

from airflow import DAG
from airflow.decorators import dag, task

SCALE_FACTOR = "1g"
BUCKET = "windmill"


@task(task_id=f"query_9")
def query_9():
    s3 = s3_file_system()
    with load_dataset(s3, "lineitem") as lineitem_ipt, load_dataset(
        s3, "orders"
    ) as orders_ipt, load_dataset(s3, "customer") as customer_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        orders = pl.read_parquet(orders_ipt).lazy()
        customer = pl.read_parquet(customer_ipt).lazy()
        output = (
            customer.join(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
            .join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
            .join(
                lineitem.group_by("L_ORDERKEY")
                .agg([pl.col("L_QUANTITY").sum().alias("SUM_QTY")])
                .filter(pl.col("SUM_QTY") > 300),
                left_on="O_ORDERKEY",
                right_on="L_ORDERKEY",
                how="inner",
            )
            .group_by(
                ["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"]
            )
            .agg([pl.col("L_QUANTITY").sum().alias("SUM_QTY")])
            .sort(["O_TOTALPRICE", "O_ORDERDATE"], descending=[True, False])
            .limit(100)
            .collect()
        )
        print(output)
        write_dataset(s3, output, 9)


@task(task_id=f"query_8")
def query_8():
    s3 = s3_file_system()
    with load_dataset(s3, "supplier") as supplier_ipt, load_dataset(
        s3, "part"
    ) as part_ipt, load_dataset(s3, "partsupp") as partsupp_ipt:
        supplier = pl.read_parquet(supplier_ipt).lazy()
        part = pl.read_parquet(part_ipt).lazy()
        partsupp = pl.read_parquet(partsupp_ipt).lazy()
        output = (
            partsupp.join(part, left_on="PS_PARTKEY", right_on="P_PARTKEY")
            .filter(pl.col("P_BRAND") != "Brand#45")
            .filter(pl.col("P_TYPE").str.starts_with("MEDIUM POLISHED").not_())
            .filter(pl.col("P_SIZE").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
            .join(
                supplier.filter(pl.col("S_COMMENT").str.contains("Customer")),
                left_on="PS_SUPPKEY",
                right_on="S_SUPPKEY",
                how="anti",
            )
            .group_by(["P_BRAND", "P_TYPE", "P_SIZE"])
            .agg([pl.col("PS_SUPPKEY").n_unique().alias("SUPPLIER_CNT")])
            .sort(
                ["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"],
                descending=[True, False, False, False],
            )
            .collect()
        )
        print(output)
        write_dataset(s3, output, 8)


@task(task_id=f"query_7")
def query_7():
    s3 = s3_file_system()
    with load_dataset(s3, "lineitem") as lineitem_ipt, load_dataset(
        s3, "part"
    ) as part_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        part = pl.read_parquet(part_ipt).lazy()
        output = (
            lineitem.join(part, left_on="L_PARTKEY", right_on="P_PARTKEY")
            .filter(pl.col("L_SHIPDATE") >= datetime.datetime(1995, 9, 1))
            .filter(
                pl.col("L_SHIPDATE")
                < datetime.datetime(1995, 9, 1) + datetime.timedelta(days=30)
            )
            .select(
                [
                    (
                        100
                        * (
                            pl.when(pl.col("P_TYPE").str.starts_with("PROMO"))
                            .then(
                                pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))
                            )
                            .otherwise(0)
                        ).sum()
                        / (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).sum()
                    ).alias("PROMO_REVENUE")
                ]
            )
            .collect()
        )
        print(output)
        write_dataset(s3, output, 7)


@task(task_id=f"query_6")
def query_6():
    s3 = s3_file_system()
    with load_dataset(s3, "lineitem") as lineitem_ipt, load_dataset(
        s3, "orders"
    ) as orders_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        orders = pl.read_parquet(orders_ipt).lazy()
        output = (
            orders.join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
            .filter(pl.col("L_SHIPMODE").is_in(["MAIL", "SHIP"]))
            .filter(pl.col("L_COMMITDATE") < pl.col("L_RECEIPTDATE"))
            .filter(pl.col("L_RECEIPTDATE") >= datetime.datetime(1994, 1, 1))
            .filter(
                pl.col("L_RECEIPTDATE")
                < datetime.datetime(1994, 1, 1) + datetime.timedelta(days=365)
            )
            .group_by(["L_SHIPMODE"])
            .agg(
                [
                    (
                        pl.when(pl.col("O_ORDERPRIORITY").is_in(["1-URGENT", "2-HIGH"]))
                        .then(1)
                        .otherwise(0)
                    )
                    .sum()
                    .alias("HIGH_LINE_COUNT"),
                    (
                        pl.when(
                            pl.col("O_ORDERPRIORITY")
                            .is_in(["1-URGENT", "2-HIGH"])
                            .not_()
                        )
                        .then(1)
                        .otherwise(0)
                    )
                    .sum()
                    .alias("LOW_LINE_COUNT"),
                ]
            )
            .sort("L_SHIPMODE")
            .collect()
        )
        print(output)
        write_dataset(s3, output, 6)


@task(task_id=f"query_5")
def query_5():
    s3 = s3_file_system()
    with load_dataset(s3, "lineitem") as lineitem_ipt, load_dataset(
        s3, "orders"
    ) as orders_ipt, load_dataset(s3, "customer") as customer_ipt, load_dataset(
        s3, "nation"
    ) as nation_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        orders = pl.read_parquet(orders_ipt).lazy()
        customer = pl.read_parquet(customer_ipt).lazy()
        nation = pl.read_parquet(nation_ipt).lazy()
        output = (
            customer.join(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
            .join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
            .join(nation, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
            .filter(pl.col("O_ORDERDATE") >= datetime.datetime(1993, 10, 1))
            .filter(
                pl.col("O_ORDERDATE")
                < datetime.datetime(1993, 10, 1) + datetime.timedelta(days=90)
            )
            .filter(pl.col("L_RETURNFLAG") == "R")
            .group_by(
                [
                    "C_CUSTKEY",
                    "C_NAME",
                    "C_ACCTBAL",
                    "C_PHONE",
                    "N_NAME",
                    "C_ADDRESS",
                    "C_COMMENT",
                ]
            )
            .agg(
                [
                    (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                    .sum()
                    .alias("REVENUE")
                ]
            )
            .sort(["REVENUE"], descending=[True])
            .limit(20)
            .collect()
        )
        print(output)
        write_dataset(s3, output, 5)


@task(task_id=f"query_4")
def query_4():
    s3 = s3_file_system()
    with load_dataset(s3, "lineitem") as lineitem_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        output = (
            lineitem.filter(pl.col("L_SHIPDATE") >= datetime.datetime(1994, 1, 1))
            .filter(
                pl.col("L_SHIPDATE")
                < datetime.datetime(1994, 1, 1) + datetime.timedelta(days=365)
            )
            .filter((pl.col("L_DISCOUNT").is_between(0.06 - 0.01, 0.06 + 0.01)))
            .filter(pl.col("L_QUANTITY") < 24)
            .select(
                [(pl.col("L_EXTENDEDPRICE") * pl.col("L_DISCOUNT")).alias("REVENUE")]
            )
            .sum()
            .collect()
        )
        print(output)
        write_dataset(s3, output, 4)


@task(task_id=f"query_3")
def query_3():
    s3 = s3_file_system()
    with load_dataset(s3, "lineitem") as lineitem_ipt, load_dataset(
        s3, "orders"
    ) as orders_ipt, load_dataset(s3, "customer") as customer_ipt, load_dataset(
        s3, "supplier"
    ) as supplier_ipt, load_dataset(
        s3, "nation"
    ) as nation_ipt, load_dataset(
        s3, "region"
    ) as region_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        orders = pl.read_parquet(orders_ipt).lazy()
        customer = pl.read_parquet(customer_ipt).lazy()
        supplier = pl.read_parquet(supplier_ipt).lazy()
        nation = pl.read_parquet(nation_ipt).lazy()
        region = pl.read_parquet(region_ipt).lazy()
        output = (
            customer.join(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
            .join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
            .join(supplier, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
            .join(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
            .join(region, left_on="N_REGIONKEY", right_on="R_REGIONKEY")
            .filter(pl.col("R_NAME") == "ASIA")
            .filter(pl.col("O_ORDERDATE") >= datetime.datetime(1994, 1, 1))
            .filter(
                pl.col("O_ORDERDATE")
                < datetime.datetime(1994, 1, 1) + datetime.timedelta(days=365)
            )
            .group_by(["N_NAME"])
            .agg(
                [
                    (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                    .sum()
                    .alias("REVENUE")
                ]
            )
            .sort(["REVENUE"], descending=[True])
            .collect()
        )
        print(output)
        write_dataset(s3, output, 3)


@task(
    task_id=f"query_2",
    execution_timeout=datetime.timedelta(hours=2),
)
def query_2():
    s3 = s3_file_system()
    with load_dataset(s3, "lineitem") as lineitem_ipt, load_dataset(
        s3, "orders"
    ) as orders_ipt, load_dataset(s3, "customer") as customer_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        orders = pl.read_parquet(orders_ipt).lazy()
        customer = pl.read_parquet(customer_ipt).lazy()
        output = (
            lineitem.join(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
            .join(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
            .filter(pl.col("C_MKTSEGMENT") == "BUILDING")
            .filter(pl.col("O_ORDERDATE") < datetime.datetime(1995, 3, 15))
            .filter(pl.col("L_SHIPDATE") > datetime.datetime(1995, 3, 15))
            .group_by(["L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"])
            .agg(
                [
                    (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                    .sum()
                    .alias("REVENUE")
                ]
            )
            .sort(["REVENUE", "O_ORDERDATE"], descending=[True, False])
            .limit(10)
            .collect()
        )
        print(output)
        write_dataset(s3, output, 2)


@task(task_id=f"query_1")
def query_1():
    s3 = s3_file_system()
    with load_dataset(s3, "lineitem") as lineitem_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        output = (
            lineitem.filter(
                pl.col("L_SHIPDATE")
                <= datetime.datetime(1998, 12, 1) - datetime.timedelta(days=90)
            )
            .group_by(["L_RETURNFLAG", "L_LINESTATUS"])
            .agg(
                [
                    pl.col("L_QUANTITY").sum().alias("SUM_QTY"),
                    pl.col("L_EXTENDEDPRICE").sum().alias("SUM_BASE_PRICE"),
                    (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                    .sum()
                    .alias("SUM_DISC_PRICE"),
                    (
                        pl.col("L_EXTENDEDPRICE")
                        * (1 - pl.col("L_DISCOUNT"))
                        * (1 + pl.col("L_TAX"))
                    )
                    .sum()
                    .alias("SUM_CHARGE"),
                    pl.col("L_QUANTITY").mean().alias("AVG_QUANTITY"),
                    pl.col("L_EXTENDEDPRICE").mean().alias("AVG_PRICE"),
                    pl.col("L_DISCOUNT").mean().alias("AVG_DISC"),
                    pl.col("L_QUANTITY").count().alias("COUNT_ORDER"),
                ]
            )
            .sort(["L_RETURNFLAG", "L_LINESTATUS"])
            .collect()
        )
        print(output)
        write_dataset(s3, output, 1)


def load_dataset(s3, dataset_name):
    dataset_uri = "s3://{}/tpc-h/{}/raw/{}.parquet".format(
        BUCKET, SCALE_FACTOR, dataset_name
    )
    return s3.open(dataset_uri, mode="rb")


def write_dataset(s3, dataset, query_number):
    output_uri = "s3://{}/tpc-h/{}/output_airflow/query_{}.parquet".format(
        BUCKET, SCALE_FACTOR, query_number
    )
    with s3.open(output_uri, mode="wb") as output:
        print("Writing results to S3 output: {}".format(output_uri))
        dataset.write_parquet(output)


def s3_file_system():
    args = {
        "anon": False,
        # "endpoint_url": "http://minio:9000",
        "key": os.environ.get("AWS_ACCESS_KEY"),
        "secret": os.environ.get("AWS_SECRET_KEY"),
        "use_ssl": False,
        "cache_regions": False,
        "client_kwargs": {
            "region_name": os.environ.get("AWS_REGION"),
        },
    }
    return s3fs.S3FileSystem(**args)


with DAG(
    dag_id="tpc_h_{}".format(SCALE_FACTOR),
    schedule=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["benchmark"],
) as dag:
    (
        query_1()
        >> query_2()
        >> query_3()
        >> query_4()
        >> query_5()
        >> query_6()
        >> query_7()
        >> query_8()
        >> query_9()
    )
