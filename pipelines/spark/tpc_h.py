from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.functions import sum, col, avg, count, when, countDistinct
import argparse
import os


BUCKET = "windmill"


def main(remote, scale, query_number):
    with SparkSession.builder.getOrCreate() as spark:
        if remote:
            connect_s3(spark)

        match query_number:
            case 1:
                lineitem = load_dataset(spark, remote, scale, "lineitem")
                output = query_1(lineitem)
            case 2:
                customer = load_dataset(spark, remote, scale, "customer")
                orders = load_dataset(spark, remote, scale, "orders")
                lineitem = load_dataset(spark, remote, scale, "lineitem")
                output = query_2(customer, orders, lineitem)
            case 3:
                customer = load_dataset(spark, remote, scale, "customer")
                orders = load_dataset(spark, remote, scale, "orders")
                lineitem = load_dataset(spark, remote, scale, "lineitem")
                supplier = load_dataset(spark, remote, scale, "supplier")
                nation = load_dataset(spark, remote, scale, "nation")
                region = load_dataset(spark, remote, scale, "region")
                output = query_3(customer, orders, lineitem, supplier, nation, region)
            case 4:
                lineitem = load_dataset(spark, remote, scale, "lineitem")
                output = query_4(lineitem)
            case 5:
                customer = load_dataset(spark, remote, scale, "customer")
                orders = load_dataset(spark, remote, scale, "orders")
                lineitem = load_dataset(spark, remote, scale, "lineitem")
                nation = load_dataset(spark, remote, scale, "nation")
                output = query_5(customer, orders, lineitem, nation)
            case 6:
                orders = load_dataset(spark, remote, scale, "orders")
                lineitem = load_dataset(spark, remote, scale, "lineitem")
                output = query_6(orders, lineitem)
            case 7:
                lineitem = load_dataset(spark, remote, scale, "lineitem")
                part = load_dataset(spark, remote, scale, "part")
                output = query_7(lineitem, part)
            case 8:
                partsupp = load_dataset(spark, remote, scale, "partsupp")
                part = load_dataset(spark, remote, scale, "part")
                supplier = load_dataset(spark, remote, scale, "supplier")
                output = query_8(partsupp, part, supplier)
            case 9:
                customer = load_dataset(spark, remote, scale, "customer")
                orders = load_dataset(spark, remote, scale, "orders")
                lineitem = load_dataset(spark, remote, scale, "lineitem")
                output = query_9(customer, orders, lineitem)

        output.show()
        if remote:
            write_dataset(output, scale, query_number)


def connect_s3(spark):
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY")
    )
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", os.environ.get("AWS_SECRET_KEY")
    )
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint",
        "s3.us-east-2.amazonaws.com",  # change it to the URL of your S3 server
    )


def load_dataset(spark, remote, scale, dataset_name_or_path):
    if remote:
        dataset_path = "s3a://{}/tpc-h/{}/input/{}.parquet".format(
            BUCKET, scale, dataset_name_or_path
        )
    else:
        dataset_path = "./data/{}.parquet".format(dataset_name_or_path)
    return spark.read.parquet(dataset_path)


def write_dataset(dataset, scale, query_number):
    output_path = "s3a://{}/tcp-h/{}/output-spark/query_{}.parquet".format(
        BUCKET, scale, query_number
    )
    dataset.write.parquet(output_path, mode="overwrite")


def query_9(customer, orders, lineitem):
    """
    select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)
    from
        customer,
        orders,
        lineitem
    where
        o_orderkey in (
            select
                l_orderkey
            from
                lineitem
            group by
                l_orderkey having
                    sum(l_quantity) > 300
        )
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
    group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
    order by
        o_totalprice desc,
        o_orderdate;
    limit 100;
    """
    return (
        customer.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
        .join(lineitem, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
        .join(
            lineitem.groupBy("L_ORDERKEY")
            .agg(sum(col("L_QUANTITY")).alias("SUM_QTY"))
            .filter(col("SUM_QTY") > 300)
            .alias("la"),
            orders.O_ORDERKEY == col("la.L_ORDERKEY"),
            how="inner",
        )
        .groupBy("C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE")
        .agg(sum(col("L_QUANTITY")).alias("SUM_QTY"))
        .sort(col("O_TOTALPRICE").desc(), col("O_ORDERDATE"))
        .limit(100)
    )


def query_8(partsupp, part, supplier):
    """
    select
        p_brand,
        p_type,
        p_size,
        count(distinct ps_suppkey) as supplier_cnt
    from
        partsupp,
        part
    where
        p_partkey = ps_partkey
        and p_brand <> 'Brand#45'
        and p_type not like 'MEDIUM POLISHED%'
        and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
        and ps_suppkey not in (
            select
                s_suppkey
            from
                supplier
            where
                s_comment like '%Customer%Complaints%'
        )
    group by
        p_brand,
        p_type,
        p_size
    order by
        supplier_cnt desc,
        p_brand,
        p_type,
        p_size;
    limit -1;
    """
    return (
        partsupp.join(part, partsupp.PS_PARTKEY == part.P_PARTKEY)
        .filter(col("P_BRAND") != "Brand#45")
        .filter(~col("P_TYPE").startswith("MEDIUM POLISHED"))
        .filter(col("P_SIZE").isin([49, 14, 23, 45, 19, 3, 36, 9]))
        .join(
            supplier.filter(col("S_COMMENT").contains("Customer")),
            partsupp.PS_SUPPKEY == supplier.S_SUPPKEY,
            how="left_anti",
        )
        .groupBy("P_BRAND", "P_TYPE", "P_SIZE")
        .agg(countDistinct("PS_SUPPKEY").alias("SUPPLIER_CNT"))
        .sort(col("SUPPLIER_CNT").desc(), col("P_BRAND"), col("P_TYPE"), col("P_SIZE"))
    )


def query_7(lineitem, part):
    """
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-09-01'
        and l_shipdate < date '1995-09-01' + interval '1' month;
    limit -1;
    """
    return (
        lineitem.join(part, lineitem.L_PARTKEY == part.P_PARTKEY)
        .filter(lineitem.L_SHIPDATE >= date(1995, 9, 1))
        .filter(lineitem.L_SHIPDATE < date(1995, 9, 1) + timedelta(days=30))
        .select(
            (
                100
                * sum(
                    when(
                        col("P_TYPE").startswith("PROMO"),
                        col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT")),
                    ).otherwise(0)
                )
                / sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT")))
            ).alias("PROMO_REVENUE")
        )
    )


def query_6(orders, lineitem):
    """
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('MAIL', 'SHIP')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '1994-01-01'
        and l_receiptdate < date '1994-01-01' + interval '1' year
    group by
        l_shipmode
    order by
        l_shipmode;
    limit -1;
    """
    return (
        orders.join(lineitem, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
        .filter(lineitem.L_SHIPMODE.isin(["MAIL", "SHIP"]))
        .filter(lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE)
        .filter(lineitem.L_RECEIPTDATE >= date(1994, 1, 1))
        .filter(lineitem.L_RECEIPTDATE < date(1994, 1, 1) + timedelta(days=365))
        .groupBy("L_SHIPMODE")
        .agg(
            sum(
                when(orders.O_ORDERPRIORITY.isin(["1-URGENT", "2-HIGH"]), 1).otherwise(
                    0
                )
            ).alias("HIGH_LINE_COUNT"),
            sum(
                when(~orders.O_ORDERPRIORITY.isin(["1-URGENT", "2-HIGH"]), 1).otherwise(
                    0
                )
            ).alias("LOW_LINE_COUNT"),
        )
        .sort("L_SHIPMODE")
    )


def query_5(customer, orders, lineitem, nation):
    """
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc;
    limit 20;
    """
    return (
        customer.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
        .join(lineitem, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
        .join(nation, customer.C_NATIONKEY == nation.N_NATIONKEY)
        .filter(orders.O_ORDERDATE >= date(1993, 10, 1))
        .filter(orders.O_ORDERDATE < date(1993, 10, 1) + timedelta(days=90))
        .filter(col("L_RETURNFLAG") == "R")
        .groupBy(
            "C_CUSTKEY",
            "C_NAME",
            "C_ACCTBAL",
            "C_PHONE",
            "N_NAME",
            "C_ADDRESS",
            "C_COMMENT",
        )
        .agg(sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("REVENUE"))
        .sort(col("REVENUE").desc())
        .limit(20)
    )


def query_4(lineitem):
    """
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1994-01-01' + interval '1' year
        and l_discount between .06 - 0.01 and .06 + 0.01
        and l_quantity < 24;
    limit -1;
    """
    return (
        lineitem.filter(lineitem.L_SHIPDATE >= date(1994, 1, 1))
        .filter(lineitem.L_SHIPDATE < date(1994, 1, 1) + timedelta(days=365))
        .filter(col("L_DISCOUNT").between(0.06 - 0.01, 0.06 + 0.01))
        .agg(sum(col("L_EXTENDEDPRICE") * col("L_DISCOUNT")).alias("REVENUE"))
    )


def query_3(customer, orders, lineitem, supplier, nation, region):
    """
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '1' year
    group by
        n_name
    order by
        revenue desc;
    limit -1;
    """
    return (
        customer.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
        .join(lineitem, orders.O_ORDERKEY == lineitem.L_ORDERKEY)
        .join(supplier, lineitem.L_SUPPKEY == supplier.S_SUPPKEY)
        .join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
        .join(region, nation.N_REGIONKEY == region.R_REGIONKEY)
        .filter(region.R_NAME == "ASIA")
        .filter(orders.O_ORDERDATE >= date(1994, 1, 1))
        .filter(orders.O_ORDERDATE < date(1994, 1, 1) + timedelta(days=365))
        .groupBy("N_NAME")
        .agg(sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("REVENUE"))
        .sort(col("REVENUE").desc())
    )


def query_2(customer, orders, lineitem):
    """
    select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    from
        customer,
        orders,
        lineitem
    where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date '1995-03-15'
        and l_shipdate > date '1995-03-15'
    group by
        l_orderkey,
        o_orderdate,
        o_shippriority
    order by
        revenue desc,
        o_orderdate;
    limit 10;
    """
    return (
        lineitem.join(orders, lineitem.L_ORDERKEY == orders.O_ORDERKEY)
        .join(customer, orders.O_CUSTKEY == customer.C_CUSTKEY)
        .filter(customer.C_MKTSEGMENT == "BUILDING")
        .filter(orders.O_ORDERDATE < date(1995, 3, 15))
        .filter(lineitem.L_SHIPDATE > date(1995, 3, 15))
        .groupBy("L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY")
        .agg(sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("REVENUE"))
        .sort(col("REVENUE").desc(), col("O_ORDERDATE"))
        .limit(10)
    )


def query_1(lineitem):
    """
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval '90' day (3)
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;
    limit -1;
    """
    return (
        lineitem.filter(lineitem.L_SHIPDATE < date(1998, 12, 1) - timedelta(days=90))
        .groupBy(["L_RETURNFLAG", "L_LINESTATUS"])
        .agg(
            sum("L_QUANTITY").alias("SUM_QTY"),
            sum("L_EXTENDEDPRICE").alias("SUM_BASE_PRICE"),
            sum(col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias(
                "SUM_DISC_PRICE"
            ),
            sum(
                col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT")) * (1 + col("L_TAX"))
            ).alias("SUM_CHARGE"),
            avg("L_QUANTITY").alias("AVG_QUANTITY"),
            avg("L_EXTENDEDPRICE").alias("AVG_PRICE"),
            avg("L_DISCOUNT").alias("AVG_DISC"),
            count("L_QUANTITY").alias("COUNT_ORDER"),
        )
        .sort("L_RETURNFLAG", "L_LINESTATUS")
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark queries")
    parser.add_argument("--remote", required=False, default="false", help="remote")
    parser.add_argument("--scale", required=False, default="1g", help="scale factor")
    parser.add_argument("--query", required=False, default="1", help="query number")
    args = parser.parse_args()

    main(args.remote.lower() == "true", args.scale, int(args.query))
