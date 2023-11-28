import os
import polars as pl
import s3fs
import datetime


def main():
    bucket = "windmill"
    args = {
        "anon": False,
        # "endpoint_url": "http://localhost:9050", # if not AWS default, for example if using MinIO
        "key": os.environ.get("AWS_ACCESS_KEY"),
        "secret": os.environ.get("AWS_SECRET_KEY"),
        "use_ssl": False,
        "cache_regions": False,
        "client_kwargs": {
            "region_name": os.environ.get("AWS_REGION"),
        },
    }
    s3 = s3fs.S3FileSystem(**args)
    scale_factor = "1g"

    orders_uri = "s3://{}/tpc-h/{}/raw/{}".format(
        bucket, scale_factor, "orders.parquet"
    )
    customer_uri = "s3://{}/tpc-h/{}/raw/{}".format(
        bucket, scale_factor, "customer.parquet"
    )
    lineitem_uri = "s3://{}/tpc-h/{}/raw/{}".format(
        bucket, scale_factor, "lineitem.parquet"
    )
    part_uri = "s3://{}/tpc-h/{}/raw/{}".format(bucket, scale_factor, "part.parquet")
    supplier_uri = "s3://{}/tpc-h/{}/raw/{}".format(
        bucket, scale_factor, "supplier.parquet"
    )
    partsupp_uri = "s3://{}/tpc-h/{}/raw/{}".format(
        bucket, scale_factor, "partsupp.parquet"
    )
    nation_uri = "s3://{}/tpc-h/{}/raw/{}".format(
        bucket, scale_factor, "nation.parquet"
    )
    region_uri = "s3://{}/tpc-h/{}/raw/{}".format(
        bucket, scale_factor, "region.parquet"
    )

    with s3.open(lineitem_uri, mode="rb") as lineitem_ipt, s3.open(
        part_uri, mode="rb"
    ) as part_ipt, s3.open(supplier_uri, mode="rb") as supplier_ipt, s3.open(
        partsupp_uri, mode="rb"
    ) as partsupp_ipt, s3.open(
        nation_uri, mode="rb"
    ) as nation_ipt, s3.open(
        region_uri, mode="rb"
    ) as region_ipt, s3.open(
        customer_uri, mode="rb"
    ) as customer_ipt, s3.open(
        orders_uri, mode="rb"
    ) as orders_ipt:
        lineitem = pl.read_parquet(lineitem_ipt).lazy()
        part = pl.read_parquet(part_ipt).lazy()
        supplier = pl.read_parquet(supplier_ipt).lazy()
        partsupp = pl.read_parquet(partsupp_ipt).lazy()
        nation = pl.read_parquet(nation_ipt)
        region = pl.read_parquet(region_ipt).lazy()
        customer = pl.read_parquet(customer_ipt).lazy()
        orders = pl.read_parquet(orders_ipt).lazy()

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_1"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_1(lineitem)
            output_df.write_parquet(output_file)

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_2"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_2(customer, orders, lineitem)
            output_df.write_parquet(output_file)

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_3"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_3(customer, orders, lineitem, supplier, nation, region)
            output_df.write_parquet(output_file)

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_4"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_4(lineitem)
            output_df.write_parquet(output_file)

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_5"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_5(customer, orders, lineitem, nation)
            output_df.write_parquet(output_file)

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_6"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_6(orders, lineitem)
            output_df.write_parquet(output_file)

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_7"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_7(lineitem, part)
            output_df.write_parquet(output_file)

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_8"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_8(partsupp, part, supplier)
            output_df.write_parquet(output_file)

        output_uri = "{}/{}/output-polars/{}.parquet".format(
            bucket, scale_factor, "query_9"
        )
        with s3.open(output_uri, mode="wb") as output_file:
            output_df = query_9(customer, orders, lineitem)
            output_df.write_parquet(output_file)

    return


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
        customer.join(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .group_by(["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"])
        .agg([pl.col("L_QUANTITY").sum().alias("SUM_QTY")])
        .collect()
        .filter(
            pl.col("O_ORDERKEY").is_in(
                lineitem.group_by("L_ORDERKEY")
                .agg([pl.col("L_QUANTITY").sum().alias("SUM_QTY")])
                .filter(pl.col("SUM_QTY") > 300)
                .select("L_ORDERKEY")
                .collect()
                .to_series()
            )
        )
        .sort(["O_TOTALPRICE", "O_ORDERDATE"], descending=[True, False])
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
        partsupp.join(part, left_on="PS_PARTKEY", right_on="P_PARTKEY")
        .filter(pl.col("P_BRAND") != "Brand#45")
        .filter(pl.col("P_TYPE").str.starts_with("MEDIUM POLISHED").not_())
        .filter(pl.col("P_SIZE").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .filter(
            pl.col("PS_SUPPKEY")
            .is_in(
                supplier.filter(
                    pl.col("S_COMMENT")
                    .str.contains("Customer")
                    .or_(pl.col("S_COMMENT").str.contains("Complaints"))
                )
                .select("S_SUPPKEY")
                .collect()
                .to_series()
            )
            .not_()
        )
        .group_by(["P_BRAND", "P_TYPE", "P_SIZE"])
        .agg([pl.col("PS_SUPPKEY").n_unique().alias("SUPPLIER_CNT")])
        .sort(
            ["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"],
            descending=[True, False, False, False],
        )
        .collect()
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
                        .then(pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                        .otherwise(0)
                    ).sum()
                    / (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).sum()
                ).alias("PROMO_REVENUE")
            ]
        )
        .collect()
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
                        pl.col("O_ORDERPRIORITY").is_in(["1-URGENT", "2-HIGH"]).not_()
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
        lineitem.filter(pl.col("L_SHIPDATE") >= datetime.datetime(1994, 1, 1))
        .filter(
            pl.col("L_SHIPDATE")
            < datetime.datetime(1994, 1, 1) + datetime.timedelta(days=365)
        )
        .filter((pl.col("L_DISCOUNT").is_between(0.06 - 0.01, 0.06 + 0.01)))
        .filter(pl.col("L_QUANTITY") < 24)
        .select([(pl.col("L_EXTENDEDPRICE") * pl.col("L_DISCOUNT")).alias("REVENUE")])
        .sum()
        .collect()
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


if __name__ == "__main__":
    main()
