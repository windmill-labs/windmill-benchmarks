import duckdb
import os

BUCKET = "windmill"


def main():
    conn = duckdb.connect()  # in memory DB
    # conn = duckdb.connect("./home/duck.db") # this can be used to persist DB data to a file on disk

    # This needs to be run to connect DuckDB to the S3 bucket
    conn.execute(
        """
        SET home_directory='./home/';
        INSTALL 'httpfs';
        LOAD 'httpfs';
        SET s3_region='{}';
        SET s3_access_key_id='{}';
        SET s3_secret_access_key='{}';
    """.format(
            os.environ.get("S3_REGION"),
            os.environ.get("AWS_ACCESS_KEY"),
            os.environ.get("AWS_SECRET_KEY"),
        )
    )

    scale_factor = "1g"
    load_table_from_parquet(conn, BUCKET, scale_factor, "customer")
    load_table_from_parquet(conn, BUCKET, scale_factor, "orders")
    load_table_from_parquet(conn, BUCKET, scale_factor, "lineitem")
    load_table_from_parquet(conn, BUCKET, scale_factor, "supplier")
    load_table_from_parquet(conn, BUCKET, scale_factor, "part")
    load_table_from_parquet(conn, BUCKET, scale_factor, "partsupp")
    load_table_from_parquet(conn, BUCKET, scale_factor, "nation")
    load_table_from_parquet(conn, BUCKET, scale_factor, "region")

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_1"
    )
    query_1(conn, output_filename)

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_2"
    )
    query_2(conn, output_filename)

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_3"
    )
    query_3(conn, output_filename)

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_4"
    )
    query_4(conn, output_filename)

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_5"
    )
    query_5(conn, output_filename)

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_6"
    )
    query_6(conn, output_filename)

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_7"
    )
    query_7(conn, output_filename)

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_8"
    )
    query_8(conn, output_filename)

    output_filename = "{}/{}/output-duckdb/{}.parquet".format(
        BUCKET, scale_factor, "query_9"
    )
    query_9(conn, output_filename)

    conn.close()

    init_sql_schema(conn)


def query_9(conn, output_filename):
    conn.execute(
        """
        COPY (
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
                o_orderdate
            limit 100
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def query_8(conn, output_filename):
    conn.execute(
        """
        COPY (
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
                p_size
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def query_7(conn, output_filename):
    conn.execute(
        """
        COPY (
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
                and l_shipdate >= (DATE '1995-09-01')
                and l_shipdate < (DATE '1995-09-01' + INTERVAL '1' month)
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def query_6(conn, output_filename):
    conn.execute(
        """
        COPY (
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
                and l_receiptdate >= (DATE '1994-01-01')
                and l_receiptdate < (DATE '1994-01-01' + INTERVAL '1' year)
            group by
                l_shipmode
            order by
                l_shipmode
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def query_5(conn, output_filename):
    conn.execute(
        """
        COPY (
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
                and o_orderdate >= (DATE '1993-10-01')
                and o_orderdate < (DATE '1993-10-01' + INTERVAL '1' month * 3)
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
                revenue desc
            limit 20
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def query_4(conn, output_filename):
    conn.execute(
        """
        COPY (
            select
        sum(l_extendedprice * l_discount) as revenue
        from
            lineitem
        where
            l_shipdate >= (DATE '1994-01-01')
            and l_shipdate < (DATE '1994-01-01' + INTERVAL '1' year)
            and l_discount between .06 - 0.01 and .06 + 0.01
            and l_quantity < 24
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def query_3(conn, output_filename):
    conn.execute(
        """
        COPY (
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
                and o_orderdate >= (DATE '1994-01-01')
                and o_orderdate < (DATE '1994-01-01' + interval '1' year)
            group by
                n_name
            order by
                revenue desc
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def query_2(conn, output_filename):
    conn.execute(
        """
        COPY (
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
                and o_orderdate < (DATE '1995-03-15')
                and l_shipdate > (DATE '1995-03-15')
            group by
                l_orderkey,
                o_orderdate,
                o_shippriority
            order by
                revenue desc,
                o_orderdate
            limit 10
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def query_1(conn, output_filename):
    conn.execute(
        """
        COPY (
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
                l_shipdate <= (DATE '1998-12-01' - INTERVAL 1 DAY * 90)
            group by
                l_returnflag,
                l_linestatus
            order by
                l_returnflag,
                l_linestatus
        ) TO 's3://{}/{}' (FORMAT 'parquet');
    """.format(
            BUCKET, output_filename
        )
    )


def load_table_from_parquet(conn, bucket, scale_factor, table_name):
    customer_uri = "s3://{}/tpc-h/{}/raw/{}.parquet".format(
        bucket, scale_factor, table_name
    )
    conn.execute(
        """
        INSERT INTO {} (SELECT * FROM read_parquet('{}'));
    """.format(
            table_name, customer_uri
        )
    )


def init_sql_schema(duckdb_conn):
    # copied straight from dss.dll from the TPC-H repo
    duckdb_conn.execute(
        """
        CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));

        CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                                    R_NAME       CHAR(25) NOT NULL,
                                    R_COMMENT    VARCHAR(152));

        CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                                P_NAME        VARCHAR(55) NOT NULL,
                                P_MFGR        CHAR(25) NOT NULL,
                                P_BRAND       CHAR(10) NOT NULL,
                                P_TYPE        VARCHAR(25) NOT NULL,
                                P_SIZE        INTEGER NOT NULL,
                                P_CONTAINER   CHAR(10) NOT NULL,
                                P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                                P_COMMENT     VARCHAR(23) NOT NULL );

        CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                                    S_NAME        CHAR(25) NOT NULL,
                                    S_ADDRESS     VARCHAR(40) NOT NULL,
                                    S_NATIONKEY   INTEGER NOT NULL,
                                    S_PHONE       CHAR(15) NOT NULL,
                                    S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                                    S_COMMENT     VARCHAR(101) NOT NULL);

        CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                                    PS_SUPPKEY     INTEGER NOT NULL,
                                    PS_AVAILQTY    INTEGER NOT NULL,
                                    PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                                    PS_COMMENT     VARCHAR(199) NOT NULL );

        CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                                    C_NAME        VARCHAR(25) NOT NULL,
                                    C_ADDRESS     VARCHAR(40) NOT NULL,
                                    C_NATIONKEY   INTEGER NOT NULL,
                                    C_PHONE       CHAR(15) NOT NULL,
                                    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                                    C_MKTSEGMENT  CHAR(10) NOT NULL,
                                    C_COMMENT     VARCHAR(117) NOT NULL);

        CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                                O_CUSTKEY        INTEGER NOT NULL,
                                O_ORDERSTATUS    CHAR(1) NOT NULL,
                                O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                                O_ORDERDATE      DATE NOT NULL,
                                O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                                O_CLERK          CHAR(15) NOT NULL, 
                                O_SHIPPRIORITY   INTEGER NOT NULL,
                                O_COMMENT        VARCHAR(79) NOT NULL);

        CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                                    L_PARTKEY     INTEGER NOT NULL,
                                    L_SUPPKEY     INTEGER NOT NULL,
                                    L_LINENUMBER  INTEGER NOT NULL,
                                    L_QUANTITY    DECIMAL(15,2) NOT NULL,
                                    L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                                    L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                                    L_TAX         DECIMAL(15,2) NOT NULL,
                                    L_RETURNFLAG  CHAR(1) NOT NULL,
                                    L_LINESTATUS  CHAR(1) NOT NULL,
                                    L_SHIPDATE    DATE NOT NULL,
                                    L_COMMITDATE  DATE NOT NULL,
                                    L_RECEIPTDATE DATE NOT NULL,
                                    L_SHIPINSTRUCT CHAR(25) NOT NULL,
                                    L_SHIPMODE     CHAR(10) NOT NULL,
                                    L_COMMENT      VARCHAR(44) NOT NULL);
    """
    )
    return


if __name__ == "__main__":
    main()
