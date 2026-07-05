"""TPC-H-derived benchmark — DuckDB.

Runs 9 TPC-H queries (spec Q1, Q3, Q5, Q6, Q10, Q12, Q14, Q16, Q18, exposed
here as query_1..query_9) sequentially against Parquet inputs read from S3.

This is NOT an audited TPC-H benchmark. See pipelines/README.md.

Config via environment:
  SCALE_FACTOR   e.g. "1", "10", "50", "100" (S3 prefix tpc-h/<sf>/raw/)
  S3_BUCKET      default "windmill"
  S3_ENDPOINT    e.g. "127.0.0.1:9000" for MinIO (omit for AWS)
  S3_USE_SSL     "true"/"false" (default true)
  S3_URL_STYLE   "path"/"vhost" (default "path" for MinIO)
  AWS_REGION / AWS_ACCESS_KEY / AWS_SECRET_KEY
  DUCKDB_VARIANT "memory" (pure in-memory; OOMs past RAM), "disk" (tight
                 memory_limit, frugal, spills hard), "spill" (generous
                 memory_limit + temp_directory) or "direct" (query the Parquet
                 directly via views with pushdown; idiomatic DuckDB-on-S3)
  DUCKDB_MEM_LIMIT  memory_limit for the disk/spill/direct variants (default "8GB")
  DUCKDB_TEMP    temp_directory for spilling (default ./duckdb_spill)
  BENCH_OUT      path to write per-query timing JSON
  WRITE_OUTPUT   "true" to COPY query results back to S3 (default "false")
"""
import duckdb
import json
import os
import time

BUCKET = os.environ.get("S3_BUCKET", "windmill")
SF = os.environ.get("SCALE_FACTOR", "1")
VARIANT = os.environ.get("DUCKDB_VARIANT", "memory")
WRITE_OUTPUT = os.environ.get("WRITE_OUTPUT", "false").lower() == "true"

TABLES = ["customer", "orders", "lineitem", "supplier",
          "part", "partsupp", "nation", "region"]


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = os.environ.get("S3_ENDPOINT")
    use_ssl = os.environ.get("S3_USE_SSL", "true").lower() == "true"
    url_style = os.environ.get("S3_URL_STYLE", "path")
    con.execute("SET s3_region=?", [os.environ.get("AWS_REGION", "us-east-1")])
    con.execute("SET s3_access_key_id=?", [os.environ.get("AWS_ACCESS_KEY", "")])
    con.execute("SET s3_secret_access_key=?", [os.environ.get("AWS_SECRET_KEY", "")])
    if endpoint:
        con.execute("SET s3_endpoint=?", [endpoint])
        con.execute("SET s3_url_style=?", [url_style])
        con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'}")
    con.execute(f"SET threads={os.cpu_count()}")
    # "disk" = tight memory_limit (frugal, spills hard); "spill" = generous
    # memory_limit + temp_directory (realistic: use most of RAM, spill only if
    # needed). "direct" = query the Parquet directly via views (projection/
    # filter pushdown, no upfront materialization) + spill safety — the
    # idiomatic DuckDB-on-S3 path. "memory" leaves both unset (pure in-memory).
    if VARIANT in ("disk", "spill", "direct"):
        temp = os.environ.get("DUCKDB_TEMP", "./duckdb_spill")
        os.makedirs(temp, exist_ok=True)
        con.execute("SET memory_limit=?", [os.environ.get("DUCKDB_MEM_LIMIT", "8GB")])
        con.execute("SET temp_directory=?", [temp])
        con.execute("SET preserve_insertion_order=false")
    return con


def load_tables(con):
    for t in TABLES:
        uri = f"s3://{BUCKET}/tpc-h/{SF}/raw/{t}.parquet"
        if VARIANT == "direct":
            # views over the Parquet: DuckDB reads only the columns/rows each
            # query needs (pushdown), no upfront full materialization.
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{uri}')")
        else:
            con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{uri}')")


def run_query(con, sql, n):
    if WRITE_OUTPUT:
        out = f"s3://{BUCKET}/tpc-h/{SF}/output-duckdb/query_{n}.parquet"
        con.execute(f"COPY ({sql}) TO '{out}' (FORMAT parquet)")
    else:
        con.execute(sql).fetchall()


QUERIES = {}


def q(n):
    def deco(f):
        QUERIES[n] = f()
        return f
    return deco


@q(1)  # TPC-H Q1
def _q1():
    return """
        select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc, count(*) as count_order
        from lineitem
        where l_shipdate <= (DATE '1998-12-01' - INTERVAL 1 DAY * 90)
        group by l_returnflag, l_linestatus
        order by l_returnflag, l_linestatus"""


@q(2)  # TPC-H Q3
def _q2():
    return """
        select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate, o_shippriority
        from customer, orders, lineitem
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey
            and l_orderkey = o_orderkey and o_orderdate < (DATE '1995-03-15')
            and l_shipdate > (DATE '1995-03-15')
        group by l_orderkey, o_orderdate, o_shippriority
        order by revenue desc, o_orderdate limit 10"""


@q(3)  # TPC-H Q5
def _q3():
    return """
        select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
        from customer, orders, lineitem, supplier, nation, region
        where c_custkey = o_custkey and l_orderkey = o_orderkey
            and l_suppkey = s_suppkey and c_nationkey = s_nationkey
            and s_nationkey = n_nationkey and n_regionkey = r_regionkey
            and r_name = 'ASIA' and o_orderdate >= (DATE '1994-01-01')
            and o_orderdate < (DATE '1994-01-01' + interval '1' year)
        group by n_name order by revenue desc"""


@q(4)  # TPC-H Q6
def _q4():
    return """
        select sum(l_extendedprice * l_discount) as revenue
        from lineitem
        where l_shipdate >= (DATE '1994-01-01')
            and l_shipdate < (DATE '1994-01-01' + INTERVAL '1' year)
            and l_discount between .06 - 0.01 and .06 + 0.01 and l_quantity < 24"""


@q(5)  # TPC-H Q10
def _q5():
    return """
        select c_custkey, c_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            c_acctbal, n_name, c_address, c_phone, c_comment
        from customer, orders, lineitem, nation
        where c_custkey = o_custkey and l_orderkey = o_orderkey
            and o_orderdate >= (DATE '1993-10-01')
            and o_orderdate < (DATE '1993-10-01' + INTERVAL '1' month * 3)
            and l_returnflag = 'R' and c_nationkey = n_nationkey
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        order by revenue desc limit 20"""


@q(6)  # TPC-H Q12
def _q6():
    return """
        select l_shipmode,
            sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH'
                then 1 else 0 end) as high_line_count,
            sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
                then 1 else 0 end) as low_line_count
        from orders, lineitem
        where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP')
            and l_commitdate < l_receiptdate and l_shipdate < l_commitdate
            and l_receiptdate >= (DATE '1994-01-01')
            and l_receiptdate < (DATE '1994-01-01' + INTERVAL '1' year)
        group by l_shipmode order by l_shipmode"""


@q(7)  # TPC-H Q14
def _q7():
    return """
        select 100.00 * sum(case when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount) else 0 end)
            / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        from lineitem, part
        where l_partkey = p_partkey and l_shipdate >= (DATE '1995-09-01')
            and l_shipdate < (DATE '1995-09-01' + INTERVAL '1' month)"""


@q(8)  # TPC-H Q16
def _q8():
    return """
        select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
        from partsupp, part
        where p_partkey = ps_partkey and p_brand <> 'Brand#45'
            and p_type not like 'MEDIUM POLISHED%'
            and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
            and ps_suppkey not in (
                select s_suppkey from supplier
                where s_comment like '%Customer%Complaints%')
        group by p_brand, p_type, p_size
        order by supplier_cnt desc, p_brand, p_type, p_size"""


@q(9)  # TPC-H Q18
def _q9():
    return """
        select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
            sum(l_quantity)
        from customer, orders, lineitem
        where o_orderkey in (
                select l_orderkey from lineitem
                group by l_orderkey having sum(l_quantity) > 300)
            and c_custkey = o_custkey and o_orderkey = l_orderkey
        group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        order by o_totalprice desc, o_orderdate limit 100"""


def main():
    timings = {}
    con = connect()
    t0 = time.time()
    load_tables(con)
    timings["load"] = time.time() - t0
    for n in range(1, 10):
        t = time.time()
        run_query(con, QUERIES[n], n)
        timings[f"query_{n}"] = time.time() - t
    con.close()
    timings["total_queries"] = sum(v for k, v in timings.items() if k.startswith("query_"))
    timings["total"] = time.time() - t0
    out = os.environ.get("BENCH_OUT")
    if out:
        with open(out, "w") as f:
            json.dump({"engine": "duckdb", "variant": VARIANT, "sf": SF,
                       "duckdb_version": duckdb.__version__, "timings": timings}, f)
    print(json.dumps(timings, indent=2))


if __name__ == "__main__":
    main()
