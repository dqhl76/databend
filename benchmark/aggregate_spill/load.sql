-- Loads lineitem from the Parquet files of tpchgen-cli. run.py replaces {tpch_dir} with their
-- directory.
CREATE OR REPLACE STAGE aggregate_spill_tpch URL = 'fs://{tpch_dir}/';

CREATE OR REPLACE TABLE lineitem_sf8 (
    l_orderkey BIGINT NOT NULL,
    l_partkey BIGINT NOT NULL,
    l_suppkey BIGINT NOT NULL,
    l_linenumber INT NOT NULL,
    l_quantity DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount DECIMAL(15, 2) NOT NULL,
    l_tax DECIMAL(15, 2) NOT NULL,
    l_returnflag VARCHAR NOT NULL,
    l_linestatus VARCHAR NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR NOT NULL,
    l_shipmode VARCHAR NOT NULL,
    l_comment VARCHAR NOT NULL
);

COPY INTO lineitem_sf8 FROM @aggregate_spill_tpch/lineitem/
    PATTERN = '.*[.]parquet'
    FILE_FORMAT = (TYPE = PARQUET);

DROP STAGE aggregate_spill_tpch;
