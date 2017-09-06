#!/usr/bin/env python
import argparse
import json
import logging
import re

from pyspark import SparkConf, SparkContext, HiveContext

import hdfs
from hive_ddl_parser import HiveDDLParser

logger = logging.getLogger(__name__)


def main():
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true", help="Enable Debugging")
    parser.add_argument('-db', '--database', required=True)  # hive database name
    parser.add_argument('-t', '--table', required=True)  # hive table name
    parser.add_argument('-p', '--partitions', required=True, type=json.loads)  # JSON parser

    args = parser.parse_args()

    # configure logging
    logging.basicConfig(format="%(asctime)s [%(name)s:%(lineno)d][%(funcName)s][%(levelname)s] %(message)s")

    # enable debugging
    if args.debug:
        logger.setLevel(logging.DEBUG)

    sc = SparkContext(conf=SparkConf().setAppName(__name__))
    sqlContext = HiveContext(sc)

    # parse ddl for table
    obj = HiveDDLParser(sc=sc, sqlContext=sqlContext, database=args.database, table=args.table)

    # parse hdfs path
    match = re.match('hdfs:\/\/(.*?)(\/.*)', obj.location)
    if match:
        hdfs_path = match.group(2)
    else:
        raise Exception("Cant parse hdfs path : {}", format(obj.location))

    partition_columns = [p[0] + "='{" + p[0] + "}'" for p in obj.partitioned_columns]
    sql = "ALTER TABLE {}.{} DROP IF EXISTS PARTITION ({})".format(args.database, args.table,
                                                                   ", ".join(partition_columns))
    path = hdfs_path + "/" + "/".join([p[0] + "={" + p[0] + "}" for p in obj.partitioned_columns])

    # loop parameter partitions
    for p in args.partitions:

        # Drop Partition
        logger.info("running sql : {}".format(sql.format(**p)))
        sqlContext.sql(sql.format(**p))

        # Check if partition hdfs folder exist and Delete
        logger.info("hdfs test -d : {}".format(path.format(**p)))
        if hdfs.direxists(path.format(**p)):
            logger.info("hdfs rm -r : {}".format(path.format(**p)))
            hdfs.rm('-r', path.format(**p))


if __name__ == "__main__":
    main()
