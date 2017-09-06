#!/usr/bin/env python
import logging
import re

from pyspark import SparkConf, SparkContext, HiveContext

logger = logging.getLogger(__name__)


class HiveDDLParser(object):
    """Parse 'SHOW CREATE TABLE' into an python object. Supports spark 1.6 and 2.1"""

    def __init__(self, sc=None, sqlContext=None, database=None, table=None):

        # init spark variables
        self._sc = None
        self._sqlContext = None
        if sc is not None and sqlContext is not None:
            self._sc = sc
            self._sqlContext = sqlContext

        # init SparkContext if not already initialized
        if self._sc is None:
            logger.debug("initalizing SparkContext")
            self._sc = SparkContext(conf=SparkConf().setAppName("hive_ddl_parser"))
            self._sqlContext = HiveContext(self._sc)

        # issue show create table to hiveContext and return ddl dataframe
        ddl = self._sqlContext.sql("SHOW CREATE TABLE {}.{}".format(database, table))

        # coalese ddl dataframe results into ddl string and strip tild char
        ddl_string = ""
        for row in ddl.rdd.map(lambda r: (str(r[0]).replace("`", ""))).collect():
            ddl_string = ddl_string + "\n" + row

        # run regex on ddl string
        match = re.search(
            r'CREATE +?(TEMPORARY +)?(EXTERNAL +)?TABLE *(?P<db>.*?\.)?(?P<table>.*?)\((?P<col>.*?)\).(COMMENT.*?)?(PARTITIONED BY \((?P<pcol>.*?)\).)?(ROW FORMAT.*?)?(STORED AS.*?)?(LOCATION ?(?P<loc>.*))?(TBLPROPERTIES \(.*?\))',
            ddl_string, re.MULTILINE | re.DOTALL)

        # check for match
        if match:
            setattr(self, 'db', match.group("db")[:-1])  # set db
            setattr(self, 'table', match.group("table"))  # set table

            # parse columns
            cols_string = match.group("col").strip()
            cols = []
            sidx = 0
            while True:
                cols_string = cols_string[sidx:]
                idx = self.comma_search(cols_string)
                if idx == -1:  # last col
                    cols.append(cols_string.strip().split(' '))
                    break

                cols.append(cols_string[:idx].strip().split(' '))
                sidx = idx + 1
            setattr(self, 'columns', cols)  # set columns

            # parse partitioned_columns (optional)
            pcols = []
            if match.group("pcol"):
                pcols_string = match.group("pcol").strip()
                sidx = 0
                while True:
                    pcols_string = pcols_string[sidx:]
                    idx = self.comma_search(pcols_string)
                    if idx == -1:  # last col
                        pcols.append(pcols_string.strip().split(' '))
                        break

                    pcols.append(pcols_string[:idx].strip().split(' '))
                    sidx = idx + 1
            setattr(self, 'partitioned_columns', pcols)  # set partitioned_columns
            if len(pcols) == 0:
                setattr(self, 'is_partitioned', False)  # set is_partitioned False
            else:
                setattr(self, 'is_partitioned', True)  # set is_partitioned True
            setattr(self, 'location', match.group("loc").strip().replace("'", ""))  # set location
        else:
            raise Exception("Unable to parse SHOW CREATE TABLE DDL")

    def comma_search(self, data):
        """Search for comma in string that is not inside () and ''
        :param data: string to be searched
        :return: index (int
        """
        is_paren = False
        is_quote = False
        # loop string
        for i, c in enumerate(data):

            # look for opening (
            if c == '(':
                is_paren = True

            # look for closing )
            if c == ')':
                is_paren = False

            # looking for single quote
            if c == '\'':
                if is_quote:
                    is_quote = False
                else:
                    is_quote = True

            # looking for comma outside of () and ''
            if c == ',' and (not is_paren and not is_quote):
                return i

        return -1
