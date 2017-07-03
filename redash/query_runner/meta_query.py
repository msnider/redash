import json
import logging
import sqlite3
import sys
import re
import tempfile

from redash import models
from redash.query_runner import BaseSQLQueryRunner
from redash.query_runner import register

from redash.utils import JSONEncoder

logger = logging.getLogger(__name__)


class MetaQuery(BaseSQLQueryRunner):
    noop_query = "pragma quick_check"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
            }
        }

    @classmethod
    def type(cls):
        return "meta_query"

    def __init__(self, configuration):
        super(MetaQuery, self).__init__(configuration)

    def _get_tables(self, schema):
        results = models.Query.meta_queries()
        for result in results:
            if result.latest_query_data is not None:
                query_data = json.loads(result.latest_query_data.data)
                schema_name = "QUERY_%d" % result.id
                table_name = "%s: %s" % (schema_name, result.name)
                cols = list(map(lambda r: r['name'], query_data['columns']))
                schema[schema_name] = {'name': table_name, 'columns': cols}
        return schema.values()

    # Run the query on the SQLITE database, but if we're missing a table that represetns another query,
    # try to load that into a properly named table. This lazily relies on the exceptions thrown by 
    # sqlite instead of pre-loading the tables.
    def _run_query(self, cursor, query):
        qids = set()
        while True:
            try:
                logger.debug(" > Query = %s", query)
                cursor.execute(query)
                break
            except sqlite3.OperationalError as e:
                logger.warn(" > Operational Error: %s", e.message)
                match = re.match('\s*no such table: QUERY\_(\d+)\s*', e.message)
                if match:
                    qid = int(match.group(1))
                    if qid not in qids:
                        qids.add(qid)
                        table_name = "QUERY_%d" % qid
                        result = models.Query.get_by_id(qid)
                        query_data = json.loads(result.latest_query_data.data)
                        column_names = list(map(lambda r: r['name'], query_data['columns']))
                        column_names.sort()
                        # Create tables for the query data: CREATE TABLE QUERY_# (); ...
                        create_sql = "CREATE TABLE `%s` (`%s`)" % (table_name, '`, `'.join(column_names))
                        logger.debug(" > Create SQL: %s", create_sql)
                        cursor.execute(create_sql)
                        # Insert query results into a local sqlite database
                        inserts = []
                        for row in query_data['rows']:
                            new_insert = []
                            for col in column_names:
                                new_insert.append(row[col])
                            inserts.append(tuple(new_insert))
                        fillers = ','.join(map(lambda x: '?', xrange(len(column_names))))
                        cursor.executemany('INSERT INTO `%s` VALUES (%s)' % (table_name, fillers), inserts)
                else:
                    raise e

    # Runs the query on a new, temporary, private SQLITE database
    # Use a blank string with sqlite to allow flushing to disk
    # @see https://www.sqlite.org/inmemorydb.html
    def run_query(self, query, user):
        connection = sqlite3.connect('') # ':memory:'
        cursor = connection.cursor()

        try:
            self._run_query(cursor, query)

            if cursor.description is not None:
                columns = self.fetch_columns([(i[0], None) for i in cursor.description])
                rows = [dict(zip((c['name'] for c in columns), row)) for row in cursor]

                data = {'columns': columns, 'rows': rows}
                error = None
                json_data = json.dumps(data, cls=JSONEncoder)
            else:
                error = 'Query completed but it returned no data.'
                json_data = None
        except KeyboardInterrupt:
            connection.cancel()
            error = "Query cancelled by user."
            json_data = None
        except Exception as e:
            # handle unicode error message
            err_class = sys.exc_info()[1].__class__
            err_args = [arg.decode('utf-8') for arg in sys.exc_info()[1].args]
            unicode_err = err_class(*err_args)
            raise unicode_err, None, sys.exc_info()[2]
        finally:
            connection.close()
        return json_data, error

register(MetaQuery)