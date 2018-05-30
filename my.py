import json
import logging
import sys
from argparse import ArgumentParser
from time import time
import pymongo
from log import logger

from extract import extract_database_schema



def extract_schema_for_table_define(host,port,database_names=None,collection_names=None):
    """ Main entry point function to extract schema."""
    start_time = time()
    logger.info('=== Start MongoDB schema analysis')
    client = pymongo.MongoClient(host=host, port=port)

    """ Extract the schema for every database in database_names

        :param pymongo_client: pymongo.mongo_client.MongoClient
        :param database_names: str, list of str, default None
        :param collection_names: str, list of str, default None
            Will be used for every database in database_names list
        :return mongo_schema: dict
        """
    mongo_schema = extract_pymongo_client_schema_for_table_define(client,
                                                 database_names=database_names,
                                                 collection_names=collection_names)

    logger.info('--- MongoDB schema analysis took %.2f s', time() - start_time)
    return mongo_schema

from past.builtins import basestring
def extract_pymongo_client_schema_for_table_define(pymongo_client, database_names=None, collection_names=None):
    """ Extract the schema for every database in database_names

    :param pymongo_client: pymongo.mongo_client.MongoClient
    :param database_names: str, list of str, default None
    :param collection_names: str, list of str, default None
        Will be used for every database in database_names list
    :return mongo_schema: dict
    """



    if isinstance(database_names, basestring):
        database_names = [database_names]

    if database_names is None:
        database_names = pymongo_client.database_names()
        database_names.remove('admin')
        database_names.remove('local')

    mongo_schema = []
    for database in database_names:
        logger.info('Extract schema of database %s', database)
        pymongo_database = pymongo_client[database]
        database_schema = extract_database_schema(pymongo_database, collection_names)
        if database_schema:  # Do not add a schema if it is empty
            for key in database_schema:
                if key != database:
                    # print(database_schema[key])
                    newDict = format_table_define(key,database_schema[key])
                    # logger.info('------------------------')
                    # logger.info(json.dumps(newDict,indent=1))
                    mongo_schema.append(newDict)

    return mongo_schema

from constant import CONSTANT_dict

def format_table_define(table_name, table_dict):
    table_define = dict()
    table_define['table'] = table_name
    table_define['fields'] = []
    for key in table_dict['object']:
        if key != '_id':
            col_define = dict()
            col_define['name'] = key
            col_define['type'] = table_dict['object'][key]['type']
            # s.add(col_define['type'])
            col_define['type'] = CONSTANT_dict[table_dict['object'][key]['type']]
            col_define['hidden'] = False
            table_define['fields'].append(col_define)

    print(s)
    return table_define

host = 'mongodb://rw:rw@10.112.20.44/testdb'
# host = 'mongodb://xbomdata:oiisda89ads220adw144ffcnn736@10.112.16.191/nio_integration_tool'
s = set()
# host = 'mongodb://test_db:test_db@10.112.16.88/testdb'
#host里指定了用户名密码和db
#获取所有的collection
port = 27017
list = extract_schema_for_table_define(host,port,database_names = 'testdb')
for i in list:
    print(json.dumps(i,indent=1))
# print(list)
import json
from myconn import MongoConn
my_conn = MongoConn()
my_conn.db['test_schema'].insert(list)


print(s)

