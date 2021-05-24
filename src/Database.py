
from queue import Queue

from logging import exception
from Query import DatabaseQuery
import json
from typing import List
from mysql.connector.connection import MySQLConnection
from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector.cursor import CursorBase
import mysql
import time
import os
import uuid
import mysql.connector
import mysql.connector.pooling

from config import MYSQL_FAILED_QUERIES_CACHE
from threading import Thread
from multiprocessing import Process, pool
from threading import Lock
from helpers import wait_until, todict
l = Lock()
from config import DATABASES
sess = str(uuid.uuid4())

from threading import Semaphore
from queue import Queue

import mysql

mysql.connector.pooling.CNX_POOL_MAXSIZE  = 256

class MysqlDatabaseService:


    queries_queue: Queue
    last_ping: int
    mydb: MySQLConnection
    
    

    queries_execution_threads: pool.ThreadPool
    connection_pool_semaphore: Semaphore

    ####
    host: str
    user: str
    password: str
    database: str




    def __init__(self,  database_uname,) -> None:
        self.last_ping = 0
        self.mydb = None
        self.queries_queue = Queue()
        self.dbname = database_uname
        self.start_connection(database_uname,)
        
 

    def start_connection(self,database_uname):
        pool_size = DATABASES[database_uname]['POOL_SIZE']
        self.connection_pool_semaphore = Semaphore(pool_size)
        self.queries_execution_threads = pool.ThreadPool(pool_size)
        self.mydb = MySQLConnectionPool(
            host=DATABASES[database_uname]['HOST'],
            user=DATABASES[database_uname]['USER'],
            password=DATABASES[database_uname]['PASS'],
            database=DATABASES[database_uname]['DB'],
            pool_name=str(uuid.uuid4()),
            pool_reset_session=True,
            autocommit= True,
            pool_size=pool_size,
        )

        print('[DatabaseConnected]', DATABASES[database_uname]['HOST'])

    
    def get_connection(self):
        # By acquiring a semaphore first, we make sure that there will always be a free connection
        # Thus avoiding .mydb.get_conmnection() to raise an exception 
        

        pooled_connection =  self.mydb.get_connection()
        #pooled_connection.autocommit = True

        return pooled_connection


    def release_connection(self, pooled_connection):
        # Release the conenction back to the pool
        pooled_connection.close()
        

        # Signal that we have one connection available
        self.connection_pool_semaphore.release()
        print (f'Avail threads: {self.connection_pool_semaphore._value}\n')

    def put_query(self, query: DatabaseQuery):
        self.queries_queue.put(query)



    def make_cb_func(self,pooled_connection):
        def f(*args):
            self.release_connection(pooled_connection)
            #print('Releasing connection\n')
        return f
    def consume_queries(self):
        while True:
            #print('Attempting to get query\n')
            query = self.queries_queue.get(block=True) # Will block until a query is available



            self.connection_pool_semaphore.acquire(blocking=True)

            
            #pooled_connection = self.get_connection()  # Will block until a connection is available
            
            cb = lambda _: self.connection_pool_semaphore.release()# self.make_cb_func(pooled_connection)

            # Perform query(ies)

            self.queries_execution_threads.apply_async(
                self.execute_query,
                args=(query,), 
                # callback=cb, # Release connection back to the pool
                # error_callback=cb,
            )

            print(f'Executing query. Queue length: {self.queries_queue.qsize()}\n',)

            # self.execute_query(query,pooled_connection) 

    

    def execute_query(self, query_item):
        ########################
        with_transaction = False
        exception_occured = False
        return_data = []
        this_query_id = query_item.id
        fetch = query_item.fetch
        ########################

        conn = self.get_connection()
        try:
            if query_item.transactional:
                with_transaction = True
                #conn.start_transaction()
            while bool(query_item):
                cursor_results = []
                cursor_results_columns = []
                cursor: CursorBase = conn.cursor()
                #print(f'conn_autocommit={conn.get_autocommit()}')
                started = int(time.time() * 1000)

                try:
                    res = cursor.execute(str(query_item.query),self.prepare_values(query_item.values))
                    # if cursor.rowcount > 0:
                    if fetch:
                        try:
                            for result in cursor.stored_results():
                                cursor_results = result.fetchall()
                                cursor_results_columns = result.description
                        except:
                            pass
                    else:
                        a = 2
                    cursor.close()
                except Exception as ex:
                    exception_occured = True
                    self.on_query_failed(query_item, str(ex))
                    print (ex)
                    break
                finally:
                    print(f"{query_item.query[0:50]}. Elapsed: {started -int(time.time()*1000) }ms")

                if exception_occured and with_transaction:
                    #self.mydb.rollback()
                    break

                else:
                    if cursor_results:
                        try:
                            field_names = [i[0] for i in cursor_results_columns]
                            this_result = []
                            for it in cursor_results:
                                this_result.append(
                                    self.create_result_object(field_names, it))
                            return_data.append(this_result)

                        except Exception as ex:
                            pass

                query_item = query_item.next_query
            if not exception_occured:
                pass
                
        except Exception as ex:
            print(ex)

        self.release_connection(conn)

    def create_result_object(self, field_names, field_values):
        ret = {}
        for i in range(0, len(field_names)):
            ret[field_names[i]] = field_values[i]
        return ret

    def on_query_failed(self, query: DatabaseQuery, exception):
        print("!!![MYSQL] Query failed. Written to log")
        filename = str(int(time.time())) + '-' + str(uuid.uuid4()) + '.json'
        write_filepath = os.path.join(MYSQL_FAILED_QUERIES_CACHE, filename)
        #query.next_query = None
        tnow = int(time.time())
        write_query_obj = todict(query)
        write_query_obj['timestamp'] = tnow
        write_query_obj['pretty_timestamp'] = time.ctime()
        write_query_obj['exception'] = exception
        with open(write_filepath, 'w+') as f:
            f.write(json.dumps(write_query_obj))


    def prepare_values(self,vals):
        return tuple(vals)




