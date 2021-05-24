from threading import Thread
from Database import MysqlDatabaseService
from config import DATABASES
from multiprocessing import  Queue as QueriesQueue
from Query import DatabaseQuery

from multiprocessing.pool import ThreadPool

class MysqlPushService:
    databases = {}
    


    def __init__(self) -> None:
        self.start()

    def start(self):
        self.databases = {}
        self.thread_pool =  ThreadPool()

        for db in DATABASES:
            dbo = MysqlDatabaseService(db)
            self.databases[db] = dbo
            # Start consuming
            Thread(target=dbo.consume_queries).start()
        
    def delegate_query(self,query: DatabaseQuery):
        """Non blocking method to delegate query to their respective database"""
        database_name = query.database

        dbo:MysqlDatabaseService = self.databases[database_name]    

        dbo.put_query(query)

    def put_query(self,query: DatabaseQuery):
        # Assign query to the right databse queue
        
        database_uname = query.database
        #print (database_uname)

        dbo: MysqlDatabaseService = self.databases.get(database_uname)

        if not dbo:
            raise f"Database {database_uname} not found"

        print(f'Queue size: {dbo.queries_queue.qsize()}')
        dbo.put_query(query)


    