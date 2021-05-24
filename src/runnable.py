from Query import DatabaseQuery
from MysqlService import MysqlPushService
from SocketService import SocketService
from config import BIND_PORT


from multiprocessing.managers import BaseManager

from http_server import run


BaseManager.register('MysqlPushService', MysqlPushService)

processing_manager = BaseManager()
processing_manager.start()



sql_service: MysqlPushService = processing_manager.MysqlPushService()



def on_query_received(query_dict: dict):
    global sql_service
    query = DatabaseQuery(**query_dict) 

    x = query
    while x:
        if x.next_query:
            x.next_query = DatabaseQuery(**x.next_query) 
        x = x.next_query

    sql_service.put_query(query)

run(on_query_received, BIND_PORT)

