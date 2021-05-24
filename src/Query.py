from typing import Callable
from uuid import uuid4


class DatabaseQuery:
    query: str
    values: tuple
    transactional: bool
    # If is set, this query will be considered transactional
    next_query: 'DatabaseQuery'
    id: str
    fetch: bool

    database: str
    def __init__(self, database:str, query, values=None, next_query=None, fetch=False, transactional = False, id= None) -> None:
        self.query = query
        self.values = values
        self.transactional = bool(next_query)
        self.next_query = next_query
        self.fetch = fetch
        self.id = str(uuid4()) if not id else id
        self.database = database
        self.transactional = transactional

    def put_next_query(self, query):
        self.next_query = query
        self.transactional = True

    def cache_to_file():
        raise NotImplementedError("Must implement")
        pass


    @staticmethod
    def from_dict( dictionary:dict):
        query = DatabaseQuery("","")
        for k, v in dictionary.items():
            if k == 'next_query' and v:
                query.next_query = DatabaseQuery.from_dict(v)
            else:
                setattr(query, k, v)
        return query