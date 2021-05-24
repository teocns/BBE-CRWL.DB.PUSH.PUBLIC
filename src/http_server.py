from PooledProcessMixin import PooledProcessMixIn
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from config import HTTP_SERVICE_AUTHENTICATION_B64
import threading
import json
from time import sleep
from typing import Callable, Tuple
import socketserver

class Handler(BaseHTTPRequestHandler):
    handle_request: Callable
    def __init__(self, *args, **kwargs):
        self.handle_request = kwargs['handle_request']
        del kwargs['handle_request']
        super(Handler,self).__init__(*args, **kwargs)

    def do_POST(self):
        try:
            length = int(self.headers.get('content-length'))

            b64_auth = self.headers.get('authorization')

            if b64_auth != HTTP_SERVICE_AUTHENTICATION_B64:
                self.send_response(401)
                self.end_headers()
                return
            
            

            js = json.loads(self.rfile.read(length))
            #self.rfile.read(length)
            #print(js)
            response = self.handle_request(js)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(response).encode('utf-8'))
        except Exception as ex:
            print(ex)
            self.send_response(503)
            self.end_headers()
            pass
    
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        


class ThreadingSimpleServer(PooledProcessMixIn, HTTPServer):
    
    def __init__(self,bind_addr, handler, processes, threads) -> None:
        self._process_n=processes  # if not set will default to number of CPU cores
        self._thread_n=threads  # if not set will default to number of threads
        HTTPServer.__init__(self, bind_addr, handler)
        #self._init_pool() # this is optional, will be called automatically






def run(handle_request, port, processes = 2, threads = 4):
    def create_handler_instance(*args, **kwargs):
        nonlocal handle_request
        kwargs['handle_request'] = handle_request
        return Handler(*args,**kwargs)

    
    server = ThreadingSimpleServer(('0.0.0.0', port), create_handler_instance, processes, threads )
    
    server.serve_forever()

