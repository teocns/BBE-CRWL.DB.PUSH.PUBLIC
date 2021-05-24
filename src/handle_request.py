import json
from run_scraper import run as handle_request
from Config import  HTTP_SERVICE_AUTHENTICATION_B64

def application(env, start_response):

    try:
        request_body_size = int(env.get('CONTENT_LENGTH', 0))
        
    except (ValueError):
        request_body_size = 0
    
    # Get authorization header or fail

    try:
        AUTH_STR = str(env.get('HTTP_AUTHORIZATION',''))
        print('Authstr;',AUTH_STR)
        if AUTH_STR !=  HTTP_SERVICE_AUTHENTICATION_B64:
            raise "nope"
    except:
        start_response('401 Unauthorized',
                       [('Content-Type', 'text/html'),
                       ('WWW-Authenticate', 'Basic realm="Login"')])
        return [b"Nope"]
        
    
    request_body = env['wsgi.input'].read(request_body_size)
    request_decoded = request_body.decode('utf-8')
    js = json.loads(request_decoded)
    response = handle_request(js)
    start_response('200 OK', [('Content-Type', 'application/json')])
    return [json.dumps(response).encode('utf-8')]