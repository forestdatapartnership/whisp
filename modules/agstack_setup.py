# functions for setting up session based on usr credentials

def start_agstack_session(email,password,user_registry_base,debug=False):
    """using session to store cookies that are persistent"""
    import requests
    session = requests.session()
    session.headers = headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    req_body = {'email': email, 'password': password}
    res = session.post(user_registry_base, json=req_body)
    if debug: print ("Cookies",session.cookies)
    if debug: print ("status code:", res.status_code)
    return session