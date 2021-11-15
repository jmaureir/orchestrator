import json
import jsonpickle
import requests

from requests.exceptions import *

from ..exceptions import APIResponseError

class AbstractApiClient(object):
    def __init__(self, api_url = "http://127.0.0.1:8020"):
        self.api_url = api_url
        
    def get(self,uri, timeout=10):
        url = "%s%s" % (self.api_url,uri)
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                json_response = response.json()
                return json_response
            else:
                raise(APIResponseError(response.status_code))
                
        except ConnectionError as e:
            print("Connection Error. Technical details given below.\n")
            print(str(e))            
        except Timeout as e:
            print("Timeout Error")
            print(str(e))
        except RequestException as e:
            print("General Error")
            print(str(e))
        except KeyboardInterrupt as e:
            print("Someone closed the program")
        except Exception as e:
            print(str(e))
            raise e
            
    def post(self,uri,**kwargs):
        url = "%s%s" % (self.api_url,uri)
        try:
            response = requests.post(url,**kwargs)

            if response.status_code == 200:
                json_response = response.json()
                return json_response
            else:
                raise(APIResponseError(response.status_code))
        except ConnectionError as e:
            print("Connection Error. Technical details given below.\n")
            print(str(e))            
        except Timeout as e:
            print("Timeout Error")
            print(str(e))
        except RequestException as e:
            print("General Error")
            print(str(e))
        except KeyboardInterrupt as e:
            print("Someone closed the program")
        except Exception as e:
            print(str(e))
            raise e
    
