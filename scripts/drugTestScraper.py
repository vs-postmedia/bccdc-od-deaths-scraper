''' drugTestScraper.py '''
import io
import ssl
import json
import requests
import urllib.request

# bug fix for SSL self-signed cert error
ssl._create_default_https_context = ssl._create_unverified_context

### VARS ###
url = 'https://bccsudrugsense.herokuapp.com/_dash-update-component'


### FUNCTIONS ###

def fetchFile(url):
     # fetch the url
    req = urllib.request.Request(url)
    remote_file = urllib.request.urlopen(req).read()
    memory_file = io.BytesIO(remote_file)

    return memory_file

def init():
    resp = json.loads(requests.get(url).text)

    print(resp)
    # memory_file = fetchFile(url)

    # print(memory_file)


init()