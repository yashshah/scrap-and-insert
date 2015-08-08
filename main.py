import urllib
import time
import re
import os
import elasticsearch
import eatiht.v2 as v2

appbase_app = "1"
appbase_app_username = "qHKbcf4M6"
appbase_app_password = "78a6cf0e-90dd-4e86-8243-33b459b5c7c5"

##Run the scriping wget script here or another file which calls wget and then this file
# es = elasticsearch.Elasticsearch()
es = elasticsearch.Elasticsearch('https://' + appbase_app_username + ':' appbase_app_password + '@scalr.api.appbase.io')
# Make sure that I'm calling to the Appbase elastic search using HTTP Authentication
# Creating the Mapping with the elastic search


# Going through all the files in a directory and extracting title
## Instead of going through the current directory, go through everything
for root, dirnames, filenames in os.walk('website-data'):
    for file_name in filenames:
        # if os.path.isfile(file_name) and "html" in file_name :
        if os.path.isfile(file_name):
            file_path = os.path.join(root, file_name)
            file = open(file_name, 'r')
            regex = re.compile('<title>(.*?)</title>', re.IGNORECASE|re.DOTALL)
            title = regex.search(file.read())
            if title:
                title = title.group(1)
                ## Check if I can replace the url with os.path or any other way
                try:
                    body = v2.extract(file_path)
                    ## Remove /n and all such characters from body
                    if body:
                        ## store link as the id and if error we check and then upsert
                        result = es.index(index='1', doc_type='article', body={
                        'body': body,
                        'title':title,
                        'link': "https://www.digitalocean.com/community/tutorials/"+ file_name
                        })
                    else:
                        print "Error at " + file_name
                    except:
                        print file_name
                        print "Unable to add it to Elastic Search"
