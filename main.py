import urllib
import time
import re
import os
from elasticsearch import Elasticsearch
import eatiht.v2 as v2
from os import environ
from celery import Celery, task
from datetime import timedelta

# Fetch the Redis connection string from the env, or use localhost by default
REDIS_URL = environ.get('REDISTOGO_URL', 'redis://localhost')

# Setup the celery instance under the 'tasks' namespace
app = Celery('tasks')

# Use Redis as our broker and define json as the default serializer
app.conf.update(
    BROKER_URL=REDIS_URL,
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],
    CELERYBEAT_SCHEDULE = {
        'scrapandinsert': {
            'task': 'tasks.scrapinsert',
            'schedule': timedelta(hours=24)
        },
    }
)
# The periodic task itself, defined by the following decorator
@task
def scrapinsert():
    appbase_app = "3"
    appbase_doc_type = "article"
    appbase_app_username = "9Y5FRKQBx"
    appbase_app_password = "5134e787-fb21-4acd-8efa-a19663a9e08e"

    ## Ping Appbase that it has been installed and scrapping has started

    ##Run the scriping wget script here or another file which calls wget and then this file
    # es = Elasticsearch()
    # url = "@localhost:9200"
    body_settings = '{ "analysis": { "filter": { "nGram_filter": { "type": "nGram", "min_gram": 2, "max_gram": 20, "token_chars": [ "letter", "digit", "punctuation", "symbol" ] } }, "analyzer": { "nGram_analyzer": { "type": "custom", "tokenizer": "standard", "filter": [ "lowercase", "asciifolding", "nGram_filter" ] }, "body_analyzer": { "type": "custom", "tokenizer": "standard", "filter": [ "lowercase", "asciifolding", "stop", "snowball", "word_delimiter" ] }, "whitespace_analyzer": { "type": "custom", "tokenizer": "standard", "filter": [ "lowercase", "asciifolding" ] } } } }'

    body_mapping = '{ "article": { "properties": { "title": { "type": "string", "index_analyzer": "nGram_analyzer", "search_analyzer": "whitespace_analyzer" }, "link": { "type": "string", "index": "not_analyzed" }, "body": { "type": "string", "analyzer": "body_analyzer" } } } }'
    url = '@scalr.api.appbase.io'
    es = Elasticsearch('https://' + appbase_app_username + ':' + appbase_app_password + url)
    es.indices.close(index = appbase_app)
    es.indices.put_settings(index = appbase_app, body = body_settings)
    es.indices.open(index = appbase_app)

    print es.indices.put_mapping(index = appbase_app, body = body_mapping, doc_type = appbase_doc_type)

    # Make sure that I'm calling to the Appbase elastic search using HTTP Authentication
    # Creating the Mapping with the elastic search


    # Going through all the files in a directory and extracting title
    ## Instead of going through the current directory, go through everything
    for root, dirnames, filenames in os.walk('yourstory.com'):
        for file_name in filenames:
            file_path = os.path.join(root, file_name)
            # if os.path.isfile(file_name) and "html" in file_name :
            file = open(file_path, 'r')
            regex = re.compile('<title>(.*?)</title>', re.IGNORECASE|re.DOTALL)
            title = regex.search(file.read())
            if title:
                title = title.group(1)
                body = v2.extract("file://" + os.path.abspath(file_path))
                try:
                    ## Remove /n and all such characters from body
                    if body:
                        ## store link as the id and if error we check and then upsert
                        result = es.index(index= appbase_app, doc_type=appbase_doc_type, body={
                        'body': body,
                        'title':title,
                        'link': file_path
                        })
                    else:
                        print "Error at " + file_name
                except:
                    print file_name
                    print "Unable to add it to Elastic Search"
