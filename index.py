from elasticsearch import Elasticsearch
import os, glob
from collections import defaultdict
host = 'https://elastic:gHvFwmRiqW3WAno3x6OUUObh@hurricanes.es.us-west1.gcp.cloud.es.io:9243'


es = Elasticsearch([host], timeout=3000)
# config = {
#         'host': '0.0.0.0'
#     }

# es = Elasticsearch([config, ])
STOPLIST = []
INDEX = 'hurricanes'
NUM_DOCS = 0
LENGTH_OF_ALL_DOCS = 0

def get_stoplist():
    stoplist_file = "/Users/tannu/Desktop/CS6200/homework1-sinhaut/config/stoplist.txt"
    with open(stoplist_file, 'r') as f:
        stoplist = f.read().splitlines()
    return stoplist

def create_index(esclient):
    stoplist = get_stoplist()
    request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "max_result_window" : 100000,
        "analysis": {
            "filter": {
                "english_stop": {
                    "type": "stop",
                    "stopwords": stoplist
                }
            },
            "analyzer": {
                "stopped": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "porter_stem"
                    ]
                }
            }
      }
    },
        "mappings": {
            "properties": {
                "id" : {
                    "type" : "text",
                },
                "text": {
                    "type": "text",
                    "fielddata": True,
                    "analyzer": "stopped",
                    "index_options": "positions"
                },
                "inlinks" :{
                    "type": "text"
                },
                "outlinks" : {
                    "type": "text"
                },
                "author": {
                    "type": "text"
                }
            }
        }
    }

    esclient.indices.create(index=INDEX, body=request_body)

"""def mergeDocs(myDoc):
    otherDoc = es.get(myDoc[id])
    docinfo = otherDoc['hits']['hits']['_source']
    otherText = docinfo['text']
    otherInlinks = docinfo['inlinks']
    otherOutlinks = docinfo['outlinks']
    otherAuthor = docinfo['author']

    myInlinks = myDoc['inlinks']
    myOutlinks = myDoc['outlinks']

    for each in myInlinks:
        if each not in otherInlinks:
            otherInlinks.append(each)

    for each in myOutlinks:
        if each not in otherOutlinks:
            otherOutlinks.append(each)

    newAuthor = otherAuthor + 'utkarshna'

    newRet = {
        'inlinks': otherInlinks,
        'outlinks': otherOutlinks,
        'author': newAuthor,
    }

    es.update(index='hurricanes', id=myDoc['id'], body=newRet)"""

def parse_content(content):
    global inlinks
    global outlinks
    while "<DOC>" in content:
        doc_end = content.find("<\DOC>")
        sub = content[:doc_end]
        
        docno_start = sub.find("<DOCNO>") + len("<DOCNO>")
        docno_end = sub.find("<\DOCNO>")
        doc_no = sub[docno_start:docno_end].strip()
        text = ""
        
        text_start = sub.find("<TEXT>") + len("<TEXT>")
        text_end = sub.find("<\TEXT>")
        text = text + sub[text_start: text_end].strip() + "\n"
        sub = sub[text_end + len("<\TEXT>"):]

        content = content[doc_end + len("<\DOC>"):]
        doc_text = text.lower()
        curr_inlinks = list(inlinks[doc_no])
        curr_outlinks = list(outlinks[doc_no])
        #print(doc_no, type(doc_text), type(curr_outlinks), type(curr_outlinks))
        ret = {
            "id": doc_no,
            "text": doc_text,
            "inlinks": curr_inlinks,
            "outlinks": curr_outlinks,
            "author": 'utkarshna'
        }
        
        # if es.exists(index=INDEX, id=doc_no):
        #     mergeDocs(ret)
        # else:
        es.index(index=INDEX, id=doc_no, body=ret)

def add_file_to_index(filename):
    with open(filename, "r") as f:
        content = f.read()
        parse_content(content)

def get_paths(data_path):
    return [f for f in glob.glob(data_path + "*")]

def get_links(links_list):
    links_dict = defaultdict(set)
    for links in links_list:
        all_links = links.split(', ')
        links_dict[all_links[0]] = all_links[1:]
    return links_dict


if __name__ == "__main__":
    if not es.ping():
        print("ITS NOT WORKING")
    else:
        print("working")
    
    outs_filepath= './pages_rawhtml/outlinks.csv'
    ins_filepath = './pages_rawhtml/inlinks.csv'

    with open(outs_filepath, 'r') as outlinks_file:
        outlinks_list = outlinks_file.readlines()

    with open(ins_filepath, 'r') as inlinks_file:
        inlinks_list = inlinks_file.readlines()

    inlinks = get_links(inlinks_list)
    outlinks = get_links(outlinks_list)
    #mport pdb; pdb.set_trace()
    if es.indices.exists(index=INDEX):
        es.indices.delete(index=INDEX)

    # Create the index
    create_index(es) 

    curr_dir = os.path.dirname(os.path.realpath( __file__ ))
    data_path = curr_dir + "/pages_rawhtml/webpages/"
    filePaths = get_paths(data_path)
    #import pdb; pdb.set_trace()
    # Add files to the hurricanes
    i = 0
    for f in filePaths:
        print(f)
        add_file_to_index(f)
        i += 1
        print(f"done:{i}")
        
