import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from lxml import etree
from copy import deepcopy
import re
import json
import gc

logging.basicConfig(level = logging.INFO, filename = 'extractXML.log', format='%(name)s - %(levelname)s - %(message)s')

def parse_xml(path, maxlength):
    all_revisions = list()
    all_pages = list()

    context = etree.iterparse(path, events=('start','end',))
    context = iter(context)

    queue = list()
    revision = dict()
    page = dict()
    user= dict()

    for event, elem in context:
        try:
            if (len(all_revisions) + len(all_pages)) >= maxlength:
                yield ( all_pages, all_revisions )
                all_revisions.clear()
                all_pages.clear()

            # process element
            tag = re.sub(r'{.+}','',elem.tag)

            if event == 'start':
                queue.append(tag)

            elif event == 'end':
                if tag == 'contributor':
                    for key in user.keys():
                        revision[key] = user[key] 
                    user.clear()

                elif tag == 'revision':
                    if 'id' in page.keys():
                        revision['pageid'] = page['id']
                    if 'title' in page.keys():
                        revision['title'] = page['title']
                    revision.pop('contributor',None)
                    all_revisions.append(deepcopy(revision))
                    revision.clear()

                elif tag == 'page':
                    all_pages.append(deepcopy(page))
                    page.clear()

                queue.pop(-1)

            if len(queue) < 2:
                parent = None
            else:
                parent = queue[-2]

            if 'contributor' == parent:
                if tag in [ 'ip', 'username', 'id' ]:
                    if elem.text is not None:
                        user[tag] = elem.text
                
            if 'revision' == parent:
                if elem.text is not None:
                    revision[tag] = elem.text

            if 'page' == parent:
                if tag in [ 'id', 'ns', 'title' ]:
                    if elem.text is not None:
                        page[tag] = elem.text

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        except Exception as e:
            logging.error(e)
            logging.error(f'error while process {revision}')
            logging.error(f'error while process {page}')
            logging.error(f'error while process {user}')
    yield ( all_pages, all_revisions )

if __name__ == '__main__':
    path = '/d1/home/saito/Wikipedia_mining/data/rawdata/thwiki-20200201-pages-meta-history.xml'
    xml_generator = parse_xml(path, 500000) 

    # authenmongo
    load_dotenv(dotenv_path = '../.env') 
    username = os.getenv('mongo_username')
    password = os.getenv('mongo_password')
    db = os.getenv('db')
    
    client = MongoClient('localhost:27017',
                        username = username,
                        password = password,
                        authSource = 'wikipedia'
                        )
    pages_col = client.wikipedia.all_pages
    revs_col = client.wikipedia.all_revisions

    count = 0
    while True:
        try:
            pages, revs = next(xml_generator)
            pages = [ dict(t) for t in {tuple(d.items()) for d in pages} ]
            revs = [ dict(t) for t in {tuple(d.items()) for d in revs} ]
            count += len(pages)
            logging.info(f'extract {count} title')
            pages_col.insert_many(pages)
            revs_col.insert_many(revs)
            logging.info(f'insert in db success')
            del pages
            del revs
        except StopIteration:
            break
