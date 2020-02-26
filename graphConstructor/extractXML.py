from lxml import etree
from copy import deepcopy
import json
import gc

def extractRevision():
    events = ( 'start' , 'end' )
    context = etree.iterparse('../data/thwiki-20200201-pages-meta-history.xml', events=events) 

    revision = dict()
    contentRevision = dict()

    isInPage = False
    isInRevisions = False
    isInContributor = False

    for event, elem in context:
        if elem.tag == 'page':
            isInPage = not isInPage
            continue

        elif elem.tag == 'revision':
            isInRevisions = not isInRevisions
            if event == 'start':
                revision.clear()
            else:
                yield deepcopy(revision)
            continue

        elif elem.tag == 'contributor':
            isInContributor = not isInContributor
            continue

        if event == 'start':
            if isInContributor:
                if elem.tag == 'id':
                    revision['userID'] = elem.text
                else:
                    revision[elem.tag] = elem.text
            
            elif isInRevisions:
                if elem.tag == 'id':
                    revision['revID'] = elem.text
                elif elem.tag == 'text':
                    contentRevision['text'] = elem.text
                    contentRevision['revID'] = revision['revID']

                    yield deepcopy(contentRevision)
                    contentRevision.clear()
                else:
                    revision[elem.tag] = elem.text

            elif isInPage:
                if elem.tag == 'id':
                    revision['pageid'] = elem.text
                else:
                    revision[elem.tag] = elem.text
                

        elem.clear(keep_tail = True)
        
        while elem.getprevious() is not None:
            del elem.getparent()[0]

if __name__ == '__main__':
    generator = extractRevision()
    
    content = list()
    revision = list()

    for i in range(10):
        print(next(generator))

#    while True:
#        try:
#            result = next(generator)
#            if 'text' in result.keys():
#                content.append(result)
#            else:
#                revision.append(result) 
#        except StopIteration:
#            break
#    
#    with open('content.txt','w') as f:
#        json.dump(content,f)
#
#    with open('revision.txt','w') as f:
#        json.dump(revision,f) 
