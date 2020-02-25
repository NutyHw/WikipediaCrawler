from crawler import *
from multiprocessing import Pool, Manager

query = {
            'action' : 'query',
            'generator' : 'random',
            'grnnamespace' : '0',
            'grnlimit' : '10',
            'format' : 'json'
        }

manager = Manager()

visited = manager.list()

def getSeedPage(query):
    res = list()

    generator = wikiCrawler(query)
    r = next(generator)
    for pageid in r:
        res.append(r[pageid]['title'])

    return res

def queryLinks(title):

    res = list()

    query = {
                'action' : 'query',
                'generator' : 'links',
                'titles' : title,
                'gplnamespace' : '0',
                'gpllimit' : 'max',
                'format' : 'json'
            }

    generator = wikiCrawler(query)
    
    while True:
        try:
            r = next(generator)
            for pageid in r.keys():
                res.append(r[pageid]['title'])
        except StopIteration:
            break
    return res

def queryLinkshere(title):

    res = list()

    query = {
                'action' : 'query',
                'generator' : 'linkshere',
                'titles' : title,
                'glhnamespace' : '0',
                'glhlimit' : 'max',
                'format' : 'json'
            }

    generator = wikiCrawler(query)

    while True:
        try:
            r = next(generator)
            for pageid in r.keys():
                res.append(r[pageid]['title'])
        except StopIteration:
            break

    return res
    
def queryCategories(title):
    
    res = list()

    query = {
                'action' : 'query',
                'generator' : 'categories',
                'titles' : title,
                'gcllimit' : 'max',
                'format' : 'json'
            }

    generator = wikiCrawler(query)

    while True:
        try:
            r = next(generator)
            for pageid in r.keys():
                res.append(r[pageid]['title'])
        except StopIteration:
            break

    return res
    
def BFS(title, visited = visited ):

    curLevelQueue = list()
    nextLevelQueue = list()
    res = list()
    level = 0

    while len(res) < 10000:
        while title in visited:
            if len(curLevelQueue) == 0:
                if len(nextLevelQueue) == 0:
                    return None
                curLevelQueue = copy.deepcopy(nextLevelQueue)
                nextLevelQueue = list()
                level += 1
                logging.info('current level : %s',str(level)) 
                logging.info('number of titles to next level : %s',str(len(curLevelQueue)))

            title = curLevelQueue.pop(0)

        if len(res) % 1000 == 0:
            logging.info('visited node : %s',str(len(res)))

        visited.append(title)

        linksTo = queryLinks(title)
        linksHere = queryLinkshere(title)
        categories = queryCategories(title)

        res.append({
                    'title' : title,
                    'categories' : categories,
                    'linksTo' : linksTo,
                    'linksHere' : linksHere
                    })
        
        for edge in linksTo:
            if not edge in visited:
                nextLevelQueue.append(edge)

    logging.info('finish crawling')

    return res

if __name__ == '__main__':
    allTitles = getSeedPage(query)
    p = Pool(8)

    res = p.map(BFS,allTitles)
    res2 = list()

    for i in res:
        res2 += i

    with open('wikiGraph.txt','w') as f:
        json.dump(res2,f)

    pipeline(res2,'wikiGraph2')
