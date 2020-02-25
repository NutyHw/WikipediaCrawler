from main2 import pipeline
import pymongo
import json

res = list()

with open('revisions.txt') as f:
    res = json.load(f)

pipeline(res,'revisions')
