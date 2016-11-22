import argparse
import pymongo
from pymongo import MongoClient
import pywikibot as pwb
from pywikibot import pagegenerators as pg
import json
from datetime import datetime


def getQuery(filename):
    with open(filename, 'r') as query_file:
        return query_file.read().replace('\n', '')

def prepareCollection(databaseName, collectionName):
    client = MongoClient()
    database = client[databaseName]
    return database[collectionName]

def createGenerator(queryFile, site):
    query = getQuery(queryFile)
    return pg.WikidataSPARQLPageGenerator(query, site=site)

def processArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", default=None, required=True)
    parser.add_argument("-c", "--collection", default=None, required=True)
    parser.add_argument("-d", "--database", default=None, required=True)
    return parser.parse_args()

def processClaims(claims):
    claimsClean = {}
    for claim in claims:
        claimList = []
        for x in claims[claim]:
            xTarget = x.getTarget()
            if (type(xTarget) == pwb.page.ItemPage or type(xTarget) == pwb.page.PropertyPage):
                targetClaim = xTarget.getID()
            elif (type(xTarget) == pwb.page.FilePage ):
                targetClaim = xTarget.fileUrl()
            elif(type(xTarget) == pwb.WbTime):
                targetClaim = xTarget.toTimestr()
            elif(type(xTarget) == pwb.Coordinate or type(xTarget) == pwb.WbQuantity or type(xTarget) == pwb.WbMonolingualText):
                targetClaim = xTarget.toWikibase()
            else:
                targetClaim = x.getTarget()
            claimList.append(targetClaim)
        claimsClean[claim] = claimList
    return claimsClean

def itemInCollection(itemID, collection):
    if collection.find({"_id" : itemID}).count() == 0:
        return False
    else:
        return True

def insertItem(jsonItem, collection):
    collection.insert_one(jsonItem)

def main():
    args = processArgs()
    collection = prepareCollection(args.database, args.collection)
    startTime = datetime.now()
    generator = createGenerator(args.query, pwb.Site('wikidata', 'wikidata'))
    for item in generator:
        try:
            item_dict = item.get()
            labels = item_dict["labels"]
            descriptions = item_dict["descriptions"]
            claims = item_dict["claims"]
            claimsClean = processClaims(claims)
            jsonItem = {}
            jsonItem["_id"] = item.getID()
            jsonItem["labels"] = labels
            jsonItem["descriptions"] = descriptions
            jsonItem["claims"] = claimsClean
            if itemInCollection(jsonItem["_id"], collection) == False:
                insertItem(jsonItem, collection)
        except (pwb.IsRedirectPage, pwb.NoPage):
            pass

    print("{} elapsed".format(datetime.now() - startTime))
    print("Done! db has {} items".format(collection.count()))

if __name__ == "__main__":
    main()
