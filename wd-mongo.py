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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", default=None, required=True)
    parser.add_argument("-c", "--collection", default=None, required=True)
    parser.add_argument("-d", "--database", default=None, required=True)
    args = parser.parse_args()
    client = MongoClient()
    database = client[args.database]
    collection = database[args.collection]
    site = pwb.Site('wikidata', 'wikidata')
    query = getQuery(args.query)
    startTime = datetime.now()
    generator = pg.WikidataSPARQLPageGenerator(query, site=site)
    for item in generator:
        try:
            jsonItem = {}
            item_dict = item.get()
            itemID = item.getID()
            labels = item_dict["labels"]
            descriptions = item_dict["descriptions"]
            claims = item_dict["claims"]
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
            jsonItem["_id"] = itemID
            jsonItem["labels"] = labels
            jsonItem["descriptions"] = descriptions
            jsonItem["claims"] = claimsClean
            if collection.find({"_id" : jsonItem["_id"]}).count() == 0:
                post_id = collection.insert_one(jsonItem).inserted_id
        except (pwb.IsRedirectPage, pwb.NoPage):
            pass

    print("{} elapsed".format(datetime.now() - startTime))
    print("Done! db has {} items".format(collection.count()))
if __name__ == "__main__":
    main()
