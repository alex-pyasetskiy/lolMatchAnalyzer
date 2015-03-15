import databaseHandler as db
import riotAPIHandler as api
import time


def getNewSummoner():
    error = 0

    # get the currently highest summonerId from the database
    summonerIDPointer = db.selectFetchOne("SELECT summonerId FROM summoner ORDER BY summonerId DESC")
    if summonerIDPointer is None:
        summonerIDPointer = 0
    else:
        summonerIDPointer = summonerIDPointer[0] + 1
    print(summonerIDPointer)

    while True:
        # check the next 40 ids
        summonerIDs = []
        for i in range(summonerIDPointer, summonerIDPointer + 39):
            summonerIDs.append(i)
        response = api.getSummonerByIDs(summonerIDs, "euw")
        if type(response) is dict:
            error = 0
            db.insertSummoner(response)
        else:
            error += 1

        # if there are more than 400 empty ids it's fair to assume this was the last one
        if error >= 20:
            print("50 Errors in a row... fairly sure that's all for now.")
            break

        summonerIDPointer += 40

# check for new Ids once a day
while True:
    epochTime = int(time.time()*1000)
    getNewSummoner()
    sleepTime = 86400000 - (int(time.time()*1000) - epochTime)
