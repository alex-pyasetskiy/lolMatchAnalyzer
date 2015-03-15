import databaseHandler as db
import riotAPIHandler as api
import time
import threading


def updateSummonerRanks(epochTime):
    # get all summoner who are
    # - over level 30
    # - were active within the last two weeks
    # - were not checked within the last 2 days
    query = "SELECT * FROM summoner WHERE summonerLevel = 30 and lastRankCheck = DATE(\"0000-00-00\")"
    result = db.selectFetchAll(query)
    updateRanks(result)

    query = "SELECT * FROM summoner WHERE revisionDate > {epochTimeLimit} and summonerLevel = 30 and lastRankCheck <= NOW() - INTERVAL 3 DAY".format(epochTimeLimit=epochTime-1209600)
    result = db.selectFetchAll(query)
    updateRanks(result)


def updateRanks(result):
    summonerIDs = []
    for row in result:
        summonerIDs.append(row[1])
        if len(summonerIDs) >= 10:
            t = threading.Thread(target=doUpdate, args=(summonerIDs,))
            t.daemon = False
            t.start()
            summonerIDs = []
            time.sleep(2.5)
    if len(summonerIDs) > 0:
        t = threading.Thread(target=doUpdate, args=(summonerIDs,))
        t.daemon = False
        t.start()
        summonerIDs = []


def doUpdate(summonerIDs):
            response = api.getRankByIDs(summonerIDs, "euw")
            db.updateSummonerRankedDate(summonerIDs)

            if type(response) is dict:
                for key in sorted(response.items()):
                    print(key[0] + " " + response[key[0]][0]["tier"])
                db.updateSummonerRanks(response, summonerIDs)


while True:
    epochTime = int(time.time())*1000
    updateSummonerRanks(epochTime)
    # updateSummonerRanks once a day
    sleepTime = 86400000 - (int(time.time()*1000) - epochTime)
    if sleepTime < 0:
        sleepTime = 0
    print("Sleep for {sleepTime} seconds".format(sleepTime=sleepTime))
    time.sleep(sleepTime)
