import databaseHandler as db
import riotAPIHandler as api
import time


def updateSummoner(epochTime):
    # get all summoners which haven't been checked within
    # 5 + ((levels away from 30) * 5) days
    # if the summoner was inactive for some time
    # instead only check every 1/4th of the time he was inactive
    # (e.g inactive for 4 months = check after a month)
    query = "SELECT * FROM summoner WHERE revisionDate > ({epochTime} - (({epochTime} - revisionDate) / 4)) and lastSummonerCheck <= NOW() - INTERVAL 5 + (ABS(summonerLevel - 30)*5) DAY".format(epochTime=epochTime)
    result = db.selectFetchAll(query)

    summonerIds = []
    # for each summoner we have to check
    for row in result:
        # add summonerId to the array
        # so we can check as many Ids with as little calls as possible
        # (40 at a time)
        summonerIds.append(row[1])
        if len(summonerIds) >= 40:
            # get json object from the Riot API
            result = api.getSummonerByIDs(summonerIds, "euw")
            # if the return value is a dictionary
            # and not a 404 or some other error code
            if type(result) is dict:
                # update the summoner entries in the database
                db.updateSummoner(result)
            summonerIds = []
    # check the remaining Ids after the loop
    if len(summonerIds) > 0:
        result = api.getSummonerByIDs(summonerIds, "euw")
        if type(result) is dict:
            db.updateSummoner(result)

while True:
    epochTime = int(time.time()*1000)  # epoch timestamp in milliseconds
    updateSummoner(epochTime)
    # update summoner once a day if it didn't take longer than a day to do so.
    sleepTime = 86400000 - (int(time.time()*1000) - epochTime)
    if sleepTime < 0:
        sleepTime = 0
    print(
        "That's it for now. Sleeping for {sleepTime} seconds".format(
            sleepTime=sleepTime
        ))
    # sleep the rest of the time
    time.sleep(sleepTime)
