import databaseHandler as db
import riotAPIHandler as api
import time
import threading


def insertMatch(matchId):
    print(matchId)
    # check match
    response = api.getMatch(matchId, "euw", True)
    # if this Id belongs to a match
    if type(response) is dict:
        # insert it into the database
        t = threading.Thread(target=insertIntoDatabase, args=(response,))
        t.daemon = False
        t.start()
        # if the match was played in the last 2 hours wait a bit so we don't
        # overlook games which are currently still running
        if (response["matchCreation"] + 7200000) > int(time.time()*1000):
            print("We're getting kinda close to currently played games. Sleeping for 5 minutes")
            time.sleep(300)
    return True

def insertIntoDatabase(data):
    db.insertMatch(data)

# get the last match we collected
result = db.selectFetchOne("SELECT matchId FROM lol.match ORDER BY idmatch DESC")
# if no matches are collected yet use this Id as entry point
if result is None:
    matchId = 1792756886
else:
    matchId = result[0] + 1


while True:
    insertMatch(matchId)
    matchId += 1
