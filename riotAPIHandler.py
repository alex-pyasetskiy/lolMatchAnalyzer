import requests
import json
import time
import sys

matchID = 1852267005
region = "euw"
timelineData = "true"

apiKey = "c05f9108-ff11-4fa6-a6b2-bdccba1b5fea"


def getMatch(matchID, region, timelineData):
    query = (
        'https://{region}.api.pvp.net/api/lol/{region}/v2.2/match/{matchID}?includeTimeline={timelineData}&api_key={apiKey}'
        ).format(
        matchID=matchID,
        region=region,
        timelineData=timelineData,
        apiKey=apiKey
        )
    response = apiCall(query)
    return response


def getSummonerByNames(summonerNames, region):
    inputString = summonerNames[0]
    for i in range(1, len(summonerNames)):
        inputString += "," + summonerNames[i]
    query = (
        'https://{region}.api.pvp.net/api/lol/{region}/v1.4/summoner/by-name/{inputString}?api_key={apiKey}'
        ).format(
        inputString=inputString,
        region=region,
        apiKey=apiKey)
    response = apiCall(query)
    return response


def getSummonerByIDs(summonerIDs, region):
    inputString = str(summonerIDs[0])
    for i in range(1, len(summonerIDs)):
        inputString += ","+str(summonerIDs[i])
    query = (
        'https://{region}.api.pvp.net/api/lol/{region}/v1.4/summoner/{inputString}?api_key={apiKey}'
        ).format(
        inputString=inputString,
        region=region,
        apiKey=apiKey)
    response = apiCall(query)
    return response


def getRankByIDs(summonerIDs, region):
    inputString = str(summonerIDs[0])
    for i in range(1, len(summonerIDs)):
        inputString += ","+str(summonerIDs[i])
    query = (
        'https://{region}.api.pvp.net/api/lol/{region}/v2.5/league/by-summoner/{inputString}/entry?api_key={apiKey}'
        ).format(
        inputString=inputString,
        region=region,
        apiKey=apiKey)
    response = apiCall(query)
    return response


def apiCall(query):
    try:
        r = requests.get(query)
        while True:
            if r.status_code == 200:
                return json.loads(r.text)
            if r.status_code == 404:
                return 404
            errorHandler(r)
            r = requests.get(query)
        return json.loads(r.text)
    except requests.ConnectionError as e:
        print("Connection Error: ")
        print(e)
        sys.exit()


def errorHandler(r):
    if r.status_code == 404:
        print("404 Not found")
        return 404
    elif r.status_code == 401:
        print ("401 Unauthorized")
        return 401
    elif r.status_code == 500:
        print ("500 Internal server error")
        time.sleep(30)
        return 500
    elif r.status_code == 503:
        print ("503 Service unavailalbe")
        time.sleep(60)
        return 503
    elif r.status_code == 429:
        sleep = r.headers['retry-after']
        print ("429 Rate limit exceeded "+str(sleep))
        time.sleep(int(sleep))
        return 429
