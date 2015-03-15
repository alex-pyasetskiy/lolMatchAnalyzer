import pymysql
import threading
import time

ranks = {"BRONZE": 1,
         "SILVER": 2,
         "GOLD": 3,
         "PLATINUM": 4,
         "DIAMOND": 5,
         "MASTER": 6,
         "CHALLENGER": 7}
divisions = {"I": 1,
             "II": 2,
             "III": 3,
             "IV": 4,
             "V": 5}

db = pymysql.connect(
    host='localhost',
    user='root',
    passwd='asdf1234',
    db='lol')

cur = db.cursor()


def selectFetchOne(query):
    newdb = pymysql.connect(
        host='localhost',
        user='root',
        passwd='asdf1234',
        db='lol')

    newcur = newdb.cursor()
    newcur.execute(query)
    row = newcur.fetchone()
    newdb.close()
    return row


def selectFetchAll(query):
    newdb = pymysql.connect(
        host='localhost',
        user='root',
        passwd='asdf1234',
        db='lol')

    newcur = newdb.cursor()
    newcur.execute(query)
    rows = newcur.fetchall()
    newdb.close()
    return rows


def insertSummoner(data):
    for summonerId in sorted(data.items()):
        summonerId = summonerId[0]
        t = threading.Thread(target=insertSingleSummoner, args=(data, summonerId))
        t.daemon = True
        t.start()


def insertSingleSummoner(data, summonerId):
    print(summonerId)
    query = (
        "INSERT INTO summoner(summonerId, summonerLevel, revisionDate, lastSummonerCheck, lastRankCheck) VALUES ('{summonerId}', '{summonerLevel}', '{revisionDate}', NOW(), DATE(\"1970-01-01\"))"
        ).format(
        summonerId=summonerId,
        summonerLevel=data[summonerId]["summonerLevel"],
        revisionDate=data[summonerId]['revisionDate']
        )
    cur.execute(query)
    db.commit()


def updateSummoner(data):
    for summonerId in sorted(data.items()):
        print(summonerId[0])
        query = "UPDATE summoner SET summonerLevel = {summonerLevel}, revisionDate = {revisionDate}, lastSummonerCheck = NOW() WHERE summonerId = {summonerId}".format(
            summonerLevel=data[summonerId[0]]["summonerLevel"],
            revisionDate=data[summonerId[0]]["revisionDate"],
            summonerId=data[summonerId[0]]["id"]
            )


def updateSummonerRanks(data, summonerIds):
    for summonerId in sorted(data.items()):
        for league in data[summonerId[0]]:
            t = threading.Thread(target=insertRankIntoDatabase, args=(summonerId, league))
            t.daemon = True
            t.start()


def updateSummonerRankedDate(summonerIds):
    newdb = pymysql.connect(
        host='localhost',
        user='root',
        passwd='asdf1234',
        db='lol')
    newcur = newdb.cursor()
    for summonerId in summonerIds:
        query = "UPDATE summoner SET lastRankCheck = NOW() WHERE summonerId = {summonerId}".format(summonerId=summonerId)
        newcur.execute(query)
        newdb.commit()
    newdb.close()


def insertRankIntoDatabase(summonerId, league):
    newdb = pymysql.connect(
        host='localhost',
        user='root',
        passwd='asdf1234',
        db='lol')
    newcur = newdb.cursor()
    query = "SELECT * FROM summonerRanks WHERE summonerId = {summonerId} and rankedType = \"{queue}\"".format(summonerId=summonerId[0], queue=league["queue"])
    result = selectFetchOne(query)
    if result is None:
        query = "INSERT INTO summonerRanks(rankedType, tier, division, summonerId) VALUES(\"{rankedType}\",\"{tier}\",\"{division}\",{summonerId})".format(
            rankedType=league["queue"],
            tier=league["tier"],
            division=league["entries"][0]["division"],
            summonerId=summonerId[0])
    else:
        query = "UPDATE summonerRanks SET rankedType=\"{rankedType}\", tier=\"{tier}\", division=\"{division}\" WHERE summonerId = {summonerId} and rankedType=\"{rankedType}\"".format(
            rankedType=league["queue"],
            tier=league["tier"],
            division=league["entries"][0]["division"],
            summonerId=summonerId[0])
    newcur.execute(query)
    newdb.commit()
    newdb.close()


def insertMatch(data):
    if "bans" in data["teams"][0]:
        query = "INSERT INTO teams(ban1,ban2,ban3) VALUES({ban1},{ban2},{ban3})".format(
            ban1=data["teams"][0]["bans"][0]["championId"],
            ban2=data["teams"][0]["bans"][1]["championId"],
            ban3=data["teams"][0]["bans"][2]["championId"]
            )
        cur.execute(query)
        db.commit()
        teamId1 = cur.lastrowid

        query = "INSERT INTO teams(ban1,ban2,ban3) VALUES({ban1},{ban2},{ban3})".format(
            ban1=data["teams"][1]["bans"][0]["championId"],
            ban2=data["teams"][1]["bans"][1]["championId"],
            ban3=data["teams"][1]["bans"][2]["championId"]
            )
        cur.execute(query)
        db.commit()
        teamId2 = cur.lastrowid
    else:
        query = "INSERT INTO teams() VALUES()"
        cur.execute(query)
        db.commit()
        teamId1 = cur.lastrowid

        query = "INSERT INTO teams() VALUES()"
        cur.execute(query)
        db.commit()
        teamId2 = cur.lastrowid

    query = "INSERT INTO matchStats(mapId, matchCreation, matchDuration, matchMode, matchType, matchVersion, platformId, queueType, region, season) VALUES(\"{mapId}\",\"{matchCreation}\",\"{matchDuration}\",\"{matchMode}\",\"{matchType}\",\"{matchVersion}\",\"{platformId}\",\"{queueType}\",\"{region}\",\"{season}\")".format(
        mapId=data["mapId"],
        matchCreation=data["matchCreation"],
        matchDuration=data["matchDuration"],
        matchMode=data["matchMode"],
        matchType=data["matchType"],
        matchVersion=data["matchVersion"],
        platformId=data["platformId"],
        queueType=data["queueType"],
        region=data["region"],
        season=data["season"]
        )
    cur.execute(query)
    db.commit()
    matchStatsId = cur.lastrowid

    query = "INSERT INTO lol.match(idteam1, idteam2, idmatchStats, matchId) VALUES({idteam1},{idteam2},{idmatchStats}, {matchId})".format(
        idteam1=teamId1,
        idteam2=teamId2,
        idmatchStats=matchStatsId,
        matchId=data["matchId"]
        )
    cur.execute(query)
    db.commit()
    matchId = cur.lastrowid
    print("MatchNumber: {matchId}".format(matchId=matchId))

    for participant in data["participants"]:
        t = threading.Thread(target=insertPlayer, args=(participant, teamId1, teamId2, data, matchId))
        t.daemon = False
        t.start()


def insertPlayer(participant, teamId1, teamId2, data, matchId):
        newdb = pymysql.connect(
            host='localhost',
            user='root',
            passwd='asdf1234',
            db='lol')
        newcur = newdb.cursor()
        stats = participant["stats"]
        timeline = participant["timeline"]

        if "neutralMinionsKilledTeamJungle" in stats:
            neutralMinionsTeam = stats["neutralMinionsKilledTeamJungle"]
        else:
            neutralMinionsTeam = 0
        if "neutralMinionsKilledEnemyJungle" in stats:
            neutralMinionsEnemy = stats["neutralMinionsKilledEnemyJungle"]
        else:
            neutralMinionsEnemy = 0
        if "wardsPlaced" in stats:
            wardsPlaced = stats["wardsPlaced"]
        else:
            wardsPlaced = 0
        if "wardsKilled" in stats:
            wardsKilled = stats["wardsKilled"]
        else:
            wardsKilled = 0
        if "sightWardsBoughtInGame" in stats:
            sightWardsBoughtInGame = stats["sightWardsBoughtInGame"]
        else:
            sightWardsBoughtInGame = 0
        if "visionWardsBoughtInGame" in stats:
            visionWardsBoughtInGame = stats["visionWardsBoughtInGame"]
        else:
            visionWardsBoughtInGame = 0
        if "firstTowerKill" in stats:
            firstTowerKill = stats["firstTowerKill"]
        else:
            firstTowerKill = 0
        if "towerKills" in stats:
            towerKills = stats["towerKills"]
        else:
            towerKills = 0
        if "inhibitorKills" in stats:
            inhibitorKills = stats["inhibitorKills"]
        else:
            inhibitorKills = 0
        if "firstInhibitorKills" in stats:
            firstInhibitorKill = stats["inhibitorKills"]
        else:
            firstInhibitorKill = 0
        if "firstInhibitorAssist" in stats:
            firstInhibitorAssist = stats["firstInhibitorAssist"]
        else:
            firstInhibitorAssist = 0

        query = "INSERT INTO playerStats(champLevel, kills, deaths, assists, wardsPlaced, sightWardsBoughtInGame, visionWardsBoughtInGame, wardsKilled, minionsKilled, neutralMinionsKilled, neutralMinionsKilledEnemyJungle, neutralMinionsKilledTeamJungle, goldEarned, goldSpent, doubleKills, tripleKills, quadraKills, pentaKills, unrealKills, firstBloodAssist, killingSprees, largestKillingSpree, largestMultiKill, firstTowerKill, towerKills, inhibitorKills, firstInhibitorKill, firstInhibitorAssist, physicalDamageDealt, physicalDamageDealtToChampions, largestCriticalStrike, magicalDamageDealt, magicalDamageDealtToChampions, totalDamageDealt, totalDamageDealtToChampions, trueDamageDealt, trueDamageDealtToChampions, physicalDamageTaken, magicalDamageTaken, totalDamageTaken, totalHeal, totalUnitsHealed, totalTimeCrowdControlDealt) VALUES({champLevel},{kills},{deaths},{assists},{wardsPlaced},{sightWardsBoughtInGame},{visionWardsBoughtInGame},{wardsKilled},{minionsKilled},{neutralMinionsKilled},{neutralMinionsKilledEnemyJungle},{neutralMinionsKilledTeamJungle},{goldEarned},{goldSpent},{doubleKills},{tripleKills},{quadraKills},{pentaKills},{unrealKills},{firstBloodAssist},{killingSprees},{largestKillingSpree},{largestMultiKill},{firstTowerKill},{towerKills},{inhibitorKills},{firstInhibitorKill},{firstInhibitorAssist},{physicalDamageDealt},{physicalDamageDealtToChampions},{largestCriticalStrike},{magicalDamageDealt},{magicalDamageDealtToChampions},{totalDamageDealt},{totalDamageDealtToChampions},{trueDamageDealt},{trueDamageDealtToChampions},{physicalDamageTaken},{magicalDamageTaken},{totalDamageTaken},{totalHeal},{totalUnitsHealed},{totalTimeCrowdControlDealt})".format(
            champLevel=stats["champLevel"],
            kills=stats["kills"],
            deaths=stats["deaths"],
            assists=stats["assists"],
            wardsPlaced=wardsPlaced,
            sightWardsBoughtInGame=sightWardsBoughtInGame,
            visionWardsBoughtInGame=visionWardsBoughtInGame,
            wardsKilled=wardsKilled,
            minionsKilled=stats["minionsKilled"],
            neutralMinionsKilled=stats["neutralMinionsKilled"],
            neutralMinionsKilledEnemyJungle=neutralMinionsEnemy,
            neutralMinionsKilledTeamJungle=neutralMinionsTeam,
            goldEarned=stats["goldEarned"],
            goldSpent=stats["goldSpent"],
            doubleKills=stats["doubleKills"],
            tripleKills=stats["tripleKills"],
            quadraKills=stats["quadraKills"],
            pentaKills=stats["pentaKills"],
            unrealKills=stats["unrealKills"],
            firstBloodAssist=stats["firstBloodAssist"],
            killingSprees=stats["killingSprees"],
            largestKillingSpree=stats["largestKillingSpree"],
            largestMultiKill=stats["largestMultiKill"],
            firstTowerKill=firstTowerKill,
            towerKills=towerKills,
            inhibitorKills=inhibitorKills,
            firstInhibitorKill=firstInhibitorKill,
            firstInhibitorAssist=firstInhibitorAssist,
            physicalDamageDealt=stats["physicalDamageDealt"],
            physicalDamageDealtToChampions=stats["physicalDamageDealtToChampions"],
            largestCriticalStrike=stats["largestCriticalStrike"],
            magicalDamageDealt=stats["magicDamageDealt"],
            magicalDamageDealtToChampions=stats["magicDamageDealtToChampions"],
            trueDamageDealt=stats["trueDamageDealt"],
            trueDamageDealtToChampions=stats["trueDamageDealtToChampions"],
            totalDamageDealt=stats["totalDamageDealt"],
            totalDamageDealtToChampions=stats["totalDamageDealtToChampions"],
            physicalDamageTaken=stats["physicalDamageTaken"],
            magicalDamageTaken=stats["magicDamageTaken"],
            totalDamageTaken=stats["totalDamageTaken"],
            totalHeal=stats["totalHeal"],
            totalUnitsHealed=stats["totalUnitsHealed"],
            totalTimeCrowdControlDealt=stats["totalTimeCrowdControlDealt"],
            )
        newcur.execute(query)
        newdb.commit()

        playerStatsId = newcur.lastrowid

        query = "INSERT INTO playerItems(item0, item1, item2, item3, item4, item5, item6) VALUES({item0},{item1},{item2},{item3},{item4},{item5},{item6})".format(
            item0=stats["item0"],
            item1=stats["item1"],
            item2=stats["item2"],
            item3=stats["item3"],
            item4=stats["item4"],
            item5=stats["item5"],
            item6=stats["item6"]
            )
        newcur.execute(query)
        newdb.commit
        playerItemsId = newcur.lastrowid

        if participant["teamId"] == 100:
            teamId = teamId1
        else:
            teamId = teamId2

        if "player" in data["participantIdentities"][participant["participantId"]-1]:
            summonerId = data["participantIdentities"][participant["participantId"]-1]["player"]["summonerId"]
        else:
            summonerId = 0

        query = "INSERT INTO player(summonerId, championId, winner, team, role, lane, idplayerStats, idplayerItems, idteam) VALUES({summonerId},{championId},{winner},{team},\"{role}\",\"{lane}\",{idplayerStats},{idplayerItems},{idteam})".format(
            summonerId=summonerId,
            championId=participant["championId"],
            winner=stats["winner"],
            team=participant["teamId"],
            role=timeline["role"],
            lane=timeline["lane"],
            idplayerStats=playerStatsId,
            idplayerItems=playerItemsId,
            idteam=teamId
            )
        newcur.execute(query)
        newdb.commit()
        playerId = newcur.lastrowid
        print("{matchId} {playerId}".format(playerId=playerId, matchId=matchId))

        for item in timeline:
            for time in item:
                query = "INSERT INTO playerTimelineData(type, time, idplayer) VALUES(\"{type}\",\"{time}\",{idplayer})".format(
                    type=item[0],
                    time=time[0],
                    idplayer=playerId)
                newcur.execute(query)
                newdb.commit()

        if "masteries" in participant:
            for mastery in participant["masteries"]:
                query = "INSERT INTO masteries(masteryId, masteryRank, idplayer) VALUES({masteryId},{masteryRank},{idplayer})".format(
                    masteryId=mastery["masteryId"],
                    masteryRank=mastery["rank"],
                    idplayer=playerId
                    )
                newcur.execute(query)
                newdb.commit()
        if "runes" in participant:
            for rune in participant["runes"]:
                query = "INSERT INTO runes(runeId, runeRank, idplayer) VALUES({runeId},{runeRank},{idplayer})".format(
                    runeId=rune["runeId"],
                    runeRank=rune["rank"],
                    idplayer=playerId)
                newcur.execute(query)
                newdb.commit()
        newdb.close()
        return True
