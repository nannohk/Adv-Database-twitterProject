import os
import gzip
import json
import sqlite3
import csv


class ChunkReader:
    def __init__(self, cnkdir):
        self.__cnkdir = cnkdir

    def get_cnkdir(self):
        return self.__cnkdir

    def get_filename(self, cnk):
        cnkdir = self.get_cnkdir()
        return f'{cnkdir}/a{cnk:08d}.cnk.gz'

    def get_records(self, cnk):
        fn = self.get_filename(cnk)
        if not os.path.exists(fn):
            return
        with gzip.open(fn, 'rb') as f:
            res_bytes = f.read()

        lst = res_bytes.splitlines()
        res_bytes = None  # to save intermediate memory
        for line in lst:
            record = eval(line)
            if record == [] or record is None:
                return
            ndx = record[0]
            yield ndx, json.loads(record[1])


if __name__ == '__main__':
#  Establish database connection
    conn = sqlite3.connect('twitterProj.db')
    cur = conn.cursor()

    def createDatabase():
        with open('dbSetup.txt') as f:
            stmt = f.read()
        cur.executescript(stmt)

    def manageData():
        cr = ChunkReader('j228')
        iter = cr.get_records(228042)
        # for ndx, record in iter:
        #     J = json.dumps(record, indent=4)
        #     print(J)

        for i, (ndx, obj) in zip(range(1000), iter):
            if 'text' in obj:
                tweetId = obj['id_str']
                tweet = obj['text']
                tweetDate = obj['created_at']
                userName = obj['user']['name']
                userId = obj['user']['id_str']
                userLocation = obj['user']['location']
                userCreationDate = obj['user']['created_at']
                followersCount = obj['user']['followers_count']
                friendsCount = obj['user']['friends_count']
                tweetLanguage = obj['lang']
                # print(tweetId, tweet, tweetDate, userName, userId, userLocation, userCreationDate, followersCount,'\n')
                # print(json.dumps(obj, indent=4))
                cur.execute('INSERT INTO user (userId, userName, userLocation, userCreationDate, userFollowers,userFriends) VALUES (?,?,?,?,?,?)',
                             (userId, userName, userLocation, userCreationDate, followersCount, friendsCount))

                cur.execute('INSERT INTO tweet (tweetId, tweet, tweetDate, userId, language) VALUES (?,?,?,?,?)',
                             (tweetId, tweet, tweetDate, userId, tweetLanguage))
        conn.commit()

# Georaphical data of twitter users
    def getLocations():
        cur.execute('select userLocation from user')
        locations = cur.fetchall()
        with open('location.csv', 'w') as blob:
            writer = csv.writer(blob)
            for location in locations:
                try:
                    writer.writerow(location[0].split(','))
                except:
                    pass
            writer.close()


# number of tweets per year


    def getTweetsPerYear():
        cur.execute('select tweetDate from tweet')
        dates = cur.fetchall()
        with open('tweetTimes.csv', 'w') as blob:
            writer = csv.writer(blob)
            for date in dates:
                year = date[0].split(' ')[-3]
                writer.writerow(year.split(','))
            writer.close()

# language of tweets
    def getTweetLanguage():
        cur.execute('select language from tweet')
        languages = cur.fetchall()
        with open('tweetLanguage.csv', 'w') as blob:
            writer = csv.writer(blob)
            for language in languages:
                writer.writerow(language[0].split(','))
            writer.close()

# program execution
    # createDatabase()

    # Insert Data from the dataset into the database
    # manageData()

# Get the locations of the users and store them in a csv file
    # getLocations()

# Get the number of tweets per year
    # getTweetsPerYear()

# Get the language of the tweets
    # getTweetLanguage()
  
#   Close database connection
    if conn:
        conn.close()