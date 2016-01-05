from pymongo import MongoClient


def get_mongo_database_with_auth(dbhost, dbport, dbname, username, password):
    client = MongoClient(dbhost, dbport)
    db = client[dbname]

    if username is not None or password is not None:
        if not db.authenticate(username, password):
            raise "Failed to authenticate to MongoDB database {0} using given username and password!".format(dbname)

    return db