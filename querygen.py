__author__ = 'Fabian'

import time

def create_cloudsearch_query(q):
    fromdate = str(int(time.mktime(q["fromdate"].timetuple())))
    untildate = str(int(time.mktime(q["untildate"].timetuple())))
    query = "(and (and subreddit:'" + q["subreddit"] + "') (and timestamp:" + fromdate + ".." + untildate + "))"
    return query
