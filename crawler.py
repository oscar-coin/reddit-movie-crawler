import praw
import time
import json
from prawoauth2 import PrawOAuth2Mini


class Crawler:
    reddit = None
    auth = None
    subreddits = []
    db = None

    def __init__(self, reddit, auth, subreddits, db):
        self.subreddits = subreddits
        self.reddit = reddit
        self.auth = auth
        self.db = db

    def crawl_subreddit(self, subreddit, t, cutoff):
        comments = self.get_comments(subreddit, t)
        for comment in comments:
            if hasattr(comment, "created_utc") and comment.created_utc < cutoff:
                break
            document = Crawler.create_comment_document(comment)
            # self.db.insert(document)
            print(json.dumps(document))

    def get_comments(self, subreddit, t):
        try:
            return self.reddit.get_comments(subreddit=subreddit, t=t, sort="new", limit=None)
        except praw.errors.OAuthInvalidToken:
            self.auth.refresh()
            return self.get_comments(subreddit, time)

    def start(self):
        cutoffs = dict((self.subreddits[i], 0) for i in range(0, len(self.subreddits)))
        t = "month"
        while True:
            for subreddit in self.subreddits:
                now = int(time.time())
                self.crawl_subreddit(subreddit, t, cutoffs[subreddit])
                cutoffs[subreddit] = now
            t = "hour"

    @staticmethod
    def create_comment_document(comment):
        doc = {
            "id": comment.id,
        }
        if hasattr(comment, "retrieved_on"):
            doc["retrieved_on"] = comment.retrieved_on
        if hasattr(comment, "author") and comment.author is not None:
            doc["author"] = comment.author.id
        if hasattr(comment, "ups"):
            doc["ups"] = comment.ups
        if hasattr(comment, "downs"):
            doc["downs"] = comment.downs,
        if hasattr(comment, "body"):
            doc["body"] = comment.body
        if hasattr(comment, "name"):
            doc["name"] = comment.name
        if hasattr(comment, "created_utc"):
            doc["created_utc"] = comment.created_utc
        if hasattr(comment, "subreddit_id"):
            doc["subreddit_id"] = comment.subreddit_id
        if hasattr(comment, "link_id"):
            doc["link_id"] = comment.link_id
        if hasattr(comment, "parent_id"):
            doc["parent_id"] = comment.parent_id
        if hasattr(comment, "score"):
            doc["score"] = comment.score
        if hasattr(comment, "controversiality"):
            doc["controversiality"] = comment.controversiality
        if hasattr(comment, "distinguished"):
            doc["distinguished"] = comment.distinguished
        return doc



def init_crawler(app_key, app_secret, access_token, refresh_token):
    scopes = ['identity', 'read']
    subreddits = ['movies']
    user_agent = 'Movie discussion crawler as part of university project 2015 (by YoungFaa)'
    reddit = praw.Reddit(user_agent=user_agent)
    auth = PrawOAuth2Mini(reddit, app_key=app_key, app_secret=app_secret, access_token=access_token, scopes=scopes,
                          refresh_token=refresh_token)
    crawler = Crawler(reddit, auth, subreddits, None)
    crawler.start()

