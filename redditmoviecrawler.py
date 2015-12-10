import mongo_connection
import praw
import sys
from prawoauth2 import PrawOAuth2Mini
from tokens import app_key, app_secret, access_token, refresh_token
from settings import user_agent, scopes
import datetime
import querygen

__author__ = 'Fabian'
today = datetime.date.today()
then = datetime.date(2015, 12, 1)
subreddits = ["movie", "movies", "film", "oscars", "comingsoon"]
reddit_client = praw.Reddit(user_agent=user_agent)
oauth_helper = PrawOAuth2Mini(reddit_client, app_key=app_key,
                              app_secret=app_secret, access_token=access_token,
                              scopes=scopes, refresh_token=refresh_token)


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


def create_documents(submission):
    print("Creating Documents")
    print(str(submission.subreddit))
    documents = []
    submission.replace_more_comments(limit=None, threshold=1)
    oauth_helper.refresh()
    comments = praw.helpers.flatten_tree(submission.comments, nested_attr=u'replies', depth_first=False)
    for comment in comments:
        if type(comment) is praw.objects.MoreComments:
            continue
        documents.append(create_comment_document(comment))
    return documents


def crawl_subreddit(reddit, subreddit, db):
    print("Create Query for ")
    query = querygen.create_cloudsearch_query({
        "fromdate": then,
        "untildate": today,
        "subreddit": subreddit
    })
    print(query)
    gen = reddit.search(query, syntax="cloudsearch", subreddit=subreddit, sort="relevance")
    count = 0
    errors = 0;

    for submission in gen:
        print("Next Submission")
        tryagain = True
        while tryagain:
            try:
                docs = create_documents(submission)
                try:
                    if docs:
                        db["reddit_posts"].insert_many(docs)
                        count += len(docs)
                    tryagain = False
                except BaseException as e:
                    print("inserting in db failed - skip")
                    print(e)
            except praw.errors.OAuthInvalidToken:
                # token expired, refresh 'em!
                print("Refreshing Token")
                oauth_helper.refresh()
            except:
                print("unecpected error " + sys.exc_info()[0])
                errors += 1;
                if errors > 100:
                    tryagain = False;
    return count


def crawl_movies(reddit_client, subreddits):
    db = mongo_connection.MongoConnection().get_db()
    print("Starting to Crawl movies")
    for subreddit in subreddits:
        print("Next Subreddit")
        try:
            oauth_helper.refresh()
            count = crawl_subreddit(reddit_client, subreddit, db)
        except praw.errors.OAuthInvalidToken:
            # token expired, refresh 'em!
            oauth_helper.refresh()
            print("Refreshing Token")
    print(count)
    return True


crawl_movies(reddit_client, subreddits)





