import mongo_connection
import praw
import unicodedata

__author__ = 'Fabian'


def crawl_movies(subreddits, queries, subreddit_start=0, queries_start=0):
    subreddits = list(enumerate(subreddits))
    queries = list(enumerate(queries))
    db = mongo_connection.MongoConnection().get_db()
    redditApi = praw.Reddit(user_agent='server:coins2015oscar2.redditmoviecrawler:0.1')
    for subreddit in subreddits[subreddit_start:]:
        for query in queries[queries_start:]:
            print("Crawling SubReddit \"r/" + subreddit[1] + "\"("+str(subreddit[0])+") for \""+query[1]+"\"(" +
                  str(query[0])+")")
            count = crawl_reddit(redditApi, subreddit[1], query[1], db)
            print("Found " + str(count) + " posts for SubReddit \"r/" + subreddit[1] + "\"("+str(subreddit[0])+") for \""+query[1]+"\"(" +
                  str(query[0])+")")
    return True


def crawl_reddit(redditApi,subreddit, query, db):
    gen = redditApi.search(query, subreddit=subreddit, sort="relevance")
    count = 0
    for submission in gen:
        docs = create_documents(submission, query)
        db["reddit_posts"].insert_many(docs)
        count += len(docs)
    return count


def create_documents(submission, query):
    insert = [create_submission_document(submission, query)]
    submission.replace_more_comments(limit=30, threshold=1)
    comments = praw.helpers.flatten_tree(submission.comments, nested_attr=u'replies', depth_first=False)
    title = submission.title
    title = unicodedata.normalize('NFKD', title).encode('ascii','ignore')
    print("Found " + str(len(comments)) + " comments for submission \"" +
          str(title) + "\"")
    for comment in comments:
        if type(comment) is praw.objects.MoreComments:
            continue
        insert.append(create_comment_document(comment, submission.title, query))
    return insert


def create_comment_document(comment, title, query):
    doc = {"type": "comment",
           "id": comment.id,
           "ups": comment.ups,
           "downs": comment.downs,
           "body": comment.body,
           "title": title,
           "query": query
           }
    if comment.author is not None:
        doc.update({"author": comment.author.name})
    return doc


def create_submission_document(submission, query):
    doc = {"type": "submission",
           "id": submission.id,
           "ups": submission.ups,
           "downs": submission.downs,
           "author": submission.author.name,
           "title": submission.title,
           "body": submission.selftext,
           "query": query
           }
    return doc
