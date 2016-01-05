import argparse
import crawler


def main():
    args = parse_args()

    crawler.init_crawler(args.app_key, args.app_secret, args.access_token, args.refresh_token)


def parse_args():
    parser = argparse.ArgumentParser()

    # Mongo params
    """
    parser.add_argument('--dbhost', help='Address of MongoDB server', default="127.0.0.1")
    parser.add_argument('--dbport', help='Port of MongoDB server', default=27017)
    parser.add_argument('--dbname', '-n', help='Database name', type=str, required=True)
    parser.add_argument('--username', help='Database user', default=None)
    parser.add_argument('--password', help='Password for the user', default=None)
    parser.add_argument('--collection', '-v', help='Collection name for Twitter stream data',
                        default="twitter_stream")
                        """

    # Reddit params
    parser.add_argument('--app_key', help='Reddit app key used for OAuth', required=True)
    parser.add_argument('--app_secret', help='Reddit app secret used for OAuth', required=True)
    parser.add_argument('--access_token', help='Reddit access token used for OAuth', required=True)
    parser.add_argument('--refresh_token', help='Reddit refresh token used for OAuth', required=True)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nGoodbye')

