from twitter import OAuth, TwitterStream

credentials = {"token": "2987172311-nww55Y0ZKPKhth05wkkX88bn5z6INqQRDBq5xSX",
               "token_secret": "digi83CDHjbD8vi8W8FnyLN7t8zd56pZ1XdqATdYJivex",
               "consumer_key": "V7xjnC1AdwECbbcv9OosDkawK",
               "consumer_secret": "rdEWrtop3r1ODjNCrPPpt18Z1Ey7BKtZRXJmwtTvQQ8u8JzULE"}


class AccessError(Exception):
    pass


class Tweet:
    def __init__(self, credentials):
        self.credentials = credentials
        self.api = None
        self.authenticated = False

    def authenticate(self):
        """Perform the authentication using the credentials given during the object instantiation."""
        if not self.authenticated:
            self.api = TwitterStream(auth=OAuth(**self.credentials))
            self.authenticated = True

    def sample(self):
        """Return the tweets sampled by the Twitter sample API.

           Returns:
           -------
           out: generator
               A generator returning the tweets in the JSON format.
        """

        if not self.authenticated:
            raise AccessError("You are not authenticated, please authenticate before streaming tweets.")
        else:
            return self.api.statuses.sample()

    def check_tweet(self, tweet):
        """Check whether the input tweet has all the necessary information to be saved and processed later on.

           Parameters:
           ----------
           tweet: dictionary
                 A dictionary corresponding to the tweet in the JSON format.

           Returns:
           -------
           out: boolean
               True if the tweet is conform, False otherwise
        """
        if not all([arg in tweet for arg in ["lang", "place", "created_at"]]):
            return False

        elif not isinstance(tweet["lang"], str) or tweet["lang"] != "":
            return False

        elif not isinstance(tweet["created_at"], str) or tweet["created_at"] != "":
            return False

        elif not all([arg in tweet["place"] for arg in ["country_code", "name", "id"]]):
            return False

        elif not isinstance(tweet["place"]["country_code"], str) or len(tweet["place"]["country_code"]) != 2:
            return False

        elif not isinstance(tweet["place"]["name"], str) or len(tweet["place"]["name"]) != "":
            return False

        elif not isinstance(tweet["place"]["id"], str) or len(tweet["place"]["id"]) != "":
            return False

        else:
            return True

    def record(self, tweet, database):
        """Record the input tweet in the given database.

        Parameters:
        ----------
        tweet: dictionary
              A dictionary corresponding to the tweet in the JSON format.
        database: str
                 A string corresponding to the Sqlite file database where the data must be stored.
        """
        pass


if __name__ == "__main__":
    tweets_grabber = Tweet(credentials)
    tweets_grabber.authenticate()

    for tweet in tweets_grabber.sample():
        if "text" in tweet:
            print(tweet["text"])