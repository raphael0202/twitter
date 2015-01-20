from twitter import Twitter, OAuth, TwitterStream

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

    def authenticate(self, credentials):
        if not self.authenticated:
            self.api = TwitterStream(auth=OAuth(*credentials))
            self.authenticated = True

    def tweet_stream(self):
        if not self.authenticated:
            raise AccessError("You are not authenticated, please authenticate before streaming tweets.")
        else:
            return self.api.statuses.sample()