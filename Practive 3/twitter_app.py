import tweepy
from kafka import SimpleProducer, KafkaClient

class tw_Listener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        # producer collects messages and
        # send to kafka when collected messages reached 500 or every 10 seconds
        self.producer = SimpleProducer(KafkaClient("localhost:9092"), async = True,
                          batch_send_every_n = 500,
                          batch_send_every_t = 10)
    def on_error(self, status_code):
        print'Error status code: ' + str(status_code)
        return True
    def on_status(self, status):
        try:
            self.producer.send_messages('twitteranalysis', status.text.encode('utf-8'))
        except Exception as e:
            print 'Exception error: ' + str(e)
            return False
        return True
    def on_timeout(self):
        return True

if __name__ == '__main__':
    # set up tweet connections based on my own twitter app
    access_token = "935916503366623232-VJzoLuiL1LeSEKsZLMMmnuuJH631M41"
    access_token_secret = "ibtu49kgfYWGZFPFK6bxmL6zS68Wxx5IfBWub6GkT4O0A"
    consumer_key = "yUcv8YyeZicHwfYaXiLcN8ufT"
    consumer_secret = "kfzEXiyYecA8xrQyBPMder7PfoEK1q86raozuC8oDISlgs0W1V"
    # create OAuthHandler instance
    auth_Han = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth_Han.set_access_token(access_token, access_token_secret)
    tw_api = tweepy.API(auth_Han)
    # create tweepy stream
    tw_stream = tweepy.Stream(auth_Han, listener = tw_Listener(tw_api))
    # track tweets with hashtags #trump and #obama
    tw_stream.filter(track=['trump', 'obama'], languages = ['en'])
    # replace #trump and #obama if you want to collect tweets with other hashtags
