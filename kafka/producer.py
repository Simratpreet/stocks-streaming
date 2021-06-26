import json
import itertools
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer

f = open('symbol_mapping.json', 'r')
stocks_dict = json.load(f)

stocks = stocks_dict.keys()



class StdOutListener(StreamListener):
    def on_data(self, data):
        data = json.loads(data)

        filter_data = {}
        for key in data.keys():
        	filter_data['id'] = str(data['id'])
        	filter_data['text'] = data['text']
        	filter_data['created_at'] = data['created_at']

        producer.send("stock_tweets_test", key=filter_data['id'], value=json.dumps(filter_data).encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

producer = KafkaProducer(bootstrap_servers='localhost:9092', key_serializer=str.encode)
l = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=stocks)