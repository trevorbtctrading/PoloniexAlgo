
import pandas as pd
import numpy as np
import requests
import time
from numba import jit
import thread
from collections import defaultdict, OrderedDict
import websocket
import requests
import json
import traceback
import bisect

#Keys and URLs and Coin
API_KEY = 'DCXX98U1-D2N8KDJA-V8JTZ3CJ-ADB6UI1X'
SECRET = 'e5d1c296cdf0d78208d79281b96479e025e6de128cdbe8d675295c4f6b2f1321b13e63ddff66724c76821cb9f58f9b06aa3419c8c816976adadfe7cf30a632d5'
URL = 'wss://api2.poloniex.com'
COIN_ONE = 'USDT_BTC'
COIN_TWO = 'USDC_BTC'
#COIN_NUMBERS = {121: 'USDT_BTC', 224: 'USDC_BTC', 226: 'USDC_USDT'}
COIN_NUMBERS = {191: 'USDT_BTC'}



orderBook = {coin: {'bids': None, 'asks': None} for coin in COIN_NUMBERS.keys()}
bboDict = {coin: (None, None) for coin in COIN_NUMBERS.keys()}


def getCoinPairs():
	global assetIds
	text = requests.get('https://poloniex.com/support/api/').text
	assetIds = pd.DataFrame(pd.read_html(text)[2])
	assetIds.index = pd.read_html(text)[2][0]
	assetIds.columns = ['id', 'pair']
	assetIds.index = assetIds['id']
	assetIds.drop('id',axis=1, inplace=True)

@jit
def getMax(numbers):
	return np.max(numbers)

@jit
def getMin(numbers):
	return np.min(numbers)

def updateBbo(coin):
	global bboDict
	global orderBook
	
	start = time.time()
	newBbo = (orderBook[coin]['bids'][-1], orderBook[coin]['asks'][0])

	if newBbo != bboDict[coin]:
		bboDict[coin] = newBbo
	 	print coin, (newBbo[0][0], newBbo[1][0]), newBbo[0][1], newBbo[1][1]


def updateOrderBook(price, quantity, coin, buySell, message):
	global OrderBook

	if buySell == 0:
		if quantity == 0:
			del orderBook[coin]['asks'][bisect.bisect_left(orderBook[coin]['asks'], (price, ))]
		else:
			bisect.insort(orderBook[coin]['asks'], (price, quantity))

	elif buySell == 1:
		if quantity == 0:
			del orderBook[coin]['bids'][bisect.bisect_right(orderBook[coin]['bids'], (price, ))]
		else:
			bisect.insort(orderBook[coin]['bids'], (price, quantity))

	else:
		print message, 'OrderBook update failed'


def parseOrders(message, coin):
	global orderBook
	global bboDict
	
	startTime = time.time()

	if message[0] in COIN_NUMBERS.keys():

		orders = message[2]

		for order in orders:
			messageType = str(order[0][0])
			
		

			if messageType == 'i':
				bids = orders[0][1]['orderBook'][1]
				asks = orders[0][1]['orderBook'][0]

				try:
					orderBook[coin]['bids'] = sorted([(float(x), float(y)) for x, y in bids.items()])
					orderBook[coin]['asks'] = sorted([(float(x), float(y)) for x, y in asks.items()])
					updateBbo(coin)
					print 'BBO: {}'.format(bboDict[coin])
				except:
					traceback.print_exc()


			


			elif messageType == 'o':
				price = float(order[2])
				quantity = float(order[3])
				buySell = float(order[1])
				bestBid = bboDict[coin][0]
				bestAsk = bboDict[coin][1]
				
				updateOrderBook(price, quantity, coin, buySell, message)
				updateBbo(coin)

			elif messageType == 't':
				pass

	#print 'Update orderbook took: {} Micros'.format((time.time() - startTime) * 1000000)


def on_message(ws, message):
    
	message = json.loads(message)
	coin = message[0]

	parseOrders(message, coin)

	


		

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    print("ONOPEN")
    def run(*args):
        
        #Subscribe to OrderBook updates
        for coin in COIN_NUMBERS.keys():
        	ws.send( json.dumps( { 'command' : 'subscribe','channel' : coin} ) )
        	print 'Subscribed to {}'.format(coin)
        
        #Subscribe to all ticker updates (Best Bid, Best Ask) HIGH LATENCY, VERY SLOW
        #ws.send( json.dumps( { 'command' : 'subscribe','channel' : 1002} ) )

        while True:
            time.sleep(1)
        ws.close()
        print("thread terminating...")
    thread.start_new_thread(run, ())


if __name__ == "__main__":

	getCoinPairs()

	websocket.enableTrace(True)
	ws = websocket.WebSocketApp("wss://api2.poloniex.com/",
	                          on_message = on_message,
	                          on_error = on_error,
	                          on_close = on_close)
	ws.on_open = on_open
	ws.run_forever()

	