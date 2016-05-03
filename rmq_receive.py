'''
predict_EURUSD.py

@version: 1.0

Receives live OHLC data from cAlgo

Created on May, 17, 2015

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com
@summary: Receives data from rmq_send.cs. Data is used to calculate feature values which are then used for model prediction.

'''

import pika
import pandas as pd
import numpy as np
import datetime as dt
from StringIO import StringIO

from TreeDataBasic import TreeFeaturesLive
from Technical import *
from Functions import *
import copy
import talib as ta
from talib import abstract
from sklearn.externals import joblib

connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost'))
channel = connection.channel()
channel.queue_declare(queue = 'Asset')

model = joblib.load('model.pkl')

def marketConditions(data):

	data = copy.deepcopy(data)

	c1 = data['X1'].ix[-1:].values <= 0
	c2 = True

	if (c1 == True) & (c2 == True):
		return True
	else:
		return True

def callback(ch, method, properties, body):

	# RECEIVE/FORMAT DATA

	str_data = StringIO(body) # Data from rmq_send.cs
	df_data = pd.DataFrame.from_csv(str_data)

	# CALCULATE FEATURES

	df_features = TreeFeaturesLive(df_data) # Function to calculate model features based on received data
	ls_exlude = ['OPEN','HIGH','LOW','CLOSE','VOLUME'] # columns not required for model input
	cols = [col for col in df_features.columns if col not in ls_exlude]
	df_x = df_features[cols] # filter to obtain only necessary cols

	# CALCULATE PROBABILITY

	if marketConditions(df_x) == True:

		ls_x = df_x.iloc[-1:].as_matrix()

		if (np.isnan(ls_x).any() == True) or (np.isinf(ls_x).any() == True): # Check data is clean
			lr_px = np.array([(0.5,0.5)])
		else:
			try:
				lr_px = model.predict_proba(ls_x) # obtain probability output
			except:
				lr_px = np.array([(0.5,0.5)])

	else:

		lr_px = np.array([(0.5,0.5)])

	# SHOW OUTPUT

	str_date = str(df_x.ix[-1:].index.format()[0])
	str_px = str(lr_px[:,1][0])
	print('{0]: {1}'.format(str_date, str_px))



channel.basic_consume(callback, queue = 'Asset', no_ack = True)
channel.start_consuming()