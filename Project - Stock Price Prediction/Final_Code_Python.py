# -*- coding: utf-8 -*-
#start own code
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import scipy.sparse
from fastai.torch_basics import *
from fastai.data.all import *
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM
#end own code

def make_date(df, date_field):
    "Make sure `df[date_field]` is of the right date type."
    field_dtype = df[date_field].dtype
    if isinstance(field_dtype, pd.core.dtypes.dtypes.DatetimeTZDtype):
        field_dtype = np.datetime64
    if not np.issubdtype(field_dtype, np.datetime64):
        df[date_field] = pd.to_datetime(df[date_field], infer_datetime_format=True)

def add_datepart(df, field_name, prefix=None, drop=True, time=False):
    "Helper function that adds columns relevant to a date in the column `field_name` of `df`."
    make_date(df, field_name)
    field = df[field_name]
    prefix = ifnone(prefix, re.sub('[Dd]ate$', '', field_name))
    attr = ['Year', 'Month', 'Week', 'Day', 'Dayofweek', 'Dayofyear', 'Is_month_end', 'Is_month_start',
            'Is_quarter_end', 'Is_quarter_start', 'Is_year_end', 'Is_year_start']
    if time: attr = attr + ['Hour', 'Minute', 'Second']
    # Pandas removed `dt.week` in v1.1.10
    week = field.dt.isocalendar().week.astype(field.dt.day.dtype) if hasattr(field.dt, 'isocalendar') else field.dt.week
    for n in attr: df[prefix + n] = getattr(field.dt, n.lower()) if n != 'Week' else week
    mask = ~field.isna()
    df[prefix + 'Elapsed'] = np.where(mask,field.values.astype(np.int64) // 10 ** 9,np.nan)
    if drop: df.drop(field_name, axis=1, inplace=True)
    return df

#start own code
mydata = pd.read_csv('stock_data.csv')
#print(mydata)

df = mydata.copy()
df = df.iloc[:,[0,4]]
print(df.columns)

data = mydata.copy()
data = data.iloc[:,[0,4]]
#print(data.columns)

mydata['Date'] = pd.to_datetime(mydata.Date,format='%Y-%m-%d')
mydata.index = mydata['Date']

plt.plot(mydata['Close'])
#plt.figure(figsize = (16,8))
#plt.plot(mydata['Date'], label='Close Price history')


data = add_datepart(data, 'Date')
data.drop('Elapsed', axis=1, inplace=True)  
print(data.columns)
datafull = data.copy()
datafull = datafull.iloc[:,[0,1,2,3,4,5]]
print(datafull)

train = datafull[:3655]
test = datafull[3655:]

x_train = train.drop('Close', axis=1)
y_train = train['Close']
x_test = test.drop('Close', axis=1)
y_test = test['Close']
print(x_train)
print(y_train)

#model1 linear
model = LinearRegression()
model.fit(x_train,y_train)
preds = model.predict(x_test)
rms=np.sqrt(np.mean(np.power((np.array(y_test)-np.array(preds)),2)))
print(rms)

test['Predictions'] = 0
test['Predictions'] = preds

test.index = datafull[3655:].index
train.index = datafull[:3655].index

plt.plot(train['Close'])

plt.plot(test[['Close', 'Predictions']])


# model2 knn
model2 = KNeighborsRegressor(n_neighbors = 3)
model2.fit(x_train,y_train)
preds2 = model2.predict(x_test)
rms2=np.sqrt(np.mean(np.power((np.array(y_test)-np.array(preds2)),2)))
print(rms2)

test['Predictions'] = 0
test['Predictions'] = preds2

test.index = datafull[3655:].index
train.index = datafull[:3655].index

plt.plot(train['Close'])

plt.plot(test[['Close', 'Predictions']])


#model3 lstm

df.index = df.Date
df.drop('Date', axis=1, inplace=True)

train2 = df[0:3655]
valid = df[3655:]
print(train)
print(valid)
#end own code
#converting dataset into x_train and y_train
scaler = MinMaxScaler(feature_range=(0, 1))
scaled_data = scaler.fit_transform(train2)

x_train2, y_train2 = [], []
for i in range(60,len(train2)):
    x_train2.append(scaled_data[i-60:i,0])
    y_train2.append(scaled_data[i,0])
x_train2, y_train2 = np.array(x_train2), np.array(y_train2)

x_train2 = np.reshape(x_train2, (x_train2.shape[0],x_train2.shape[1],1))
print(x_train2)



# start own code
model3 = Sequential()
model3.add(LSTM(units=50, return_sequences=True, input_shape=(x_train2.shape[1],1)))
model3.add(LSTM(units=50))
model3.add(Dense(1))

model3.compile(loss='mean_squared_error', optimizer='adam')
model3.fit(x_train2, y_train2, epochs=2, batch_size=1, verbose=1)
#end own code
#predicting 246 values, using past 60 from the train data
inputs = df[len(df) - len(valid) - 60:].values
inputs = inputs.reshape(-1,1)
inputs  = scaler.transform(inputs)

X_test = []
for i in range(60,inputs.shape[0]):
    X_test.append(inputs[i-60:i,0])
X_test = np.array(X_test)

X_test = np.reshape(X_test, (X_test.shape[0],X_test.shape[1],1))
closing_price = model3.predict(X_test)
closing_price = scaler.inverse_transform(closing_price)


rms3=np.sqrt(np.mean(np.power((valid-closing_price),2)))

#start own code


#for plotting
train4 = df[:3655]
valid4 = df[3655:]
valid4['Predictions'] = closing_price
plt.plot(train4['Close'])
plt.plot(valid4[['Close','Predictions']])

#end own code