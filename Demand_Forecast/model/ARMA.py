import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

ChinaBank = pd.read_csv('ChinaBank.csv',index_col = 'Date',parse_dates=['Date'])
ChinaBank.index = pd.DatetimeIndex(ChinaBank.index)

sub = ChinaBank.loc['2014-01':'2014-06',]
sub = sub.copy()
sub['Close_diff_1'] = sub['Close'].diff(1)
sub['Close_diff_2'] = sub['Close_diff_1'].diff(1)

fig = plt.figure(figsize=(20,6))
ax1 = fig.add_subplot(131)
ax1.plot(sub['Close'])
ax2 = fig.add_subplot(132)
ax2.plot(sub['Close_diff_1'])
ax3 = fig.add_subplot(133)
ax3.plot(sub['Close_diff_2'])
plt.show()  #一阶近似平稳

train = sub.loc['2014-01':'2014-03',['Close_diff_1']]
test = sub.loc['2014-04':'2014-06',['Close_diff_1']]
train.dropna(inplace=True)

fig = plt.figure(figsize=(12, 8))
ax1 = fig.add_subplot(211)
fig = sm.graphics.tsa.plot_acf(train, lags=20, ax=ax1)
ax1.xaxis.set_ticks_position('bottom')
fig.tight_layout()

ax2 = fig.add_subplot(212)
fig = sm.graphics.tsa.plot_pacf(train, lags=20, ax=ax2)
ax2.xaxis.set_ticks_position('bottom')
fig.tight_layout()
plt.show()  #ARIMA(0,1,0)

# train_results = sm.tsa.arma_order_select_ic(train, ic=['aic', 'bic'], trend='nc', max_ar=8, max_ma=8)
#
# print('AIC', train_results.aic_min_order)
# print('BIC', train_results.bic_min_order)
model = sm.tsa.ARIMA(train, order=(0, 1, 0))
results = model.fit()
resid = results.resid #赋值
fig = plt.figure(figsize=(12,8))
fig = sm.graphics.tsa.plot_acf(resid.values.squeeze(), lags=40)
plt.show()

model = sm.tsa.ARIMA(sub.loc['2014-01':'2014-06',['Close_diff_1']], order=(0, 1, 0))
results = model.fit()
predict_sunspots = results.predict(start=str('2014-04'),end=str('2014-05'),dynamic=False)
print(predict_sunspots)
fig, ax = plt.subplots(figsize=(12, 8))
ax = sub.plot(ax=ax)
predict_sunspots.plot(ax=ax)
plt.show()

