```python 
# 초기코드 - python
import pandas as pd

# 데이터 로드
train = pd.read_csv('./data/train.csv')
test = pd.read_csv('./data/test.csv')

test.head()
train.info()
train.describe()
train['crim'].sort_values(ascending=False)
test['crim'].sort_values(ascending=False)

train.isna().sum().sum()
test.isna().sum().sum()

# 결측치, 이상치 없음
feature = train.drop(['medv'], axis=1)
label = train['medv']

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(feature, label, test_size=0.3)

# Random Forest Regressor
from sklearn.ensemble import RandomForestRegressor
rfr = RandomForestRegressor(n_estimators=100, max_depth=5, n_jobs=-1)
rfr.fit(X_train, y_train)

# XGBOOST Regressor
from xgboost import XGBRegressor
xgb = XGBRegressor(learning_rate=0.1,max_depth=9, n_estimators=100)
xgb.fit(X_train, y_train)

from sklearn.model_selection import cross_val_score, KFold
for i in range(5, 15):
    xgb = XGBRegressor(learning_rate=0.1,max_depth=i, n_estimators=100)
    kfold = KFold(n_splits=5)
    scores = cross_val_score(xgb, X_train, y_train, cv=kfold, scoring='r2')
    print(f"{i}: {scores.mean():.4f}, {scores.std():.4f}")

# pred = rfr.predict(X_test)
pred = xgb.predict(X_test)
pred 
from sklearn.metrics import mean_absolute_error
mean_absolute_error(y_test, pred)

from sklearn.metrics import mean_squared_error 
mean_squared_error(y_test, pred)

import numpy as np
*** from sklearn.metrics import mean_squared_error 
MSE = mean_squared_error(y_test, pred) 
np.sqrt(MSE)

from sklearn.metrics import mean_squared_log_error
mean_squared_log_error(y_test, pred)

def MAPE(y_test, pred):
    return np.mean(np.abs((y_test - pred) / y_test)) * 100 
MAPE(y_test, pred)

def MAE(y_test, pred): 
    return np.mean((y_test - pred) / y_test) * 100
MAE(y_test, pred)

import matplotlib.pyplot as plt
plt.plot(y_test, pred, 'or')

rand = y_test - pred
rand

# 잔차의 정규성을 확인하기 위한 QQ plot 시각화
import scipy.stats as stats
plt.figure(figsize=(10,5))
stats.probplot(rand, dist=stats.norm, plot=plt)
plt.show()

# R2 계산
from sklearn.metrics import r2_score
R2 = r2_score(y_test, pred)
print(R2)

plt.scatter(y_test, pred)
plt.show()

SST = ((y_test - y_test.mean())**2).sum()
SST

SSE = ((pred - y_test.mean())**2).sum()
SSE

SSR = ((y_test - pred)**2).sum()
SSR

print(f"SST:\t\t\t{SST:.4f}")
print(f"SSE:\t\t\t{SSE:.4f}")
print(f"SSR:\t\t\t{SSR:.4f}")
print(f"R2 Score:\t\t{1- (SSR/SST):.4f}")
print(f"Adjusted R2 Score:\t{1- ((SSR/(400-12-1))/(SST/(400-1))):.4f}")

# 모델 fitting이 잘 됐다. 성능이 좋다. 
SST:			7665.0517
SSE:			7537.2822
SSR:			899.5576
R2 Score:		0.8826
Adjusted R2 Score:	0.8790 
test_index = test['index']
testset = test.drop('index', axis=1)

pred_real = rfr.predict(testset)

df = pd.DataFrame({'index':test_index, 'medv':pred_real})
df

# csv 파일 저장 예시 - python
df.to_csv('submission.csv')
```
