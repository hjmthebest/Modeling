```python

# 초기코드 - python
import pandas as pd

# 데이터 로드
train = pd.read_csv('./data/train.csv')
test = pd.read_csv('./data/test.csv')

| user_id                          | subscribed |
|----------------------------------|------------|
| 2a6edcfae41a7bca26bc6e18b5ff578c | 1          |
| 1ca2ea9a4f2e3f1d1103972730724d7a | 0          |
| 541252c92613800e4c225f941bc76b3c | 0          |
| Cd8979ec9615905dc2b77a9504088296 | 0          |
| 51e49c5e0eb7f38d4ed921161b7186b9 | 1          |

train.head()
train.isna().sum().sum()
train.info()
train.describe()

from sklearn.model_selection import train_test_split

feature = train.drop(['user_id', 'subscribed'], axis=1)
label = train['subscribed']

X_train, X_test, y_train, y_test = train_test_split(feature, label, test_size=0.3)

from sklearn.ensemble import RandomForestClassifier
rfc = RandomForestClassifier(n_estimators=500, max_depth=11, n_jobs=-1)

from sklearn.model_selection import cross_val_score, KFold
kfold = KFold(n_splits=5)
scores = cross_val_score(rfc, X_train, y_train, cv=kfold)
scores

scores.std()

for i in range(5, 15):
    rfc = RandomForestClassifier(n_estimators=500, max_depth=i, n_jobs=-1)
    kfold = KFold(n_splits=5)
    scores = cross_val_score(rfc, X_train, y_train, cv=kfold)
    print(f"{i}: {scores.mean():.4f}, {scores.std():.4f}") 
rfc.fit(X_train, y_train)
pred = rfc.predict(X_test)
pd.DataFrame({'prediction':pred, 'y':y_test})

from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix, classification_report

print(f"accuracy_score: {accuracy_score(pred, y_test):.4f}")
print(f"precision_score: {precision_score(pred, y_test):.4f}")
print(f"recall_score: {recall_score(pred, y_test):.4f}")

confusion_matrix(pred, y_test)
print(classification_report(pred, y_test))

pred_proba = rfc.predict_proba(X_test)
pred_proba_num = pred_proba[:, 1]
pred_proba_num

from sklearn.metrics import roc_curve, auc, f1_score, roc_auc_score
import seaborn as sns
import matplotlib.pyplot as plt

fpr, tpr, threshold = roc_curve(y_test, pred_proba_num)
plt.plot(fpr, tpr, color='red', label='ROC')
plt.plot([0,1], [0,1], color='black', linestyle='--', label='Random Guess')

plt.title('Receiver Operating Characteristic Curve')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.legend()
plt.show()

feature_x = test.drop(['user_id'], axis=1)
feature_user_id = test['user_id']
feature_x

new_pred = rfc.predict(feature_x)
df = pd.DataFrame({'user_id':feature_user_id, 'subscribed':new_pred})

# csv 파일 저장 예시 - python
df.to_csv('submission.csv')
```
