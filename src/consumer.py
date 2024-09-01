import json
import numpy as np
import joblib
import warnings
warnings.filterwarnings('ignore')

rob_scaler = joblib.load('../models/rob_scaler.joblib')
rf_clf= joblib.load('../models/rf_clf.joblib')

def consumer_task(transaction):
    transaction = json.loads(transaction)
    transaction['scaled_amount'] = rob_scaler.transform(np.reshape(transaction['Amount'],(-1,1))).reshape(-1)[0]
    
    del transaction['Amount']
    pred = rf_clf.predict(np.array(list(transaction.values())).reshape(-1,1))
    
    if pred[0]==0:
        prediction = 'valid Transaction'
    else:
        prediction = 'fraud Transaction'
        return prediction    
    