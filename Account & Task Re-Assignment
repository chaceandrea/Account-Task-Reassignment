import json
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin


def make_query(query):
    response = sf.query(query)
    firstrecords = response.get('records')
    nextrecordsurl = response.get('nextRecordsUrl')

    while not response.get('done'):
        response = sf.query_more(nextrecordsurl, identifier_is_url=True)
        firstrecords.extend(response.get('records'))
        nextrecordsurl = response.get('nextRecordsUrl')
    return firstrecords


def task_dataframe_format(firstrecords):
    df_records = pd.DataFrame(firstrecords).drop(labels='attributes', axis=1)
    ownernames = df_records['Owner'].apply(pd.Series).drop(labels='attributes', axis=1)
    accountnames = df_records['Account'].apply(pd.Series).drop(labels='attributes', axis=1)
    ids = df_records['Id'].apply(pd.Series)
    subject = df_records['Subject'].apply(pd.Series)
    what = df_records['What'].apply(pd.Series).drop(labels='attributes', axis=1)
    taskdataframe = pd.concat([ownernames, accountnames, subject, ids, what], axis=1)
    taskdataframe.columns = ['Task Owner', 'Account', 'Subject', 'ID', 'Related to']
    return taskdataframe


bulkAccountList = []
bulkTaskList = []

loginInfo = json.load(open('logn.json'))
sfusername = loginInfo['sfusername']
sfpassword = loginInfo['sfpassword']
security_token = loginInfo['security_token']
domain = 'login'
session_id, instance = SalesforceLogin(username=sfusername, password=sfpassword, security_token=security_token,
                                       domain=domain)
sf = Salesforce(instance=instance, session_id=session_id)

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 500)
pd.set_option('display.min_rows', 500)
pd.set_option('display.max_colwidth', 150)
pd.set_option('display.width', 120)
pd.set_option('expand_frame_repr', True)

accountquery = "SELECT Id, Owner.Name, Name, RecordType.Name FROM Account WHERE OwnerId = '00541000006fXoQAAU' AND " \
               "(RecordType.Name = 'Honorary Alumni' OR RecordType.Name = 'Prospects' OR RecordType.Name = 'Alumni')"
person1taskquery = "SELECT Id, Owner.Name, Account.Name, Subject, What.Type FROM Task WHERE Status = 'Open' and " \
                   "Task.Account.OwnerId = '0052M000009Du88QAC' and What.Type = 'Alumni__c' and " \
                   "OwnerId != '0052M000009Du88QAC'"
person2taskquery = "SELECT Id, Owner.Name, Account.Name, Subject, What.Type FROM Task WHERE Status = 'Open' and " \
                   "Task.Account.OwnerId = '0052M00000ARkFRQA1' and What.Type = 'Alumni__c' and " \
                   "OwnerId != '0052M00000ARkFRQA1'"
person3taskquery = "SELECT Id, Owner.Name, Account.Name, Subject, What.Type FROM Task WHERE Status = 'Open' and " \
                   "Task.Account.OwnerId = '0052M000008lbosQAA' and What.Type = 'Alumni__c' and " \
                   "OwnerId != '0052M000008lbosQAA'"
person4taskquery = "SELECT Id, Owner.Name, Account.Name, Subject, What.Type FROM Task WHERE Status = 'Open' and " \
                   "Task.Account.OwnerId = '0052M000009zW1TQAU' and What.Type = 'Alumni__c' and " \
                   "OwnerId != '0052M000009zW1TQAU'"

accountQueryResults = make_query(accountquery)

df_records = pd.DataFrame(accountQueryResults).drop(labels='attributes', axis=1)
ownernames = df_records['Owner'].apply(pd.Series).drop(labels='attributes', axis=1)
accountnames = df_records['Name'].apply(pd.Series)
ids = df_records['Id'].apply(pd.Series)
recordtype = df_records['RecordType'].apply(pd.Series).drop(labels='attributes', axis=1)
accountnames = pd.concat([ownernames, accountnames, ids, recordtype], axis=1)
accountnames.columns = ['Owner', 'Account', 'ID', 'Record Type']
currentaccounts = accountnames.sample(frac=1).reset_index(drop=True)

amountPerCoordinator = int(len(currentaccounts) / 4)

# Create Coordinator Dataframes
person1dataframe = currentaccounts.iloc[:amountPerCoordinator]
person2dataframe = currentaccounts.iloc[amountPerCoordinator:(amountPerCoordinator * 2)]
person3dataframe = currentaccounts.iloc[amountPerCoordinator * 2:(amountPerCoordinator * 3)]
person4dataframe = currentaccounts.iloc[amountPerCoordinator * 3:(amountPerCoordinator * 4)]

# If there is a remainder, add to person 2's dataframe
if len(currentaccounts) % 4 != 0:
    person2dataframe = person2dataframe.append(currentaccounts.iloc[-(len(currentaccounts) % 4):])

# Create list of dicts for bulk update
for row in person1dataframe.itertuples():
    dict = {'Id': f'{row[3]}', 'OwnerId': '0052M000009Du88QAC'}
    bulkAccountList.append(dict)
    print("Account ID being assigned to person1: " + row[3])

for row in person2dataframe.itertuples():
    dict = {'Id': f'{row[3]}', 'OwnerId': '0052M00000ARkFRQA1'}
    bulkAccountList.append(dict)
    print("Account ID being assigned to person2: " + row[3])

for row in person3dataframe.itertuples():
    dict = {'Id': f'{row[3]}', 'OwnerId': '0052M000008lbosQAA'}
    bulkAccountList.append(dict)
    print("Account ID being assigned to person3: " + row[3])

for row in person4dataframe.itertuples():
    dict = {'Id': f'{row[3]}', 'OwnerId': '0052M000009zW1TQAU'}
    bulkAccountList.append(dict)
    print("Account ID being assigned to person4: " + row[3])

# Send bulk account update through API
bulkAccountUpdateResponse = sf.bulk.Account.update(bulkAccountList, batch_size=10000, use_serial=False)

person1queryresults = make_query(person1taskquery)
person1tasks = task_dataframe_format(person1queryresults)

person2queryresults = make_query(person2taskquery)
person2tasks = task_dataframe_format(person2queryresults)

person3queryresults = make_query(person3taskquery)
person3tasks = task_dataframe_format(person3queryresults)

person4queryresults = make_query(person4taskquery)
person4tasks = task_dataframe_format(person4queryresults)

# Create list of dicts for bulk update
for row in person1tasks.itertuples():
    dict = {'Id': f'{row[4]}', 'OwnerId': '0052M000009Du88QAC'}
    bulkTaskList.append(dict)

for row in person2tasks.itertuples():
    dict = {'Id': f'{row[4]}', 'OwnerId': '0052M00000ARkFRQA1'}
    bulkTaskList.append(dict)

for row in person3tasks.itertuples():
    dict = {'Id': f'{row[4]}', 'OwnerId': '0052M000008lbosQAA'}
    bulkTaskList.append(dict)

for row in person4tasks.itertuples():
    dict = {'Id': f'{row[4]}', 'OwnerId': '0052M000009zW1TQAU'}
    bulkTaskList.append(dict)

# Send bulk task update through API
bulkTaskUpdateResponse = sf.bulk.Task.update(bulkTaskList, batch_size=10000, use_serial=False)
