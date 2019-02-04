import flask
import requests
import os

def rpoListener(event, context):
    
    file = event
    project = os.environ.get('GCP_PROJECT', 'Specified environment variable is not set.')
    print(f"Project : {project}")
    print(f"Processing file: {file['name']}.")
    name = file['name']
    print( name.rfind('_'))
    splitIndex = name.rfind('_')
    ttl = name[splitIndex+1:]
    print (ttl)
    int_ttl = int(ttl)
    prefixIndex = name.find('/')
    print(prefixIndex)
    prefix = name[0:prefixIndex]
    print(prefix)
    print('Bucket: {}'.format(file['bucket']))
    #print(f"Processing full file: {file}")
    bucket = file['bucket']
    bucketAndPrefix = 'gs://'+bucket+'/'+prefix
    print(bucket)
    print ('data:','invoking REST call')
    dictToSend = {'datasetName': prefix, 'dataStorageName': bucketAndPrefix, 'projectId':project, 'retentionPeriod': int_ttl, 'type': 'DATASET'}
    res = requests.post('http://104.198.4.155:8080/retentionrules/', json=dictToSend)
    print ('response from server:',res.text)
    dictFromServer = res.json()