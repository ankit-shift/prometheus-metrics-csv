#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import requests
import sys
from dateutil.parser import parse
from datetime import timedelta  
from multiprocessing import Process, Manager, Lock
   
if len(sys.argv) != 5:
    print('PrometheusURL: {0} https://promethues_host:9090'.format(sys.argv[1]))
    print('StartDate: {0} 2022-10-17T09:30:55Z'.format(sys.argv[2]))
    print('EndDate: {0} 2023-01-17T09:32:13Z'.format(sys.argv[3]))
    print('Metrics: {0} metric1{{filter_label="value"}},metric2{{filter_label="value"}}'.format(sys.argv[4]))
    sys.exit(1)
    
try :
    global promURL
    global startDate
    global endDate
    global metrics
    promURL = sys.argv[1]
    startDate = parse(sys.argv[2])
    endDate = parse(sys.argv[3])
    metrics = sys.argv[4].split(',') 
except :
    print('DateFormat Error : expected "2022-10-17T09:30:55Z" recieved {0}'.format(sys.argv[1]))   

def createDailyBatches(startDate,endDate):
    dateArray = []
    flaggedDate = startDate
    while flaggedDate < endDate :
        dateArray.append([flaggedDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (flaggedDate + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')])
        flaggedDate += timedelta(days=1,seconds=1)
    return dateArray

def printQueryRangeHeaderLabels(results, writer, dictionary, lock):
    if dictionary['writeHeader'] == False or 'labels' in dictionary:
        return  dictionary['labels']    
    try:  
        lock.acquire()
        if dictionary['writeHeader']:
            global labels 
            for result in results:
                    labels = set()
                    labels.update(result['metric'].keys())
            labels.discard('__name__')
            labels = sorted(labels)
            writer.writerow(['timestamp','name','value'] + labels)
            dictionary['writeHeader'] = False
            dictionary['labels']= labels.copy()
            return dictionary['labels']
    finally:
        lock.release()
        
        
def processQueryRange(metric,start,end,writer, dictionary, lock): 
    response = requests.get('{0}/api/v1/query_range'.format(promURL),
        params={
        'query': metric,
        'start': start,
        'end': end,
        'step': 8,
        }, timeout=120, verify=False)
    results = response.json()['data']['result']
    labels = printQueryRangeHeaderLabels(results, writer, dictionary, lock)
    for result in results:
        l = [result['values'][0][0]] + [result['metric'].get('__name__', '')] + [result['values'][0][1]] 
        for label in labels:
            l.append(result['metric'].get(label, ''))
        writer.writerow(l)
        
def run_parallel(*functions):
    '''
    Run functions in parallel
    '''
    processes = []
    for function in functions:
        proc = Process(target=function)
        proc.start()
        processes.append(proc)
    for proc in processes:
        proc.join()


def processMetric(metric):
    with open(metric.split('{')[0]+'.csv', 'w', newline='') as file:        
            manager = Manager()
            lock = Lock()
            dictionary = manager.dict()
            dictionary['writeHeader'] = True
            writer = csv.writer(file)
            dateArray = createDailyBatches(startDate,endDate)
            functionalReferences = [lambda i=i: processQueryRange(metric, i[0], i[1], writer, dictionary, lock) for i in dateArray]
            print('Initiated :',len(functionalReferences),'routines created for metric ', metric)
            run_parallel(*functionalReferences)
            print('Completed :', metric)
    file.close()

if __name__ == '__main__':
    metricProcessorReferences = [lambda metric=metric: processMetric(metric) for metric in metrics]
    print('Total Metrics Requested :', len(metricProcessorReferences) )
    run_parallel(*metricProcessorReferences)   

