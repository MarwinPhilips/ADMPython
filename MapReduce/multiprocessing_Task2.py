import csv
import sys
import datetime
import multiprocessing
import collections
import pandas
import numpy
from MapReduce import MapReduce

"""
Ziel Teil 2: Mittelwerte von Responsetimes pro Minute (Clientseitiges Log)
-> ergibt 2 mapreduces?
"""


class TimestampInMinute:
    def __init__(self, timestamp):
        self.timestamp = timestamp
        self.count = 0
        self.errorCount = 0
        self.Durations = []

    @property
    def averageduration(self):
        return sum(self.Durations) / self.count


class LogMessage:
    def __init__(self, code, client_id, loc_ts, err_code, time):
        self.code = code
        self.client_id = client_id
        self.loc_ts = loc_ts
        self.err_code = err_code
        self.time = time


class SendReceived:
    def __init__(self, sent, received, messageerror):
        self.sent = sent
        self.received = received
        self.sent_minute = sent // 60000
        self.duration = received - sent
        self.messageerror = messageerror

def safeintcast(val,default):
    try:
        return int(val)
    except(ValueError, TypeError):
        return default


def task1map1(file):
    """
    Returns a list of 
    :param file: 
    :return: 
    """
    start_time = datetime.datetime.now()
    print(multiprocessing.current_process().name + " started mapping.")
    with open(file) as f:
        #data = csv.DictReader(f, dialect='excel')
        data = pandas.read_csv(f, dialect='excel', index_col=0)
        data.drop("length", axis=1, inplace=True)
        data.drop("op", axis=1, inplace=True)
        data.drop("thread_id", axis=1, inplace=True)
        print("Time for loading: ", datetime.datetime.now() - start_time)
        filtered_list = []
        for row in zip(data['code'], data['client_id'], data['loc_ts'], data['err_code'], data['time']):
            #message = LogMessage(str(row['code']), int(row['client_id']), int(row['loc_ts']),
                                 #safeintcast(row['err_code'], -1), int(row['time']))
            if int(row[1]) != 0:
                #logs.append(pandas.Series([row['code'], row['client_id'], row['loc_ts'], safeintcast(row['err_code'], -1), row['time']]), ignore_index=True)
                filtered_list.append([row[0], row[1], row[2], safeintcast(row[3], -1), row[4]])
                if len(filtered_list) > 50:
                    break
                    #pass
    logs = pandas.DataFrame(filtered_list, columns=["code", "client_id", "loc_ts", "err_code", "time"])
    stop_time = datetime.datetime.now()
    running_time = stop_time - start_time
    print(multiprocessing.current_process().name + " finished mapping. Time spent: " + str(running_time))
    return logs


def task1shuffle1(data):
    start_time = datetime.datetime.now()
    print("Starting to shuffle...")
    #l = list(data)
    data_list = pandas.concat(data)
    data_list.sort_values(by=["client_id", "loc_ts", "code"], inplace=True)
    data_list.reset_index(drop=True, inplace=True)
    print("Time for sorting: ", datetime.datetime.now()-start_time)
    logcount = len(data_list)
    print("Number of lines: ", logcount)
    print("Size of list in memory: ", sys.getsizeof(data_list))
    sendreceiveds = []
    i = 0
    send = None
    receive = None
    while i < logcount-1:
        error = False
        test = data_list.get_value(i, "code")
        if data_list.get_value(i, "code") == 'msg_snd':
            send = data_list.iloc[[i]].values.tolist()[0]
            i += 1
            if data_list.get_value(i, "code") == 'res_rcv':
                receive = data_list.iloc[[i]].values.tolist()[0]
                i += 1
            else:
                print('Error while working with res_rcv ' + data_list.get_value(i, "code") + ' ' + str(data_list.get_value(i, "client_id")) + ' ' + str(
                    data_list.get_value(i, "loc_ts")))
                error = True
        else:
            print('Error while working with msg_snd ' + data_list.get_value(i, "code") + ' ' + str(data_list.get_value(i, "client_id")) + ' ' + str(
                data_list.get_value(i, "loc_ts")))
            error = True
        if error:
            i += 1
        else:
            messageerror = False
            if (send[3] > -1) or (receive[3] > -1): messageerror = True
            sendreceiveds.append([send[4], receive[4], send[4] // 60000,  messageerror])
    sendreceiveds.sort(key=lambda x:x[2])
    grouped_list = []
    minute = 0
    count = 0
    for sr in sendreceiveds:
        if sr[2] == minute:
            pass
    stop_time = datetime.datetime.now()
    running_time = stop_time - start_time
    print("Shuffeling terminated. Time spent: " + str(running_time))
    return sendreceiveds


def task1reduce1(sendReceiveds):
    currentminute = TimestampInMinute(0)
    timestampsinminutes = []
    for sr in sendReceiveds:
        if sr[2] != currentminute.timestamp:
            currentminute = TimestampInMinute(sr[2])
            timestampsinminutes.append(currentminute)
        currentminute.count += 1
        currentminute.Durations.append(sr[1]-sr[0])
        if sr[3]: currentminute.errorCount += 1
    return timestampsinminutes


if __name__ == '__main__':
    import glob
    start_time = datetime.datetime.now()
    print("Number of processors: " + str(multiprocessing.cpu_count()))
    chunks = glob.glob("E:\\Dropbox\\04 Info FH\\Advanced Data Management\\MapReduce\\split_2_12\\*.csv")
    # The first MapReduce produces a list of send-receive-pairs:
    mapper = MapReduce(task1map1, task1reduce1, task1shuffle1, multiprocessing.cpu_count())
    minute_averages = mapper(chunks)
    # The second Mapreduce produces the minute averages:
    df = pandas.DataFrame.from_dict(minute_averages[0], orient='index').reset_index()
    df = df.rename(columns={'index': 'minute', 0: 'count'})
    df.to_csv("E:\\Dropbox\\04 Info FH\\Advanced Data Management\\MapReduce\\output.csv")
    stop_time = datetime.datetime.now()
    running_time = stop_time - start_time
    print("Program terminated. Time spent: " + str(running_time))
