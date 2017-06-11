import datetime
import multiprocessing
import sys
import pandas
import MapReduce

"""
Ziel Teil 2: Mittelwerte von Responsetimes pro Minute (Clientseitiges Log)
"""


def safeintcast(val,default):
    try:
        return int(val)
    except(ValueError, TypeError):
        return default


def task1map1(file):
    """
    Returns a filtered pandas DataFrame object.
    :param file: a csv file containing the data
    :return: filtered pandas DataFrame object
    """
    start_time = datetime.datetime.now()
    print(multiprocessing.current_process().name + " started mapping.")
    with open(file) as f:
        data = pandas.read_csv(f, dialect='excel', index_col=0)
        data.drop("length", axis=1, inplace=True)
        data.drop("op", axis=1, inplace=True)
        data.drop("thread_id", axis=1, inplace=True)
        print("Time for loading: ", datetime.datetime.now() - start_time)
        filtered_list = []
        for row in zip(data['code'], data['client_id'], data['loc_ts'], data['err_code'], data['time']):
            if int(row[1]) != 0:
                filtered_list.append([row[0], row[1], row[2], safeintcast(row[3], -1), row[4]])

                # limit amount of items for testing:
                if len(filtered_list) > 50:
                    #break
                    pass
    logs = pandas.DataFrame(filtered_list, columns=["code", "client_id", "loc_ts", "err_code", "time"])
    stop_time = datetime.datetime.now()
    running_time = stop_time - start_time
    print(multiprocessing.current_process().name + " finished mapping. Time spent: " + str(running_time))
    return logs


def task1shuffle1(data):
    """
    Iterates through all items to generate send-received-pairs and then groups them by minute
    :param data: list of pandas dataframes
    :return: list of lists containing all send-received-pairs per minute
    """
    start_time = datetime.datetime.now()
    print("Starting to shuffle...")
    data_list = pandas.concat(data)
    data_list.sort_values(by=["client_id", "loc_ts", "code"], inplace=True)
    data_list.reset_index(drop=True, inplace=True)
    print("Time for sorting: ", datetime.datetime.now()-start_time)
    print("Number of lines: ", len(data_list))
    print("Size of list in memory: ", sys.getsizeof(data_list))
    sendreceiveds = []
    pair = False
    send = None
    receive = None
    for row in data_list.itertuples():
        error = False
        if pair == False and row[1] == 'msg_snd':
            pair = True
            send = row
            continue
        elif pair == True and row[1] == 'res_rcv':
            pair = False
            receive = row
        else:
            error = True
            print('Error while working with ' + row[1] + ' ' + str(
                row[2]) + ' ' + str(row[3]))
        if not error:
            messageerror = (send[4] > -1 or receive[4] > -1)
            sendreceiveds.append([send[5], receive[5], send[5] // 60000, receive[5]-send[5],  messageerror])

    sendreceiveds.sort(key=lambda x:x[2])
    grouped_list = []
    sublist = []
    minute = 0
    count = 0
    for sr in sendreceiveds:
        if sr[2] == minute:
            sublist.append(sr)
            count+=1
        else:
            if not sublist:
                sublist.append(sr)
                minute = sr[2]
            else:
                grouped_list.append(pandas.DataFrame(sublist))
                sublist = []
                sublist.append(sr)
                minute = sr[2]
    print("Shuffeling terminated. Time spent: " + str(datetime.datetime.now()-start_time))
    return grouped_list


def task1reduce1(sendReceiveds):
    """
    Reduces a list of send-received-pairs to their average duration
    :param sendReceiveds: list of send-received pairs within a minute
    :return: a tuple containing the minute number and its average duration for requests
    """

    # print(multiprocessing.current_process().name + " started reducing.")

    return [sendReceiveds[2][0], sendReceiveds[3].mean()]


if __name__ == '__main__':
    import glob
    start_time = datetime.datetime.now()
    print("Number of processors: " + str(multiprocessing.cpu_count()))
    chunks = glob.glob("E:\\Dropbox\\04 Info FH\\Advanced Data Management\\MapReduce\\split_2_12\\*.csv")
    mapper = MapReduce(task1map1, task1reduce1, task1shuffle1, multiprocessing.cpu_count())
    minute_averages = mapper(chunks)
    df = pandas.DataFrame(list(minute_averages))
    df = df.rename(columns={0: 'minute', 1: 'average'})
    df.to_csv("E:\\Dropbox\\04 Info FH\\Advanced Data Management\\MapReduce\\output.csv")
    print("Program terminated. Time spent: " + str(datetime.datetime.now()-start_time))
