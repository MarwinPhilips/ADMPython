import csv


class TimestampInMinute:
    def __init__(self, timestamp):
        self.timestamp = timestamp
        self.count = 0
        self.errorCount = 0
        self.Durations = []

    @property
    def averageduration(self):
        return sum(self.Durations) / self.count


class SendReceived:
    def __init__(self, sent, received, messageerror):
        self.sent = sent
        self.received = received
        self.sent_minute = sent // 60000
        self.duration = received - sent
        self.messageerror = messageerror


class LogMessage:
    def __init__(self, code, client_id, loc_ts, err_code, time):
        self.code = code
        self.client_id = client_id
        self.loc_ts = loc_ts
        self.err_code = err_code
        self.time = time


path = 'C:/ADM/client_trace50.csv'
data = csv.DictReader(open(path),dialect='excel')


def task2firstmap(data):
    logs = []
    for row in data:
        # somehow the clientid 0 is not following the logic to increment the loc_ts for every call. there is no way to clearly identify which
        # request belongs to which response
        message = LogMessage(str(row['code']),int(row['client_id']),int(row['loc_ts']),safeintcast(row['err_code'], -1), int(row['time']))
        if message.client_id != 0:
            logs.append(message)
    return logs

def safeintcast(val,default):
    try:
        return int(val)
    except(ValueError, TypeError):
        return default

def task2firstreduce(logs):
    logcount = len(logs)
    sendreceiveds = []
    i = 0
    send = None
    receive = None
    while i < logcount:
        error = False
        if logs[i].code == 'msg_snd':
            send = logs[i]
            i += 1
            if logs[i].code == 'res_rcv':
                receive = logs[i]
                i += 1
            else:
                print('Error while working with res_rcv ' + logs[i].code + ' ' + str(logs[i].client_id) + ' ' + str(logs[i].loc_ts))
                error = True
        else:
            print('Error while working with msg_snd ' + logs[i].code + ' ' + str(logs[i].client_id) + ' ' + str(logs[i].loc_ts))
            error = True
        if error:
            i += 1
        else:
            messageerror = False
            if (send.err_code > -1) or (receive.err_code > -1): messageerror = True
            sendreceiveds.append(SendReceived(send.time, receive.time, messageerror))

    return sendreceiveds


def task2secondreduce(sendReceiveds):
    currentminute = TimestampInMinute(0)
    timestampsinminutes = []
    for sr in sendReceiveds:
        if sr.sent_minute != currentminute.timestamp:
            currentminute = TimestampInMinute(sr.sent_minute)
            timestampsinminutes.append(currentminute)
        currentminute.count += 1
        currentminute.Durations.append(sr.duration)
        if sr.messageerror: currentminute.errorCount += 1
    return timestampsinminutes


def write(timestampsinminutes):
    file = open('C:/ADM/task2result.csv', 'w')
    for tsim in timestampsinminutes:
        file.write(str(tsim.timestamp)+','+str(tsim.count)+','+str(tsim.errorCount)+','+str(tsim.averageduration)+'\n')


logs = task2firstmap(data)
print('data loaded')
# sorting first by clientid, second by local timestamp, third by type. This combination identifies one package send-receive
logs.sort(key=lambda x: (x.client_id, x.loc_ts, x.code))
print('data sorted 1')
sendReceiveds = task2firstreduce(logs)
print('firstReduce done')
sendReceiveds.sort(key=lambda x: x.sent_minute)
print('data sorted 2')
timestampsinminutes = task2secondreduce(sendReceiveds)
print('secondreduce done')
write(timestampsinminutes)
print('write done')