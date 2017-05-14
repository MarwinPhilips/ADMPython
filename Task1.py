import csv

class timestampCount:
    def __init__(self, timestamp):
        self.timestamp = timestamp
        self.count = 1

path = 'C:/ADM/mw_trace50.csv'
data = csv.DictReader(open(path),dialect='excel')


def task1map(data):
    timestamps = []
    for row in data:
        if row['code']=='res_snd':
            timestamps.append(int(row['time'])//60000)
    return timestamps


def task1reduce(timestamps):
    timestampsCount = []
    previousTimestamp = timestampCount(0)
    for ts in timestamps:
        if ts != previousTimestamp.timestamp:
            previousTimestamp = timestampCount(ts)
            timestampsCount.append(previousTimestamp)
        else:
            previousTimestamp.count += 1
    return timestampsCount


def write(timestampsCount):
    file = open('C:/ADM/task1result.csv', 'w')
    for tsc in timestampsCount:
        file.write(str(tsc.timestamp)+','+str(tsc.count)+'\n')

timestamps = task1map(data)
sorted(timestamps)
timestampsCount = task1reduce(timestamps)

write(timestampsCount)