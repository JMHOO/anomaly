'''
Basic IO interface
Jiaming Hu 2017-07-12

Mimic the feed API for events processing.

Classes:

JSONLog

StreamLog

BatchLog

FlaggedPurchaseLog

'''
import json, json.decoder
import six
import os

'''
JSONLog
Base class, implement an iterator for streaming the log
'''
class JSONLog():
    def __init__(self, log_file):
        # this is a mimic feed API interface
        # in this example, we just load whole log file, it's memory intensive if log file is huge, obviously
        self.json_decoder = json.JSONDecoder()
        with open(log_file, 'r') as f:
            self._buffered_data = f.read()

        self.lineCount = self._buffered_data.count('\n')

    """
    Parse a json object from a buffer, return it and the rest of the buffer, otherwise return None.
    """
    def __json_splitter__(self, buffer):
        buffer = buffer.strip()
        try:
            obj, index = self.json_decoder.raw_decode(buffer)
            rest = buffer[json.decoder.WHITESPACE.match(buffer, index).end():]
            return obj, rest
        except ValueError:
            return None

    '''
    Return JSON object, use __json_splitter__ as backend
    WARNING: this function is not fast enough, avoid to use it for building social network.
    '''
    def Stream(self):
        splitter = self.__json_splitter__
        buffered = six.text_type('')

        for data in self._buffered_data:
            buffered += data
            while True:
                buffer_split = splitter(buffered)
                if buffer_split is None:
                    break
                item, buffered = buffer_split
                yield item

    '''
    4x faster than Stream(), it process data as lines, then convert to JSON object directly
    '''
    def StreamLine(self):
        for data in self._buffered_data.split('\n'):
            if data is None or data == '': continue
            try:
                obj = self.json_decoder.decode(data)
                yield obj
            except:
                continue

'''
Process stream_log.json
All function inherit from base class
'''
class StreamLog(JSONLog):
    def __init__(self, log_file):
        JSONLog.__init__(self, log_file)


'''
Process batch_log.json
It extract first line: parameters, Stream the rest of file as JSON object
'''
class BatchLog(JSONLog):
    def __init__(self, log_file):
        JSONLog.__init__(self, log_file)

        try:
            self._params, index = self.json_decoder.raw_decode(self._buffered_data)
            self._buffered_data = self._buffered_data[json.decoder.WHITESPACE.match(self._buffered_data, index).end():]
        except ValueError:
            print('Critical error, no D/T parameters inside the batch_log file.')

        # verifying paramters
        if 'D' not in self._params or 'T' not in self._params:
            print('Critical error, D/T parameters incomplete, initialize to regular value, set D=2, T=50')
            self._params['D'] = 2
            self._params['T'] = 50

        print('Parameters: D={}, T={}'.format(self._params['D'], self._params['T']))

    def paramDegree(self):
        return self._params['D']
    def paramTrackable(self):
        return self._params['T']

'''
output writer
Write flagged purchases to json file, one line each time

This class will check the existence of flagged_purchases.json 
'''
class FlaggedPurchasesLog():

    def __init__(self, log_file):
        self.__log_file = log_file
        if not os.path.exists(log_file):
            dir = os.path.dirname(log_file)
            print(dir)
            if not os.path.exists(dir): os.makedirs(dir)
            file = open(log_file, "a")
            file.close()

    def append(self, event_string):
        with open(self.__log_file, "a") as f:
            f.write(event_string)
            f.write('\n')
