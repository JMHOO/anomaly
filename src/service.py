'''
Main Service
Jiaming Hu 2017-07-12

This class should be run as a daemon or service
Now, for simplicity, there is no event loop inside.

The service will be blocked on StreamLog.Stream() which is a JSON object generator


Classes:
AnomalyService

'''
import sys, time
from src.feed import StreamLog, BatchLog, FlaggedPurchasesLog
from src.core import SocialNetwork, EventAction

class AnomalyService:
    def __init__(self):
        self._stream_log = None
        self._batch_log = None

    def Start(self, args):
        # create IO objects
        self._stream_log = StreamLog(args.stream_log_file)
        self._batch_log = BatchLog(args.batch_log_file)
        self._flagged_log = FlaggedPurchasesLog(args.flagged_purchase_file)

        # create core object, SocialNetwork
        self._social_network = SocialNetwork(self._batch_log.paramDegree(),
                                             self._batch_log.paramTrackable(),
                                             self._flagged_log)

        #build initial network
        print('Building network......\n')
        start_time = time.time()
        for i, blog in enumerate(self._batch_log.StreamLine()):
            self._social_network.processEvent(blog, EventAction.initializing)

            if i % 10000 == 0 or i == self._batch_log.lineCount - 1:
                end_time = time.time()
                # Difference between start and end-times.
                time_dif = end_time - start_time
                start_time = end_time

                sys.stdout.write("\r")
                sys.stdout.write("{:.2f}% -- processing speed: {:.0f} logs/s".format(i/self._batch_log.lineCount*100, 1000/time_dif))
                sys.stdout.flush()

        print('\nDone.\nListening on stream log.....\n')

        # show the stats of our social network
        self._social_network.status()

        # feed with stream log, the Stream() function return a generator
        for slog in self._stream_log.Stream():
            self._social_network.processEvent(slog, EventAction.processing)



