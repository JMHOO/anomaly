'''
Core modular
Jiaming Hu 2017-07-12

Classes:
Person: store person id and the relationship

Event: storing 'event' data, as a data object used by different classes

Purchases: maintain purchases history, it fetchs group purchases and summary their mean and sd

SocialNetwork: core component
   It's role can be a graph(person connection) and a coordinator(process event with Purchase class)

'''
from enum import Enum
from datetime import datetime
from math import sqrt

'''
Person class maintain person id and his/her network
Use adjacent list to storage relationship.

For example: 
A and B are friends, B and C are friends, the data structure should be like this:
A -> [B]
B -> [A, C]
C -> [B]
'''
class Person:
    def __init__(self, id):
        self.__id = id
        self.__adjacences = []

    def beFriendWith(self, person_id):
        if person_id not in self.__adjacences:
            self.__adjacences.append(person_id)

    def unFriendWith(self, person_id):
        self.__adjacences.remove(person_id)

    def getId(self):
        return self.__id

    def network(self):
        return self.__adjacences

# Enum class
class EventType(Enum):
    none = 0
    purchase = 1
    befriend = 2
    unfriend = 3

# Action, in order to mark different behavior for batch log and stream log
class EventAction(Enum):
    initializing = 1
    processing = 2

'''
Store event data, deserialize from JSON, serialize back to string
'''
class Event():
    def __init__(self, json_object):
        self.type = EventType.none
        self.valid = True

        if not json_object:
            print('Event:: JSON object is null.')

        try:
            self.type_str = json_object['event_type']
            self.timestamp_str = json_object['timestamp']
            if json_object['event_type'] == 'befriend':
                self.type = EventType.befriend
                self.id1 = int(json_object['id1'])
                self.id2 = int(json_object['id2'])
            elif json_object['event_type'] == 'unfriend':
                self.type = EventType.unfriend
                self.id1 = int(json_object['id1'])
                self.id2 = int(json_object['id2'])
            elif json_object['event_type'] == 'purchase':
                self.type = EventType.purchase
                self.id1 = int(json_object['id'])
                self.id2 = None
                self.amount = float(json_object['amount'])

            self.timestamp = datetime.strptime(json_object['timestamp'], '%Y-%m-%d %H:%M:%S')

        except:
            self.valid = False

    def toString(self, mean = None, sd = None):
        output = "{{\"event_type\":\"{type}\", \"timestamp\":\"{time}\", \"id\": \"{id}\", \"amount\": \"{amount:.2f}\"}}"
        output_with_mean = "{{\"event_type\":\"{type}\", \"timestamp\":\"{time}\", \"id\": \"{id}\", \"amount\": \"{amount:.2f}\", \"mean\": \"{mean:.2f}\", \"sd\": \"{sd:.2f}\"}}"
        output_friends = "{{\"event_type\":\"{type}\", \"timestamp\":\"{time}\", \"id1\": \"{id1}\", \"id2\": \"{id2}\"}}"

        if self.type is not EventType.purchase:
            return output_friends.format(type=self.type_str, time=self.timestamp_str, id1=self.id1, id2=self.id2)

        if mean is not None and sd is not None:
            output = output_with_mean
        return output.format(type=self.type_str, time=self.timestamp_str, id=self.id1, amount=self.amount, mean=mean, sd=sd)

'''
Some kind of memory DB

 use following data structure to storage purchase data
 list = [
   {'userid': 1, 'amount': 5.67, 'timestamp': 2017-11-01 13:12:00},
   {'userid': 1, 'amount': 5.67, 'timestamp': 2017-11-01 13:12:00},
 ]
 
 It has a strategy to only keep necessary data and get rid of outdated data.
 For example:
    When T=50, current people count is 10000, in worst case, there is no edges in the network,
    we need to keep 50 * 10000 records in order to calculate mean and sd
'''
class Purchase:
    def __init__(self, social_network):
        self.__data = []
        self.__socialObj = social_network

    def append(self, event, action):
        self.__data.append({'userid': event.id1, 'amount': event.amount, 'timestamp': event.timestamp})

        # reduce the memory usage, get rid of outdated data
        if action is EventAction.processing:
            max_records_length = self.__socialObj.getPeopleCount() * self.__socialObj.getT()
            needs_to_removed = len(self.__data) - max_records_length
            # shrink list size when history no longer needed
            if needs_to_removed > 0:
                self.__data = self.__data[needs_to_removed:]

    def history(self, person_id):
        t = self.__socialObj.getT()

        # get the user list who connected to current person
        userlist = self.__socialObj.getUserNetwork(person_id)

        # reverse fetch the history up to max(T, data.count)
        # assuming the original data was time order.
        history = []
        print('\tConnected people count:{}'.format(len(userlist)))
        for p in reversed(self.__data):
            if len(history) == t:
                break

            if p['userid'] in userlist:
                history.append(p)

        return history #[p for p in self.__data if p['userid'] in userlist]

    # calculate the mean and sd of user's social network
    def summaryGroup(self, person_id):
        history = self.history(person_id)

        # no history? return minimal value
        if len(history) == 0:
            return 0.00, 0.00

        #extra_records_count = len(history) - self.__socialObj.getT()
        #if extra_records_count > 0: history = history[extra_records_count:]

        amounts = [p['amount'] for p in history]

        mean = float(sum(amounts)) / len(amounts)
        amounts = list(map((lambda x: (mean - x) ** 2), amounts))
        sd = sqrt(float(sum(amounts)) / len(amounts))

        print('\tFetched purchase history count: {}, mean={:.2f}, sd={:.2f}'.format(len(history), mean, sd))

        return mean, sd

    def status(self):
        max_records_length = self.__socialObj.getPeopleCount() * self.__socialObj.getT()
        print('Purchase:: Total records: {}, capacity: {}'.format(len(self.__data), max_records_length))

'''
Core component
Process every events: purchase/befriend/unfriend
maintain user list, purchase object
user dictionary = { 1: Person Object, 2: Person Object, 3: Person Object ...... }
'''
class SocialNetwork:
    def __init__(self, paramD, paramT, flag_purchase_instance):
        self._paramD = int(paramD)
        self._paramT = int(paramT)
        self.__flagPurchase = flag_purchase_instance
        self.__people = {}
        self.__purchases = Purchase(self)

    def getD(self):
        return self._paramD
    def getT(self):
        return self._paramT
    def getPeopleCount(self):
        return len(self.__people)

    # similar to add a edge into a graph
    # when target user doesn't exist, create a new person object
    def beFriends(self, ifrom, ito):
        p1 = self.getPerson(ifrom, create_if_nx=True)
        p2 = self.getPerson(ito, create_if_nx=True)
        p1.beFriendWith(ito)
        p2.beFriendWith(ifrom)

    # person must exist, otherwise, ignore
    def unFriends(self, ifrom, ito):
        p1 = self.getPerson(ifrom)
        p2 = self.getPerson(ito)
        if p1: p1.unFriendWith(ito)
        if p2: p2.unFriendWith(ifrom)

    # use dictionary/hash table for fast retrieve Person
    def getPerson(self, person_id, create_if_nx = False):
        person = None
        try:
            person = self.__people[person_id]
        except KeyError:
            if create_if_nx:
                person = Person(person_id)
                self.__people[person_id] = person
            else:
                print('The user id {} not exist in system'.format(person_id))

        return person

    # generate a user id list belong to user's social network
    # recursively called until Degree == 0
    def getUserNetwork(self, person_id):
        userlist = self.__retrive_user_network__(degree=self._paramD, person_id=person_id)
        # unique person id
        return list(set(userlist))

    # Recursion body
    def __retrive_user_network__(self, degree, person_id):
        # end condition 1
        if degree == 0:
            return []

        # end condition 2, can't find that person, exception
        person = self.getPerson(person_id)
        if not person: return []

        # end condition 3, this person is alone .....
        userlist = person.network()
        if len(userlist) == 0: return []

        degree -= 1
        if degree > 0:
            waiting_list = userlist[:]
            for u in waiting_list:
                # adjacent list will contain repeat user id, use set to unique them
                userlist.extend(set(self.__retrive_user_network__(degree=degree, person_id=u)))

        return userlist

    # processing purchase data
    # this function will be called during the building network(batch log) and stream log(running)
    def __process_purchase__(self, event, action):

        # when initializing network, it's not necessary to mark purchases
        if action == EventAction.processing:
            mean, sd = self.__purchases.summaryGroup(event.id1)
            if event.amount > (mean + 3 * sd):
                print('\tFlagged a purchase, by user:{}, amount:{:.2f}, mean:{:.2f}, sd:{:.2f}'.format(
                    event.id1, event.amount, mean, sd
                ))
                self.__flagPurchase.append(event.toString(mean, sd))

        self.__purchases.append(event, action)

    # Main event processor
    def processEvent(self, json_object, action):

        # create event object
        e = Event(json_object)
        if not e.valid:
            print('Invalid event, ignore --- {}'.format(json_object))
            return

        if action is not EventAction.initializing:
            print('SocialNetwork::processEvent -- {}'.format(e.toString()))

        # event dispatch
        if e.type is EventType.befriend:
            self.beFriends(e.id1, e.id2)
        elif e.type is EventType.unfriend:
            self.unFriends(e.id1, e.id2)
        elif e.type is EventType.purchase:
            self.__process_purchase__(e, action)

    def status(self, show_graph=False):
        print('SocialNetwork::status, D:{}, T:{}, people count: {}'.format(self._paramD, self._paramT, len(self.__people)))
        if show_graph:
            for p in self.__people.values():
                line = "\t" + str(p.getId())
                network = p.network()
                if len(network) > 0:
                    line += ' --> '
                    line += ','.join(str(x) for x in network)
                print(line)
        self.__purchases.status()
