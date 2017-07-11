from enum import Enum

class Person:
    def __int__(self):
        self.__id = None
        self.__adjacences = []

    def beFriendWith(self, person_id):
        if person_id not in self.__adjacences:
            self.__adjacences.append(person_id)

    def unFriendWith(self, person_id):
        self.__adjacences.remove(person_id)

class NetworkOperation(Enum):
    BeFriends = 1
    UnFriends = 2

class SocialNetwork:
    def __int__(self):
        self.__people = {}


    def __op_friends(self, operation, ifrom, ito):
        perFrom, perTo = self.getPerson(ifrom), self.getPerson(ito)

        if perFrom is None or perTo is None:
            print('Operation failed!')
            return

        if operation is NetworkOperation.BeFriends:
            perFrom.beFriendWith(ito)
            perTo.beFriendWith(ifrom)
        elif operation is NetworkOperation.UnFriends:
            perFrom.unFriendWith(ito)
            perTo.unFriendWith(ifrom)

    def beFriends(self, ifrom, ito):
        self.__op_friends(NetworkOperation.BeFriends, ifrom, ito)

    def unFriends(self, ifrom, ito):
        self.__op_friends(NetworkOperation.UnFriends, ifrom, ito)

    def getPerson(self, person_id):
        person = None
        try:
            person = self.__people[person_id]
        except KeyError:
            print('The user id {} not exist in system'.format(person_id))

        return person