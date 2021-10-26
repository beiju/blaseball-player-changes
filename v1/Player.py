from typing import List


class Player:
    def __init__(self, chron_entry):
        self.entityId = chron_entry['entityId']
        self.hash = chron_entry['hash']
        self.validFrom = chron_entry['validFrom']
        self.data = chron_entry['data']

    def set_state(self, path: List[str], value):
        obj = self.data['state']
        for component in path[:-1]:
            if component not in obj:
                obj[component] = {}
            obj = obj[component]
        obj[path[-1]] = value

    def increment_counter(self, path: List[str]):
        obj = self.data
        for component in path[:-1]:
            if component not in obj:
                obj[component] = {}
            obj = obj[component]
        obj[path[-1]] = obj.get(path[-1], 0) + 1

    def reset_counter(self, path: List[str]):
        obj = self.data
        for component in path[:-1]:
            if component not in obj:
                obj[component] = {}
            obj = obj[component]
        obj[path[-1]] = 0
