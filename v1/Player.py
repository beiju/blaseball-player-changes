class Player:
    def __init__(self, chron_entry):
        self.entityId = chron_entry['entityId']
        self.hash = chron_entry['hash']
        self.validFrom = chron_entry['validFrom']
        self.data = chron_entry['data']