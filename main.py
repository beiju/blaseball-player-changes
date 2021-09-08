from blaseball_mike.chronicler import paged_get_lazy
from blaseball_mike.session import session

from find_changes import get_change

CHRON_VERSIONS_URL = "http://127.0.0.1:8000/vcr/v2/versions"


def main():
    s = session(None)
    outputs = map(get_change, paged_get_lazy(CHRON_VERSIONS_URL, {
        'type': 'player',
        'order': 'asc'
    }, s))

    # Just to consume iterator
    for val in outputs:
        print(val)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
