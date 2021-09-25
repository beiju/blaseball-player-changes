from blaseball_mike.chronicler import paged_get_lazy

from find_changes import get_change, session

# CHRON_VERSIONS_URL = "http://127.0.0.1:8000/vcr/v2/versions"
CHRON_VERSIONS_URL = "https://api.sibr.dev/chronicler/v2/versions"


def main():
    outputs = map(get_change, paged_get_lazy(CHRON_VERSIONS_URL, {
        'type': 'player',
        'order': 'asc',
    }, session))

    # Just to consume iterator
    for i, val in enumerate(outputs):
        if val is not None:
            print(i, val.after['name'], val.valid_from, val.sources)
        else:
            print(i, None)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
