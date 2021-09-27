from blaseball_mike.chronicler import paged_get_lazy

from ChangeSource import ChangeSourceType
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
        # Don't print these changes because they clutter up the output
        if (len(val.sources) == 1 and val.sources[0].source_type in {
            ChangeSourceType.TRAJ_RESET,
            ChangeSourceType.HITS_TRACKER,
        }):
            continue

        if val is not None:
            print(i, val.after['name'], val.valid_from, val.sources)
        else:
            print(i, None)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
