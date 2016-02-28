# db-to-gtfs

Convert the timetable data published through the [DB API](http://data.deutschebahn.com/apis/fahrplan/) to a GTFS feed.

## Usage

Get a GTFS feed valid from May 1st to August 1st 2016

    $ ./db_to_gtfs.py --start-date 2016-5-1 --end-date 2016-8-1

### Method

The script begins at seed stations (Berlin Hbf, by default) and crawls to every station reachable by trips from the seed stations. Trips, routes and service dates are collected on the way and finally written to the GTFS files.
    
## Flags
See

    $ ./db_to_gtfs.py --help
    
for available command line arguments.

## License

See LICENSE.
