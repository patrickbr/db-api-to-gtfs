# db-to-gtfs

Convert the timetable data published through the [DB API](http://data.deutschebahn.com/apis/fahrplan/) to a GTFS feed.

An example feed valid from February 28th to May 1rst can be found [here](http://patrickbrosi.de/de/projects/dbgtfs).

**Update:** A complete GTFS feed for Germany (including busses, subways, trams, long distance, regional and urban rail) can now be found here: [http://gtfs.de](http://gtfs.de)

## Usage

Get a GTFS feed valid from May 1st to August 1st 2016

    $ ./db_to_gtfs.py --api-key <your api key> --start-date 2016-5-1 --end-date 2016-8-1

### Method

The script begins at seed stations (Berlin Hbf, by default) and crawls to every station reachable by trips from the seed stations. Trips, routes and service dates are collected on the way and ultimately written to the GTFS files.
    
## Flags
See

    $ ./db_to_gtfs.py --help
    
for available command line arguments.

## Obtain API key

See http://data.deutschebahn.com/apis/fahrplan/

## License

See LICENSE.
