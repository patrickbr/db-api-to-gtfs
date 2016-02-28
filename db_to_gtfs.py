#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
DB API to GTFS converter- (C) 2016 by Patrick Brosi

Harvests the Deutsche Bahn API and generates a GTFS feed

Usage:
  db_to_gtfs.py [--start-date=<date>] [--end-date=<date>] [--station-seed=<sids>] [--output-dir=<dir>]
  db_to_gtfs.py -h | --help
  db_to_gtfs.py --version

Options:
  -h --help                     Show this screen.
  --version                     Show version.
  --start-date=<date>           Start date of the feed (current date by default)
  --end-date=<date>             End date of the feed (start date + 3 days by default)
  --station-seed=<sids>         Comma separated list of seed stations [default: 8011160]
  --output-dir=<dir>            Output directory [default: .]
"""

import urllib2
import json
import string
import time
import unicodecsv as csv
import HTMLParser
import os
from dateutil.parser import parse as dateparse
from datetime import timedelta
from sets import Set

DEP_URL = 'http://open-api.bahn.de/bin/rest.exe/departureBoard?authKey=<insert api key here>&lang=de&id=00$id&date=$date&time=$time&format=json'
STATION_URL = 'http://open-api.bahn.de/bin/rest.exe/location.name?authKey=<insert api key here>&lang=de&input=$search&format=json'


class DBApiToGTFS(object):

    """A GTFS converter for the DB API"""

    def __init__(self, options):
        self.stops = {}
        self.trips = []
        self.routes = []
        self.agencies = []
        self.calendar_dates = []
        self.processed = []
        self.start_date = options['start_date']
        self.end_date = options['end_date']
        self.out_dir = options['output_dir']
        self.htmlparser = HTMLParser.HTMLParser()

    def process_station_by_id(self, sid):
        """Process a station by its id"""
        if not int(sid) in self.stops:
            # get the 'official' writing of station name
            det = self.get_station_detail(sid)
            if det == None:
                print 'Warning: station %d not found.' % sid
            self.process_station_by_ob(det)

    def process_station_by_ob(self, station):
        """Process a station by its object, already containing needed information"""
        if station == None:
            return
        if not int(station['id']) in self.stops:
            gtfs_station = self.get_station_ob(station)
            self.stops[int(station['id'])] = gtfs_station

    def get_all_trips_for_stop(self, stop):
        """Read all trips for a stop object"""
        if stop['trips_fetched']:
            return

        stop['trips_fetched'] = True

        while stop['last_date'] < self.end_date:
            print '@ #%d (%s) on %s %s, trips collected: %d' % (stop['stop_id'], stop['stop_name'], stop['last_date'].strftime('%Y-%m-%d'), str(stop['last_check_h']) + ':' + str(stop['last_check_m']), len(self.trips))
            requrl = string.Template(DEP_URL).substitute({
                'id': stop['stop_id'],
                'date': stop['last_date'].strftime('%Y-%m-%d'),
                'time': str(stop['last_check_h']) + ':' + str(stop['last_check_m'])
            })
            response = urllib2.urlopen(requrl)
            data = json.load(response)

            # if no trips have been found, add 24 hours to search timespan
            stop['last_date'] = stop['last_date'] + timedelta(days=1)

            if 'DepartureBoard' in data and 'Departure' in data['DepartureBoard']:
                # catch strange behavior of the DB xml-to-json converter...
                if type(data['DepartureBoard']['Departure']) is dict:
                    data['DepartureBoard']['Departure'] = [
                        data['DepartureBoard']['Departure']]

                for dep in data['DepartureBoard']['Departure']:
                    # update last check time...
                    if dep['time'].split(':')[0] > stop['last_check_h']:
                        stop['last_check_h'] = dep['time'].split(':')[0]
                    if dep['time'].split(':')[1] > stop['last_check_m']:
                        stop['last_check_m'] = dep['time'].split(':')[1]
                    if dateparse(dep['date']) > stop['last_date']:
                        stop['last_date'] = dateparse(dep['date'])

                    # only process deps for this stop
                    if int(dep['stopid']) == stop['stop_id']:
                        if not self.dep_processed(dep):
                            # we assume train ids (the 55 in "ICE 55") are
                            # unique per day!
                            self.process_trip(dep['JourneyDetailRef']['ref'])
                            self.processed.append(dep)

    def get_first_in_list(self, obj):
        """Expand the strange array format of the DB json format"""
        if type(obj) is list:
            return obj[0]
        return obj

    def dep_processed(self, dep):
        """Check if a dearture has already been processed"""
        for proced in self.processed:
            if proced['date'] == dep['date'] and proced['direction'] == dep['direction'] and proced['name'] == dep['name']:
                return True
        return False

    def fetch_json(self, url):
        """Fetch remote json"""

        # TODO: error handling, retry etc.
        response = urllib2.urlopen(url)
        data = json.load(response)

        return data

    def process_trip(self, trip_url):
        """Process a fetched trip"""
        data = self.fetch_json(trip_url)

        trip = {
            'stoptimes': [],
            'name': '',
            'type': '',
            'notes': [],
            'service_dates': Set()
        }

        if 'JourneyDetail' in data:
            # meta data
            trip['name'] = self.get_first_in_list(
                data['JourneyDetail']['Names']['Name']).get('name')
            trip['type'] = self.get_first_in_list(
                data['JourneyDetail']['Types']['Type']).get('type')
            trip['agency_id'] = self.get_first_in_list(
                data['JourneyDetail']['Operators']['Operator']).get('name')

            for stoptime in data['JourneyDetail']['Stops']['Stop']:
                # check if this station is known
                self.process_station_by_ob({
                    'name': stoptime['name'],
                    'id': stoptime['id'],
                    'lat': stoptime['lat'],
                    'lon': stoptime['lon']
                })

                trip['stoptimes'].append({
                    'stop_id': int(stoptime['id']),
                    'stop_sequence': int(stoptime['routeIdx']),
                    'arrival_time': stoptime['arrTime'] if 'arrTime' in stoptime else stoptime['depTime'],
                    'departure_time': stoptime['depTime'] if 'depTime' in stoptime else stoptime['arrTime'],
                    'arrival_date': stoptime['arrDate'] if 'arrDate' in stoptime else stoptime['depDate'],
                    'departure_date': stoptime['depDate'] if 'depDate' in stoptime else stoptime['arrDate'],
                })

                self.stops[int(stoptime['id'])]['has_trip'] = True

            trip['route_id'] = self.route_append(trip)

            # discard 'empty' trips
            if len(trip['stoptimes']) > 1:
                self.pack_trip(trip)
                self.trip_append(trip)

    def trip_compare(self, trip1, trip2):
        """Check if two trips are equal"""
        if trip1['name'] != trip2['name']:
            return False

        if trip1['type'] != trip2['type']:
            return False

        if len(trip1['stoptimes']) != len(trip2['stoptimes']):
            return False

        # check departures
        for i in range(0, len(trip1['stoptimes']) - 1):
            if trip1['stoptimes'][i]['stop_id'] != trip2['stoptimes'][i]['stop_id']:
                return False
            if trip1['stoptimes'][i]['departure_time'] != trip2['stoptimes'][i]['departure_time']:
                return False

        return True

    def trip_append(self, trip_add):
        """Append a trip or add the added trips calendar date to an existing, equal trip"""
        for trip in self.trips:
            if self.trip_compare(trip, trip_add):
                trip['service_dates'].add(trip_add['service_date'])
                return

        trip_add['service_dates'].add(trip_add['service_date'])
        self.trips.append(trip_add)

    # append route based on trip, return route id
    def route_append(self, trip):
        """Append a route based on a trip (if route is new), return route id"""
        short_name = trip['type']
        long_name = trip['name']
        agency_id = trip['agency_id']

        for rid, route in enumerate(self.routes):
            if route['route_long_name'] == long_name and route['agency_id'] == agency_id:
                return rid

        self.routes.append({
            'route_short_name': short_name,
            'route_long_name': long_name,
            'route_type': 2,
            'agency_id': agency_id
        })

        if not agency_id in self.agencies:
            self.agencies.append(agency_id)

        return len(self.routes) - 1

    # packs the trip, corrects overhanging times to GTFS format etc
    def pack_trip(self, trip):
        """Finalize trip"""
        start_date = dateparse(trip['stoptimes'][0]['departure_date'])

        for stoptime in trip['stoptimes']:
            # gtfs needs stoptimes overlapping into the following day to have
            # times like 25:35:00
            arrdelta = (dateparse(stoptime['arrival_date']) - start_date).days
            depdelta = (
                dateparse(stoptime['departure_date']) - start_date).days
            if arrdelta > 0:
                stoptime['arrival_date'] = start_date
                stoptime['arrival_time'] = str(int(stoptime['arrival_time'].split(':')[0]) + 24 * arrdelta) + ":" + stoptime[
                    'arrival_time'].split(':')[1]

            if depdelta > 0:
                stoptime['departure_date'] = start_date
                stoptime['departure_time'] = str(int(stoptime['departure_time'].split(':')[0]) + 24 * depdelta) + ":" + stoptime[
                    'departure_time'].split(':')[1]

        trip['headsign'] = self.stops[
            trip['stoptimes'][-1]['stop_id']]['stop_name']
        trip['service_date'] = start_date

    def get_station_detail(self, stat_id):
        """Return station detail from remote"""
        requrl = string.Template(STATION_URL).substitute({'search': stat_id})
        data = self.fetch_json(requrl)
        station = data.get('LocationList', {}).get('StopLocation')
        return station

    def get_station_ob(self, station_ob):
        """Get internal station object from remote DB object"""
        gtfsstation = {
            'stop_id': int(station_ob['id']),
            'stop_name': self.htmlparser.unescape(station_ob['name']),
            'stop_lat': float(station_ob['lat']),
            'stop_lon': float(station_ob['lon']),
            'last_check_h': 0,
            'last_check_m': 0,
            'last_date': self.start_date,
            'has_trip': False,
            'trips_fetched': False
        }
        return gtfsstation

    def generate_calendar_dates(self):
        """Generate minimized calendar dates"""
        for trip in self.trips:
            for sid, cdate in enumerate(self.calendar_dates):
                if cdate == trip['service_dates']:
                    trip['service_id'] = sid
                    break

            if 'service_id' not in trip:
                self.calendar_dates.append(trip['service_dates'])
                trip['service_id'] = len(self.calendar_dates) - 1

    def write_trips(self):
        """Write trips to file"""
        with open(os.path.join(self.out_dir, 'trips.txt'), 'wb') as fhandle, open(os.path.join(self.out_dir, 'stop_times.txt'), 'wb') as sfhandle:
            trip_fieldnames = [
                'route_id', 'service_id', 'trip_id', 'trip_headsign']
            trip_writer = csv.DictWriter(fhandle, delimiter=',',
                                         quotechar='"', fieldnames=trip_fieldnames)
            trip_writer.writeheader()

            stoptimes_fieldnames = [
                'trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']
            stoptimes_writer = csv.DictWriter(sfhandle, delimiter=',',
                                              quotechar='"', fieldnames=stoptimes_fieldnames)
            stoptimes_writer.writeheader()

            for tid, trip in enumerate(self.trips):
                trip_writer.writerow({
                    'route_id': tid,
                    'service_id': trip['service_id'],
                    'trip_id': tid,
                    'trip_headsign': trip['headsign']
                })

                for stoptime in trip['stoptimes']:
                    stoptimes_writer.writerow({
                        'trip_id': tid,
                        'arrival_time': stoptime['arrival_time'] + ':00',
                        'departure_time': stoptime['departure_time'] + ':00',
                        'stop_id': stoptime['stop_id'],
                        'stop_sequence': stoptime['stop_sequence']
                    })

    def write_routes(self):
        """Write routes to file"""
        with open(os.path.join(self.out_dir, 'routes.txt'), 'wb') as fhandle:
            route_fieldnames = [
                'route_id', 'route_short_name', 'route_long_name', 'route_type', 'agency_id']
            route_writer = csv.DictWriter(fhandle, delimiter=',',
                                          quotechar='"', fieldnames=route_fieldnames)
            route_writer.writeheader()

            for rid, route in enumerate(self.routes):
                route_writer.writerow({
                    'route_id': rid,
                    'route_short_name': route['route_short_name'],
                    'route_long_name': route['route_long_name'],
                    'route_type': route['route_type'],
                    'agency_id': route['agency_id']
                })

    def write_stops(self):
        """Write stops to file"""
        with open(os.path.join(self.out_dir, 'stops.txt'), 'wb') as fhandle:
            route_fieldnames = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
            route_writer = csv.DictWriter(fhandle, delimiter=',',
                                          quotechar='"', fieldnames=route_fieldnames)
            route_writer.writeheader()

            for stop in self.stops.itervalues():
                if not stop['has_trip']:
                    continue

                route_writer.writerow({
                    'stop_id': stop['stop_id'],
                    'stop_name': stop['stop_name'],
                    'stop_lat': stop['stop_lat'],
                    'stop_lon': stop['stop_lon']
                })

    def write_calendar_dates(self):
        """Write calendar dates to file"""
        with open(os.path.join(self.out_dir, 'calendar_dates.txt'), 'wb') as fhandle:
            calendar_fieldnames = ['service_id', 'date', 'exception_type']
            calendar_writer = csv.DictWriter(fhandle, delimiter=',',
                                             quotechar='"', fieldnames=calendar_fieldnames)
            calendar_writer.writeheader()

            for sid, cdate in enumerate(self.calendar_dates):
                for date in cdate:
                    calendar_writer.writerow({
                        'service_id': sid,
                        'date': date.strftime('%Y%m%d'),
                        'exception_type': 1
                    })

    def write_agencies(self):
        """Write agencies to file"""
        with open(os.path.join(self.out_dir, 'agency.txt'), 'wb') as fhandle:
            agency_fieldnames = [
                'agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang']
            agency_writer = csv.DictWriter(fhandle, delimiter=',',
                                           quotechar='"', fieldnames=agency_fieldnames)
            agency_writer.writeheader()

            for agency in self.agencies:
                agency_writer.writerow({
                    'agency_id': agency,
                    'agency_name': agency,
                    'agency_url': 'http://www.bahn.de',
                    'agency_timezone': 'Europe/Berlin',
                    'agency_lang': 'de'
                })

    def write_feed_info(self):
        """Write feed_info to file"""
        with open(os.path.join(self.out_dir, 'feed_info.txt'), 'wb') as fhandle:
            fieldnames = [
                'feed_publisher_name', 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date']
            writer = csv.DictWriter(fhandle, delimiter=',',
                                    quotechar='"', fieldnames=fieldnames)
            writer.writeheader()

            writer.writerow({
                'feed_publisher_name': 'DB-to-GTFS converter, based on DB-API data',
                'feed_publisher_url': 'http://www.patrickbrosi.de/de/dbgtfs',
                'feed_lang': 'de',
                'feed_start_date': self.start_date.strftime('%Y%m%d'),
                'feed_end_date': self.end_date.strftime('%Y%m%d')
            })

    def get_unfetched_station_id(self):
        """Return a station that has not yet been trip-processed"""
        for sid in self.stops:
            if not self.stops[sid]['trips_fetched']:
                return self.stops[sid]['stop_id']
        return None


def main(options=None):
    if not options['--start-date']:
        options['--start-date'] = time.strftime("%Y-%m-%d")
    if not options['--end-date']:
        options['--end-date'] = (
            dateparse(options['--start-date']) + timedelta(days=3)).strftime("%Y-%m-%d")

    converter = DBApiToGTFS({
        'start_date': dateparse(options['--start-date']),
        'end_date': dateparse(options['--end-date']),
        'output_dir': options['--output-dir']
    })

    station_seed = options['--station-seed'].split(',')

    print 'Generating GTFS feed from %s to %s' % (options['--start-date'], options['--end-date'])

    for seed in station_seed:
        converter.process_station_by_id(int(seed))

    sid = converter.get_unfetched_station_id()
    while sid:
        converter.get_all_trips_for_stop(converter.stops[sid])
        sid = converter.get_unfetched_station_id()

    converter.generate_calendar_dates()

    converter.write_trips()
    converter.write_stops()
    converter.write_calendar_dates()
    converter.write_routes()
    converter.write_agencies()
    converter.write_feed_info()

    print 'Done, written %d trips, %d routes, %s services, %s stops' % (len(converter.trips), len(converter.routes), len(converter.calendar_dates), len(converter.stops))

if __name__ == '__main__':
    from docopt import docopt

    arguments = docopt(
        __doc__, version='DB Api to GTFS converter 0,1, 2016 by Patrick Brosi')
    try:
        main(options=arguments)
    except KeyboardInterrupt:
        print "\nCancelled by user."
    exit(0)
