import json
import argparse

from init import DrinkingWaterHandler
from update import Updater
from tag_filter import Filter
from OSM import OSM
from psql import PSQL
from osmium.io import Reader as oreader
import datetime as dt


def initialize(planet, base_url, mode, filt, host, port, database, user, password):
   try:
      print("Starting initialization")

      r = oreader(planet)
      header = r.header()
      planet_ts = header.get("osmosis_replication_timestamp")
      r.close()

      print(planet_ts)

      if planet_ts is None:
         raise Exception('Planet timestamp cannot be None')
      print("Planet timestamp:", planet_ts)

      planet_ts = dt.datetime.strptime(planet_ts, '%Y-%m-%dT%H:%M:%SZ')

      p = PSQL(host, port, database, user, password)

      p.create_tables()

      h = DrinkingWaterHandler(filt, p)
      h.apply_file(planet)
      
      planet_seq, state_timestamp = OSM.calculate_state_from_timestamp(base_url, mode, planet_ts)
      
      print("Saving current state", planet_seq, state_timestamp)
      p.save_update(planet_seq, state_timestamp)
   finally:
      p.stop()
      h.stop()

def update(base_url, mode, filt, host, port, database, user, password):
   try:
      p = PSQL(host, port, database, user, password)
      current_seq_num = p.get_seq_number()
      
      u = Updater(base_url, mode, current_seq_num, filt, p)
      u.update()
   finally:
      p.stop()

def main():
   parser = argparse.ArgumentParser(description='Initialize or update a Postgres database with OSM data')

   parser.add_argument('-i', '--init', dest='initialize', action='store_const', const=True, default=False, help='Initialize the database and supply a planet file')
   parser.add_argument('-u', '--update', dest='update', action='store_const', const=True, default=False, help='Update and initialized database by downloading updates')
   
   parser.add_argument('-f', '--filter', dest='filter_path', default='./config/filter.json', help='Filter file')
   
   parser.add_argument('-b', '--base_url', dest='base_url', default='https://planet.osm.ch/replication', help='Planet website. Used both in init and update')
   parser.add_argument('-m', '--mode', dest='mode', default='hour', help='replication mode (hour, minute...). Used both in init and update')


   parser.add_argument('-ph', '--psqlhost', dest='psqlhost', required=True, help='Postgres host')
   parser.add_argument('-ppo', '--psqlport', dest='psqlport', default=5432, help='Postgres port')
   parser.add_argument('-pd', '--psqldb', dest='psqldb', required=True, help='Postgres database')
   parser.add_argument('-pu', '--psqluser', dest='psqluser', required=True, help='Postgres user')
   parser.add_argument('-pp', '--psqlpassword', dest='psqlpassword', required=True, help='Postgres password')

   parser.add_argument('-p', '--planet', dest='planet', default=None, help='Planet file in pbf format. Option valid only if --init is used')

   args = parser.parse_args()

   if args.initialize and args.update:
      return print('Select either --init or --update, not both')
   if not args.initialize and not args.update:
      return print('Select one of --init or --update')
   if args.initialize and args.planet is None:
      return print('To initialize, we need a planet file')
   if args.update and args.planet:
      print('Ignoring planet file during updatess')

   filt = Filter(args.filter_path)

   print(args)

   if args.initialize:
      initialize(args.planet, args.base_url, args.mode, filt, args.psqlhost, args.psqlport, args.psqldb, args.psqluser, args.psqlpassword)
   elif args.update:
      update(args.base_url, args.mode, filt, args.psqlhost, args.psqlport, args.psqldb, args.psqluser, args.psqlpassword)

if __name__ == '__main__':
   main()