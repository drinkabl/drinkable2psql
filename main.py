import argparse

from tag_filter import Filter
from PSQL import PSQL
from Initializer import Initializer
import logging

def setup_logging(debug):
   logger = logging.getLogger()
   if not debug:
      logger.setLevel(logging.INFO)
   else:
      logger.setLevel(logging.DEBUG)
   c_handler = logging.StreamHandler()
   c_format = logging.Formatter('[%(asctime)s] %(levelname)s - %(filename)s - %(message)s')
   c_handler.setFormatter(c_format)
   logger.addHandler(c_handler)


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

   parser.add_argument('-p', '--planet', dest='planet_path', default=None, help='Planet file in pbf format. Option valid only if --init is used')

   parser.add_argument('-d', '--debug', dest='debug', action='store_const', const=True, default=False, help='Debug mode')

   args = parser.parse_args()

   if args.initialize and args.update:
      return print('Select either --init or --update, not both')
   if not args.initialize and not args.update:
      return print('Select one of --init or --update')
   if args.initialize and args.planet_path is None:
      return print('To initialize, we need a planet file (-p)')
   if args.update and args.planet_path:
      print('Ignoring planet file during updatess')

   setup_logging(args.debug)
   logging.debug('Options parsed')

   logging.info(f'base url: {args.base_url}, mode: {args.mode}')

   filt = Filter(args.filter_path)
   if filt is None:
      return

   psql = PSQL(args.psqlhost, args.psqlport, args.psqldb, args.psqluser, args.psqlpassword)

   if args.initialize:
      try:
         initializer = Initializer(args, psql, filt)
         initializer.start()
      except:
         logging.critical('Something came up.', exc_info=True)
         return
   psql.stop()
   logging.state('Stopping PSQL. It might take some seconds')
   # elif args.update:
   #    update(args.base_url, args.mode, filt, args.psqlhost, args.psqlport, args.psqldb, args.psqluser, args.psqlpassword)

if __name__ == "__main__":
   main()   