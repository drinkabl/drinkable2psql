import logging
import sys
import math 

from osmium.io import Reader as oreader
import osmium
import threading
from time import sleep

from OSM import OSM


class Initializer():
   def __init__(self, options, psql, filt):
      self.options = options
      self.psql = psql
      self.base_url = options.base_url
      self.mode = options.mode
      self.filter = filt
      self.planet_path = options.planet_path

   def get_planet_info(self):
      try:
         reader = oreader(self.planet_path)
         header = reader.header()
         timestamp = header.get("osmosis_replication_timestamp")
         reader.close()
      except Exception as e:
         logging.critical(f'There was a problem while parsing planet file. Pheraps the folder provided is wrong', exc_info=True)
         raise e

      if timestamp is None:
         logging.critical(f'Timestamp from planet header is None. It is impossible to continue.')
         raise Exception('Timestamp from planet header is None. It is impossible to continue.')
      
      logging.info(f'The headers of planet are valid and we found the following timestamp: {timestamp}')
      timestamp_py = OSM.timestamp_to_datetime(timestamp)
      state_planet = OSM.get_closest_state(self.base_url, self.mode, timestamp_py)
      logging.info(f'We were able to find the planet state: {state_planet["sequenceNumber"]} - Header timestamp: {timestamp_py}, state timestamp: {state_planet["timestamp"]}')

      return timestamp_py, state_planet
   def start(self):
      planet_timestamp, planet_state = self.get_planet_info()
      planet_decoder = PlanetDecoder(self.filter, self.psql)
      planet_decoder.apply_file(self.planet_path)
      planet_decoder.stop()

      self.psql.save_update(planet_state['sequenceNumber'], planet_state['timestamp'])


class PlanetDecoder(osmium.SimpleHandler):
   def __init__(self, filt, psql):
      super(PlanetDecoder, self).__init__()
      self.filter = filt
      self.psql = psql
      self.processed_nodes = 0
      self.processed_matching_filter = 0

      self.stat_last_time_processed_nodes = 0
      self.stat_thread_should_stop = False
      self.stat_thread = threading.Thread(target=self.statistics)
      self.stat_thread.start()

   def should_keep(self, tags):
      return self.filter.apply(tags)

   def node(self, n):
      self.processed_nodes += 1

      if self.should_keep(n.tags):
         self.processed_matching_filter += 1

         tags = {x.k: x.v for x in n.tags}
         self.psql.add_job('add', {
            "id": n.id,
            "timestamp": n.timestamp,
            "lat": n.location.lat_without_check(),
            "lon": n.location.lon_without_check(),
            "uid": n.uid,
            "user": n.user,
            "version": n.version,
            "tags": tags
         })

   def statistics(self):
      logging.debug('Statistic thread started')
      while not self.stat_thread_should_stop:
         sleep(1)
         processed_since_last_time = self.processed_nodes - self.stat_last_time_processed_nodes
         self.stat_last_time_processed_nodes = self.processed_nodes

         logging.info(f'Processing {math.floor(processed_since_last_time / 1000)}k/s - found: {self.processed_matching_filter} nodes matching the criteria')

   def stop(self):
      logging.debug('Stopping thread')
      self.stat_thread_should_stop = True