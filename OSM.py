import logging
import urllib.request
from datetime import datetime


class OSM:
   @staticmethod
   def timestamp_to_datetime(timestamp):
      logging.debug(f'Converting {timestamp} into a valid datetime')
      return datetime.strptime(timestamp.replace('\\', ''), '%Y-%m-%dT%H:%M:%SZ')

   @staticmethod
   def sequence_to_path(sequence_number):
      sequence_pad = ('0' * (9 - len(str(sequence_number)))) + str(sequence_number)
      path = f'{sequence_pad[0:3]}/{sequence_pad[3:6]}/{sequence_pad[6:9]}'
      logging.debug(f'{sequence_number} padded and converted to {path}')
      return path

   @staticmethod
   def decode_state_info(state):
      logging.debug(f'Decoding state information for state')
      rows = state.split('\n')
      content = {}
      for row in rows:
         row = row.strip()
         if len(row) == 0 or row[0] == '#':
            continue
         key, value = row.split('=')
         content[key] = value
      
      if not 'timestamp' in content:
         raise f'Timestamp not found in state'
      if not 'sequenceNumber' in content:
         raise f'sequenceNumber not found in state'
   
      content['timestamp'] = OSM.timestamp_to_datetime(content['timestamp'])
      content['sequenceNumber'] = int(content['sequenceNumber'])
      return content

   @staticmethod
   def download_state_info(url):
      logging.debug(f'Downloading {url}')
      with urllib.request.urlopen(url) as response:
         return response.read().decode('utf8')

   @staticmethod
   def get_state_info(url):
      logging.debug(f'Getting state information for url {url}')
      state = OSM.download_state_info(url)
      state_dict = OSM.decode_state_info(state)
      return state_dict
   
   @staticmethod
   def get_latest_state_info(base_url, mode):
      logging.debug(f'Getting most recent state information')
      url = f'{base_url}/{mode}/state.txt'
      state = OSM.get_state_info(url)
      logging.debug(f'latest state has the following content: {state}')
      return state

   @staticmethod
   def get_state_info_from_sequence(base_url, mode, sequence_number):
      logging.debug(f'Getting state information for sequence {sequence_number}')
      sequence_path = OSM.sequence_to_path(sequence_number)
      url = f'{base_url}/{mode}/{sequence_path}.state.txt'
      state = OSM.get_state_info(url)
      logging.debug(f'state {sequence_number} has the following content: {state}')
      return state

   @staticmethod
   def get_closest_state(base_url, mode, timestamp):
      latest_state = OSM.get_latest_state_info(base_url, mode)
      high = latest_state['sequenceNumber']
      low = 0
      mid = 0

      while low <= high:
         mid = (high + low) // 2
         state_mid = OSM.get_state_info_from_sequence(base_url, mode, mid)

         if state_mid['timestamp'] < timestamp:
            low = mid + 1
         elif state_mid['timestamp'] > timestamp:
            high = mid - 1
         else:
            return state_mid
            
      if state_mid is None:
         raise 'Cannot find any matching state for the given timestamp. Is the planet file too old?'

      # make sure we actually take a smaller value
      if state_mid['timestamp'] > timestamp:
         state_mid = OSM.get_state_info_from_sequence(base_url, mode, mid - 1)

      logging.debug(f'Getting closest state from timestamp: given {timestamp}, we found {state_mid}')

      return state_mid
