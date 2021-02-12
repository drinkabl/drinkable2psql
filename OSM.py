import urllib.request
import datetime as dt
import threading
from io import BytesIO
import gzip
import xml.etree.ElementTree as ET


class OSM:
   @staticmethod
   def state_number_decompose(state_number):
      url_state = ('0' * (9 - len(str(state_number)))) + str(state_number)
      
      part1 = url_state[0:3]
      part2 = url_state[3:6]
      name = url_state[6:9]

      return {"part1": part1, "part2": part2, "name": name}

   @staticmethod
   def extract_state_info(url):
      sequence_number = None
      timestamp = None

      with urllib.request.urlopen(url) as response:
         state = response.read().decode('utf8')
         lines = state.split('\n')
         for line in lines:
            if len(line) == 0 or line[0] == '#':
               continue
            try:
               key, value = line.split('=')
               if key == 'sequenceNumber':
                  sequence_number = int(value)
               elif key == 'timestamp':
                  timestamp = dt.datetime.strptime(value.replace('\\', ''), '%Y-%m-%dT%H:%M:%SZ')
            except Exception as e:
               print(e)
               continue

      if sequence_number is None:
         raise Exception(f'{url} has no sequence number')
      return sequence_number, timestamp

   @staticmethod
   def get_latest_state_info(base_url, mode):
      url = f'{base_url}/{mode}/state.txt'
      return OSM.extract_state_info(url)

   @staticmethod
   def get_state_info(base_url, mode, state_number):
      state_decomposed = OSM.state_number_decompose(state_number)
      url = f"{base_url}/{mode}/{state_decomposed['part1']}/{state_decomposed['part2']}/{state_decomposed['name']}.state.txt"
      return OSM.extract_state_info(url)

   @staticmethod
   def download_state(base_url, mode, state_number):
      state_decomposed = OSM.state_number_decompose(state_number)
      url = f"{base_url}/{mode}/{state_decomposed['part1']}/{state_decomposed['part2']}/{state_decomposed['name']}.osc.gz"
      with urllib.request.urlopen(url) as response:
         responseByte = BytesIO(response.read())
         with gzip.GzipFile(fileobj=responseByte) as oscFile:
            xml = ET.parse(oscFile).getroot()
            return xml

   @staticmethod
   def get_elem(xmlElement):
      tags = xmlElement.findall('tag')
      elem = xmlElement.attrib
      elem['tags'] = {tag.get('k'): tag.get('v') for tag in tags}
      
      return elem

   @staticmethod
   def get_nodes(xmlElement):
      return [OSM.get_elem(x) for x in xmlElement.findall('node')]

   @staticmethod
   def calculate_state_from_timestamp(base_url, mode, target_timestamp):
      (latest_seq_number, latest_state_timestamp) = OSM.get_latest_state_info(base_url, mode)
      if latest_state_timestamp.timestamp() < target_timestamp.timestamp():
         return latest_seq_number

      curr_seq_number = latest_seq_number
      while True:
         _, state_timestamp = OSM.get_state_info(base_url, mode, curr_seq_number)

         if state_timestamp.timestamp() < target_timestamp.timestamp():
            return curr_seq_number, state_timestamp
         curr_seq_number -= 1

         if curr_seq_number < 0:
            raise Exception("sequence number less then 0. That is totally unexpected")