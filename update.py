
from OSM import OSM
import datetime as dt
import json

class Updater:
   def __init__(self, base_url, mode, current_seq_num, filt, psql):
      self.base_url = base_url
      self.mode = mode
      self.current_seq_num = current_seq_num
      self.filter = filt
      self.psql = psql

   def get_latest_state_info(self):
      return OSM.get_latest_state_info(self.base_url, self.mode)
   
   def get_state_info(self, state_number):
      return OSM.get_state_info(self.base_url, self.mode)

   def download_state(self, state_number):
      return OSM.download_state(self.base_url, self.mode, state_number)

   def update(self):
      latest_info_seq_num, latest_info_timestamp = self.get_latest_state_info()
      print(f'Latest state sequence number "{latest_info_seq_num}" on date {latest_info_timestamp.strftime("%Y/%m/%dT%H:%M:%SZ")}')
      print(f'We need to download {latest_info_seq_num - self.current_seq_num} states')

      if latest_info_seq_num - self.current_seq_num == 0:
         return print('Up to date!')
      elif latest_info_seq_num - self.current_seq_num < 0:
         return print('Your version is newer that the one on the server')


      for state_num in range(self.current_seq_num + 1, latest_info_seq_num+1):
         xml = self.download_state(state_num)
         
         modifies = xml.findall('modify')
         adds = xml.findall('add')
         deletes = xml.findall('delete')

         modified_nodes = [x for modify in modifies for x in OSM.get_nodes(modify) ]
         added_nodes = [x for add in adds for x in OSM.get_nodes(add)]
         deleted_nodes = [x for delete in deletes for x in OSM.get_nodes(delete)]

         
         fountains_modified = [x for x in modified_nodes if self.filter.apply(x['tags'])]
         fountains_added = [x for x in added_nodes if self.filter.apply(x['tags'])]
         fountains_deleted = [x for x in deleted_nodes if self.filter.apply(x['tags'])]

         for n in fountains_modified:
            self.psql.add_job('update', {
                  "id": n['id'],
                  "timestamp": dt.datetime.strptime(n['timestamp'], '%Y-%m-%dT%H:%M:%SZ'),
                  "lat": n['lat'],
                  "lon": n['lon'],
                  "uid": n['uid'],
                  "user": n['user'],
                  "version": n['version'],
                  "tags": n['tags']
            })

         for n in fountains_added:
            self.psql.add_job('update', {
                  "id": n['id'],
                  "timestamp": dt.datetime.strptime(n['timestamp'], '%Y-%m-%dT%H:%M:%SZ'),
                  "lat": n['lat'],
                  "lon": n['lon'],
                  "uid": n['uid'],
                  "user": n['user'],
                  "version": n['version'],
                  "tags": n['tags']
            })

         for n in fountains_deleted:
            self.psql.add_job('update', {
                  "id": n['id'],
                  "timestamp": dt.datetime.strptime(n['timestamp'], '%Y-%m-%dT%H:%M:%SZ'),
                  "lat": n['lat'],
                  "lon": n['lon'],
                  "uid": n['uid'],
                  "user": n['user'],
                  "version": n['version'],
                  "tags": n['tags']
            })

         sequence_number, state_timestamp = OSM.get_state_info(self.base_url, self.mode, state_num)

         self.psql.save_update(state_num, state_timestamp)
         print(f'Updated database to version {state_num}. Added {len(fountains_added)}, updated {len(fountains_modified)} and deleted {len(fountains_deleted)} fountains.')
