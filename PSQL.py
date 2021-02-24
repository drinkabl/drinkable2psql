import psycopg2.extras as extras
import psycopg2
import logging
import threading
import sys
from time import sleep
import json

class PSQL:
   def __init__(self, host, port, database, user, password, sleep_between_send=10):
      self.login = {
         "host": host,
         "port": port,
         "dbname": database,
         "user": user,
         "password": password
      }

      self.sleep_between_send = sleep_between_send
      logging.debug(f'Sleeps between PSQL commits of {sleep_between_send}s')

      self.conn = self.connect()
      logging.debug(f'PSQL connected and ready')

      self.jobs = []

      self.worker_should_stop = False
      self.worker = threading.Thread(target=self.process)
      self.worker.start()

   def connect(self):
      """ Connect to the PostgreSQL database server """
      conn = None
      try:
         # connect to the PostgreSQL server
         logging.debug('Connecting to the PostgreSQL database...')
         conn = psycopg2.connect(**self.login)
      except (Exception, psycopg2.DatabaseError) as error:
         logging.critical(error)
         sys.exit(1) 
      logging.info("Connection to PSQL successful")
      return conn

   def save_update(self, seq, ts):
      logging.debug(f'Saving the status of sequence {seq} with timestamp {ts}')
      query = 'INSERT INTO osm_sequence ("seq", "timestamp") VALUES(%s, %s);'

      cursor = self.conn.cursor()
      cursor.execute(query, (seq, ts))
      self.conn.commit()
      cursor.close()

   def get_seq_number(self):
      query = 'SELECT * FROM osm_sequence ORDER BY seq DESC;'

      cursor = self.conn.cursor(cursor_factory = extras.DictCursor)
      cursor.execute(query)
      version = cursor.fetchone()
      cursor.close()

      if version is None:
         return None

      logging.debug(f'Getting current sequence for the database: {version["seq"]}')

      return version['seq']

   def execute_batch(self, query, vals, page_size):
      cursor = self.conn.cursor()
      extras.execute_batch(cursor, query, vals, page_size)
      self.conn.commit()
      cursor.close()

   def insert_state_batch(self, vals, page_size=500):
      logging.debug(f'Inserting {len(vals)}')
      query = 'INSERT INTO osm_nodes ("id", "timestamp", "lat", "lon", "uid", "user", "version", "tags") VALUES (%s,%s,%s,%s,%s,%s,%s,%s);'
      self.execute_batch(query, vals, page_size)
      
   def update_state_batch(self, vals, page_size=500):
      logging.debug(f'Updating {len(vals)}')
      query = 'UPDATE osm_nodes SET "timestamp" = %s, "lat" = %s, "lon" = %s, "uid" = %s, "user" = %s, "version" = %s, "tags" = %s WHERE "id" = %s;'
      self.execute_batch(query, vals, page_size)

   def delete_state_batch(self, vals, page_size=500):
      logging.debug(f'Deleting {len(vals)}')
      query = 'DELETE FROM osm_nodes WHERE "id" = %s'
      self.execute_batch(query, vals, page_size)

   @staticmethod
   def to_state_insert(jobs):
      return [(x['id'], x['timestamp'], x['lat'], x['lon'], x['uid'], x['user'], x['version'], json.dumps(x['tags'])) for x in jobs]

   @staticmethod
   def to_state_update(jobs):
      return [(x['timestamp'], x['lat'], x['lon'], x['uid'], x['user'], x['version'], json.dumps(x['tags']), x['id']) for x in jobs]

   @staticmethod
   def to_state_delete(jobs):
      return [(x['id']) for x in jobs]

   def process(self):
      while not self.worker_should_stop:
         sleep(self.sleep_between_send)
         if len(self.jobs) == 0:
            logging.debug('Nothing to do')
            continue

         jobs = self.jobs
         self.jobs = self.jobs[len(jobs):]

         adds = [x[1] for x in jobs if x[0] == 'add']
         deletes = [x[1] for x in jobs if x[0] == 'delete']
         updates = [x[1] for x in jobs if x[0] == 'update']

         logging.debug(f'sending {len(adds)} adds, {len(deletes)} deletes, {len(updates)} updates')

         if len(adds) > 0:
            values = PSQL.to_state_insert(adds)
            self.insert_state_batch(values)

         if len(deletes) > 0:
            values = PSQL.to_state_delete(deletes)
            self.delete_state_batch(values)
         
         if len(updates) > 0:
            values = PSQL.to_state_update(updates)
            self.update_state_batch(values)


   def add_job(self, event_type, job):
      self.jobs.append((event_type, job))

   def stop(self):
      while len(self.jobs) > 0:
         sleep(self.sleep_between_send)
         
      self.worker_should_stop = True
      self.conn.close()