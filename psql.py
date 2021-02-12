import psycopg2.extras as extras
import psycopg2
import threading
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

      self.conn = self.connect()

      self.jobs = []

      self.thread_process_kill = False
      self.thread_process = threading.Thread(target=self.process)
      self.thread_process.start()

   def connect(self):
      """ Connect to the PostgreSQL database server """
      conn = None
      try:
         # connect to the PostgreSQL server
         print('Connecting to the PostgreSQL database...')
         conn = psycopg2.connect(**self.login)
      except (Exception, psycopg2.DatabaseError) as error:
         print(error)
         sys.exit(1) 
      print("Connection to PSQL successful")
      return conn

   def create_tables(self):
      cursor = self.conn.cursor()
      cursor.execute(open("./table.sql", "r").read())
      self.conn.commit()
      cursor.close()
      
   def save_update(self, seq, ts):
      query = 'INSERT INTO osm_sequence ("seq", "timestamp") VALUES(%s, %s);'

      cursor = self.conn.cursor()
      cursor.execute(query, (seq, ts))
      self.conn.commit()
      cursor.close()

   def get_seq_number(self):
      query = 'SELECT * FROM osm_sequence ORDER BY seq DESC;'

      cursor = self.conn.cursor()
      cursor.execute(query)
      version = cursor.fetchone()
      cursor.close()

      if len(version) == 0:
         return None

      return version[0]

   def execute_batch(self, query, vals, page_size):
      cursor = self.conn.cursor()
      extras.execute_batch(cursor, query, vals, page_size)
      self.conn.commit()
      cursor.close()

   def insert_event_batch(self, vals, page_size=500):
      query = 'INSERT INTO osm_nodes_event ("event_type", "id", "timestamp", "lat", "lon", "uid", "user", "version", "tags") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);'
      self.execute_batch(query, vals, page_size)

   def insert_state_batch(self, vals, page_size=500):
      query = 'INSERT INTO osm_nodes_state ("id", "timestamp", "lat", "lon", "uid", "user", "version", "tags") VALUES (%s,%s,%s,%s,%s,%s,%s,%s);'
      self.execute_batch(query, vals, page_size)
      
   def update_state_batch(self, vals, page_size=500):
      query = 'UPDATE osm_nodes_state SET "timestamp" = %s, "lat" = %s, "lon" = %s, "uid" = %s, "user" = %s, "version" = %s, "tags" = %s WHERE "id" = %s;'
      self.execute_batch(query, vals, page_size)

   def delete_state_batch(self, vals, page_size=500):
      query = 'DELETE FROM osm_nodes_state WHERE "id" = %s'
      self.execute_batch(query, vals, page_size)

   def to_event_values(self, event_type, jobs):
      return [(event_type, x['id'], x['timestamp'], x['lat'], x['lon'], x['uid'], x['user'], x['version'], json.dumps(x['tags'])) for x in jobs]

   def to_state_insert(self, jobs):
      return [(x['id'], x['timestamp'], x['lat'], x['lon'], x['uid'], x['user'], x['version'], json.dumps(x['tags'])) for x in jobs]

   def to_state_update(self, jobs):
      return [(x['timestamp'], x['lat'], x['lon'], x['uid'], x['user'], x['version'], json.dumps(x['tags']), x['id']) for x in jobs]

   def to_state_delete(self, jobs):
      return [(x['id']) for x in jobs]

   def process(self):
      while not self.thread_process_kill:
         sleep(self.sleep_between_send)
         if len(self.jobs) == 0:
            continue

         jobs = self.jobs
         self.jobs = self.jobs[len(jobs):]

         adds = [x[1] for x in jobs if x[0] == 'add']
         deletes = [x[1] for x in jobs if x[0] == 'delete']
         updates = [x[1] for x in jobs if x[0] == 'update']

         print('sending ', len(adds), 'adds', len(deletes), 'deletes', len(updates), 'updates')


         self.insert_event_batch(self.to_event_values('add', adds) + self.to_event_values('delete', deletes) + self.to_event_values('update', updates))

         if len(adds) > 0:
            values = self.to_state_insert(adds)
            self.insert_state_batch(values)

         if len(deletes) > 0:
            values = self.to_state_delete(deletes)
            self.delete_state_batch(values)
         
         if len(updates) > 0:
            values = self.to_state_update(updates)
            self.update_state_batch(values)


   def add_job(self, event_type, job):
      self.jobs.append((event_type, job))

   def stop(self):
      while len(self.jobs) > 0:
         sleep(self.sleep_between_send)
         
      self.thread_process_kill = True
      self.conn.close()