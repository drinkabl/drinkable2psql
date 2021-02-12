import osmium
import threading
from time import sleep
import math
import datetime as dt
import pytz

class DrinkingWaterHandler(osmium.SimpleHandler):
    def __init__(self, filt, psql):
        super(DrinkingWaterHandler, self).__init__()
        self.filter = filt
        self.psql = psql
        self.processed = 0
        self.processe_filtered = 0
        self.last_time_processed = 0

        self.thread_stats_kill = False
        self.thread_stats = threading.Thread(target=self.statistics)
        self.thread_stats.start()

    def should_keep(self, tags):
        return self.filter.apply(tags)

    def node(self, n):
        self.processed += 1

        if self.should_keep(n.tags):
            self.processe_filtered += 1

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
        while not self.thread_stats_kill:
            sleep(1)
            processed_since_last_time = self.processed - self.last_time_processed
            self.last_time_processed = self.processed

            print(f"Processing {math.floor(processed_since_last_time / 1000)}k/s - found: {self.processe_filtered}")

    def stop(self):
        self.thread_stats_kill = True
