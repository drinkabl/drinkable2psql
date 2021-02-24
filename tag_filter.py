import json
import logging

class Filter:
   def __init__(self, file_path):
      try:
         with open(file_path, 'r') as file:
            self.json = json.load(file)
            logging.debug('Filter file correctly loaded')
      except Exception:
         logging.critical("Can't read the filter file. Did you submitted the correct path with --filter (-f)? Is it a valid JSON?", exc_info=True)
         return None   

   def apply(self, tags):
      return any([all([tags.get(key) == tag_filter[key] for key in tag_filter]) for tag_filter in self.json])