import json

class Filter:
   def __init__(self, file_path):
      with open(file_path, 'r') as file:
         self.json = json.load(file)

   def apply(self, tags):
      return any([all([tags.get(key) == tag_filter[key] for key in tag_filter]) for tag_filter in self.json])