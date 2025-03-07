import copy
import logging
from io import BufferedReader
import scrapy
from scrapy import Request
from scrapy.shell import inspect_response # required for debugging

import os, sys
import json
import gzip
import typing

default_base_url = 'https://www.transfermarkt.co.uk'
# logging.basicConfig(
#     filename="log.txt", format="%(levelname)s: %(message)s", level=logging.INFO
# )

def read_lines(file_name: str, reading_fn: typing.Callable[[str], BufferedReader]) -> typing.List[dict]:
  """A function that reads JSON lines from a file.

  :param file_name: The name of the file to read from.
  :type file_name: str
  :param reading_fn: A function object to be used for opening the file.
  :type reading_fn: typing.Callable[[str], BufferedReader]
  :return: A list of json objects (dict)
  :rtype: typing.List[dict]
  """
  with reading_fn(file_name) as f:
    lines = f.readlines()
    parents = [ json.loads(line) for line in lines ]
  
  return parents

class BaseSpider(scrapy.Spider):
  def __init__(self, base_url=None, parents=None, seasons=None):

    if base_url is not None:
      self.base_url = base_url
    else:
      self.base_url = default_base_url

    # identify parents file extension (if any)
    if parents is not None:
      extension = parents.split(".")[-1]
      if extension:
        self.gzip_compressed = extension == "gz"
      else: # if no extension, assume the file is not compressed
        self.gzip_compressed = False
    else:
      self.gzip_compressed = False
    
    # load parent objects, either from stdin, a file or a zipped file
    if parents is not None:
      if self.gzip_compressed:
        parents = read_lines(parents, gzip.open)
      else:
        parents = read_lines(parents, open)
    elif not sys.stdin.isatty():
        parents = [ json.loads(line) for line in sys.stdin ]
    else:
      parents = self.scrape_parents()

    # 2nd level parents are redundat
    for parent in parents:
      if parent.get('parent') is not None:
        del parent['parent']

    if seasons:
      self.seasons = [int(s.strip()) for s in seasons.split(',') if s.strip()]
    else:
      self.seasons = [s for s in range(2024, 1869, -1)]

    self.entrypoints = parents

  def scrape_parents(self):
    if not os.environ.get('SCRAPY_CHECK'):
      raise Exception("Backfilling is not yet supported, please provide a 'parents' file")
    else:
      return []

  def start_requests(self):

    applicable_items = []

    for item in self.entrypoints:
      for season in self.seasons:
        season_item = copy.deepcopy(item)
        season_item['seasoned_href'] = self.seasonize_entrypoin_href(season_item, season)
        applicable_items.append(season_item)

    return [
      Request(
        item['seasoned_href'],
        cb_kwargs={
          'parent': item
        }
      )
      for item in applicable_items
    ]

  def seasonize_entrypoin_href(self, item, season):

    if item['type'] == 'club':
      seasonized_href = f"{self.base_url}{item['href']}/saison_id/{season}"
    elif item['type'] == 'competition':
      if item['competition_type'] in ['first_tier', 'second_tier', 'third_tier', 'fourth_tier', 'fifth_tier', 'sixth_tier', 'play_offs', 'regional_championship', 'reserve_league', 'youth_league']:
        seasonized_href = f"{self.base_url}{item['href']}/plus/0?saison_id={season}"
      elif item['competition_type'] in ['domestic_cup', 'domestic_super_cup', 'domestic_youth_cup', 'league_cup', 'further_cup', 'further_super_cup', 'international_youth_cup', 'national_youth_super_cup']:
        seasonized_href = f"{self.base_url}{item['href']}?saison_id={season}".replace("wettbewerb", "pokalwettbewerb")
      else:
        seasonized_href = f"{self.base_url}{item['href']}?saison_id={season}"
    else:
      seasonized_href = f"{self.base_url}{item['href']}"

    return seasonized_href

  def safe_strip(self, word):
    if word:
      return word.strip()
    else:
      return word

