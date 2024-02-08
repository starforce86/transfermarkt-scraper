from tfmkt.spiders.common import BaseSpider
from scrapy.shell import Response
from scrapy.shell import inspect_response # required for debugging
from urllib.parse import unquote, urlparse
import re
import json

class PlayersSpider(BaseSpider):
  name = 'players'

  players = []

  def parse(self, response, parent):
      """Parse clubs's page to collect all player's urls.

        @url https://www.transfermarkt.co.uk/sc-braga/startseite/verein/1075/saison_id/2019
        @returns requests 37 37
        @cb_kwargs {"parent": "dummy"}
      """

      # uncommenting the two lines below will open a scrapy shell with the context of this request
      # when you run the crawler. this is useful for developing new extractors

      # inspect_response(response, self)
      # exit(1)

      players_table = response.xpath("//div[@class='responsive-table']")
      assert len(players_table) == 1

      players_table = players_table[0]

      player_hrefs = players_table.xpath('//table[@class="inline-table"]//td[@class="hauptlink"]/a/@href').getall()

      for href in player_hrefs:
        if href not in self.players:
          self.players.append(href)

          cb_kwargs = {
            'base' : {
              'type': 'player',
              'href': href,
              'parent': parent
            }
          }

          yield response.follow(href, self.parse_details, cb_kwargs=cb_kwargs)

  def parse_details(self, response, base):
    """Extract player details from the main page.
    It currently only parses the PLAYER DATA section.

      @url https://www.transfermarkt.co.uk/steven-berghuis/profil/spieler/129554
      @returns items 1 1
      @cb_kwargs {"base": {"href": "some_href/code", "type": "player", "parent": {}}}
      @scrapes href type parent name last_name number
    """

    # uncommenting the two lines below will open a scrapy shell with the context of this request
    # when you run the crawler. this is useful for developing new extractors

    # inspect_response(response, self)
    # exit(1)

    # parse 'PLAYER DATA' section

    attributes = {}

    name_element = response.xpath("//h1[@class='data-header__headline-wrapper']")
    attributes["name"] = self.safe_strip("".join(name_element.xpath("text()").getall()).strip())
    attributes["last_name"] = self.safe_strip(name_element.xpath("strong/text()").get())
    attributes["number"] = self.safe_strip(name_element.xpath("span/text()").get())

    attributes['name_in_home_country'] = response.xpath("//span[text()='Name in home country:']/following::span[1]/text()").get()
    attributes['date_of_birth'] = response.xpath("//span[@itemprop='birthDate']/text()").get('').strip().split(" (")[0]
    attributes['place_of_birth'] = {
      'country': response.xpath("//span[text()='Place of birth:']/following::span[1]/span/img/@title").get(),
      'city': response.xpath("//span[text()='Place of birth:']/following::span[1]/span/text()").get()
    }
    attributes['age'] = response.xpath("//span[@itemprop='birthDate']/text()").get('').strip().split('(')[-1].split(')')[0]
    attributes['height'] = response.xpath("//span[text()='Height:']/following::span[1]/text()").get()
    attributes['citizenship'] = response.xpath("//span[text()='Citizenship:']/following::span[1]/img/@title").get()
    attributes['position'] = self.safe_strip(response.xpath("//span[text()='Position:']/following::span[1]/text()").get())
    attributes['player_agent'] = {
      'href': response.xpath("//span[text()='Player agent:']/following::span[1]/a/@href").get(),
      'name': response.xpath("//span[text()='Player agent:']/following::span[1]/a/text()").get()
    }
    attributes['image_url'] = response.xpath("//img[@class='data-header__profile-image']/@src").get()
    attributes['current_club'] = {
      'href': response.xpath("//span[contains(text(),'Current club:')]/following::span[1]/a/@href").get()
    }
    attributes['foot'] = response.xpath("//span[text()='Foot:']/following::span[1]/text()").get()
    attributes['joined'] = response.xpath("//span[text()='Joined:']/following::span[1]/text()").get()
    attributes['contract_expires'] = self.safe_strip(response.xpath("//span[text()='Contract expires:']/following::span[1]/text()").get())
    attributes['day_of_last_contract_extension'] = response.xpath("//span[text()='Date of last contract extension:']/following::span[1]/text()").get()
    attributes['outfitter'] = response.xpath("//span[text()='Outfitter:']/following::span[1]/text()").get()

    current_market_value_text = self.safe_strip(response.xpath("//div[@class='tm-player-market-value-development__current-value']/text()").get())
    current_market_value_link = self.safe_strip(response.xpath("//div[@class='tm-player-market-value-development__current-value']/a/text()").get())
    if current_market_value_text: # sometimes the actual value is in the same level (https://www.transfermarkt.co.uk/femi-seriki/profil/spieler/638649)
      attributes['current_market_value'] = current_market_value_text
    else: # sometimes is one level down (https://www.transfermarkt.co.uk/rhys-norrington-davies/profil/spieler/543164)
      attributes['current_market_value'] = current_market_value_link
    attributes['highest_market_value'] = self.safe_strip(response.xpath("//div[@class='tm-player-market-value-development__max-value']/text()").get())

    social_media_value_node = response.xpath("//span[text()='Social-Media:']/following::span[1]")
    if len(social_media_value_node) > 0:
      attributes['social_media'] = []
      for element in social_media_value_node.xpath('div[@class="socialmedia-icons"]/a'):
        href = element.xpath('@href').get()
        attributes['social_media'].append(
          href
        )

    # parse historical market value from figure
    # attributes['market_value_history'] = self.parse_market_history(response)

    attributes['career_stats'] = self.parse_career_stats(response)
    attributes['national_team_career'] = self.parse_national_team_career(response)

    attributes['code'] = unquote(urlparse(base["href"]).path.split("/")[1])

    cb_kwargs = {
      'base': {
        **base,
        **attributes
      }
    }
    player_id = base["href"].split('/')[-1]
    yield response.follow(f"{self.base_url}/ceapi/transferHistory/list/{player_id}", self.parse_transfer_history, cb_kwargs=cb_kwargs)

  def parse_market_history(self, response: Response):
    """
    Parse player's market history from the graph
    """
    pattern = re.compile('\'data\'\:.*\}\}]')

    try:
      parsed_script = json.loads(
        '{' + response.xpath("//script[contains(., 'series')]/text()").re(pattern)[0].replace("\'", "\"").encode().decode('unicode_escape') + '}'
      )
      return parsed_script["data"]
    except Exception as err:
      self.logger.warning("Failed to scrape market value history from %s", response.url)
      return None

  def parse_market_value_history(self, response: Response, base):
    """
    Get player's market history
    """
    yield {
      **base,
      'market_value_history': json.loads(response.text)
    }

  def parse_transfer_history(self, response: Response, base):
    """
    Get player's transfer history
    """
    cb_kwargs = {
      'base': {
        **base,
        'transfer_history': json.loads(response.text)
      }
    }
    player_id = base["href"].split('/')[-1]
    yield response.follow(f"{self.base_url}/ceapi/marketValueDevelopment/graph/{player_id}", self.parse_market_value_history, cb_kwargs=cb_kwargs)

  def parse_career_stats(self, response: Response):
    """
    Parse player's career stats
    """
    result = {'list': []}
    if not (career_stats_box := response.xpath("//div[@data-viewport='Leistungsdaten_Saison']")):
      return result

    if not (table_rows := career_stats_box.css('table.items tbody tr.odd, table.items tbody tr.even')):
      return result

    for row in table_rows[0:]:
      result['list'].append({
        'competition': {
          'logo_url': row.xpath('td[1]/img/@src').get(),
          'href': row.xpath('td[2]/a/@href').get(),
          'name': row.xpath('td[2]/a/text()').get()
        },
        'appearances': {
          'href': row.xpath('td[3]/a/@href').get(),
          'number': row.xpath('td[3]/a/text()').get()
        },
        'goals': row.xpath('td[4]/text()').get(),
        'assists': row.xpath('td[5]/text()').get(),
        'minutes_per_goal': row.xpath('td[6]/text()').get(),
        'minutes_played': row.xpath('td[7]/text()').get(),
      })
    if total_row := career_stats_box.css('table.items tfoot tr'):
      result['total'] = {
        'appearances': total_row[0].xpath('td[3]/text()').get(),
        'goals': total_row[0].xpath('td[4]/text()').get(),
        'assists': total_row[0].xpath('td[5]/text()').get(),
        'minutes_per_goal': total_row[0].xpath('td[6]/text()').get(),
        'minutes_played': total_row[0].xpath('td[7]/text()').get(),
      }
    return result

  def parse_national_team_career(self, response: Response):
    """
    Parse player's national team career
    """
    result = []
    if not (national_career_box := response.xpath("//div[@data-viewport='Laenderspielkarriere']")):
      return result

    if not (table_rows := national_career_box.css('div.national-career__row')):
      return result

    for row in table_rows[1:]:
      result.append({
        'nationtal_team': {
          'country_flag_url': row.xpath('div[2]/img/@data-src').get(),
          'club_href': row.xpath('div[2]/a/@href').get(),
          'country_name': row.xpath('div[2]/a/text()').get()
        },
        'debut': {
          'href': row.xpath('div[3]/a/@href').get(),
          'date': row.xpath('div[3]/a/text()').get()
        },
        'matches': {
          'href': row.xpath('div[4]/a/@href').get(),
          'number': row.xpath('div[4]/a/text()').get()
        },
        'tore': {
          'href': row.xpath('div[5]/a/@href').get(),
          'number': row.xpath('div[5]/a/text()').get()
        },
      })
    return result
