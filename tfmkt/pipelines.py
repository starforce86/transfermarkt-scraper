import json

import psycopg2
from itemadapter import ItemAdapter


def quote(s):
    if s:
        return str(s).replace("'", "''")
    else:
        return ''


class DatabasePipeline:

    def __init__(self, db_host, db_port, db_user, db_password, db_db):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_db = db_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            db_host=crawler.settings.get("DATABASE_HOST"),
            db_port=crawler.settings.get("DATABASE_PORT"),
            db_user=crawler.settings.get("DATABASE_USER"),
            db_password=crawler.settings.get("DATABASE_PASSWORD"),
            db_db=crawler.settings.get("DATABASE_DB"),
        )

    def open_spider(self, spider):
        self.pgconn = psycopg2.connect(
            database=self.db_db,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        self.pgcursor = self.pgconn.cursor()

        if spider.name == 'competitions':
            self.pgcursor.execute("CREATE TABLE IF NOT EXISTS competition "
                                  "(id SERIAL PRIMARY KEY,"
                                  "href VARCHAR(255) UNIQUE,"
                                  "tag VARCHAR(255),"
                                  "country_id VARCHAR(255),"
                                  "country_name VARCHAR(255),"
                                  "country_code VARCHAR(255),"
                                  "competition_type VARCHAR(255),"
                                  "created_at TIMESTAMPTZ,"
                                  "updated_at TIMESTAMPTZ )")
            self.pgconn.commit()
        elif spider.name == 'clubs':
            self.pgcursor.execute("CREATE TABLE IF NOT EXISTS club "
                                  "(id SERIAL PRIMARY KEY,"
                                  "href VARCHAR(255) UNIQUE,"
                                  "competition_id INT,"
                                  "code VARCHAR(255),"
                                  "name VARCHAR(255),"
                                  "coach_name VARCHAR(255),"
                                  "average_age FLOAT,"
                                  "foreigners_number INT,"
                                  "foreigners_percentage VARCHAR(255),"
                                  "national_team_players VARCHAR(255),"
                                  "net_transfer_record VARCHAR(255),"
                                  "squad_size VARCHAR(255),"
                                  "stadium_name VARCHAR(255),"
                                  "stadium_seats VARCHAR(255),"
                                  "total_market_value VARCHAR(255),"
                                  "created_at TIMESTAMPTZ,"
                                  "updated_at TIMESTAMPTZ )")
            self.pgconn.commit()
        elif spider.name == 'games':
            self.pgcursor.execute("CREATE TABLE IF NOT EXISTS game "
                                  "(id SERIAL PRIMARY KEY,"
                                  "href VARCHAR(255) UNIQUE,"
                                  "tm_game_id VARCHAR(255),"
                                  "home_club_href VARCHAR(255),"
                                  "home_club_id INT,"
                                  "home_club_type VARCHAR(255),"
                                  "home_club_position VARCHAR(255),"
                                  "home_manager_name VARCHAR(255),"
                                  "away_club_href VARCHAR(255),"
                                  "away_club_id INT,"
                                  "away_club_type VARCHAR(255),"
                                  "away_club_position VARCHAR(255),"
                                  "away_manager_name VARCHAR(255),"
                                  "result VARCHAR(255),"
                                  "matchday VARCHAR(255),"
                                  "date VARCHAR(255),"
                                  "stadium VARCHAR(512),"
                                  "attendance VARCHAR(255),"
                                  "referee VARCHAR(255),"
                                  "created_at TIMESTAMPTZ,"
                                  "updated_at TIMESTAMPTZ )")
            self.pgconn.commit()
        elif spider.name == 'players':
            self.pgcursor.execute("CREATE TABLE IF NOT EXISTS player "
                                  "(id SERIAL PRIMARY KEY,"
                                  "href VARCHAR(255) UNIQUE,"
                                  "code VARCHAR(255),"
                                  "current_club_id INT,"
                                  "name VARCHAR(255),"
                                  "last_name VARCHAR(255),"
                                  "number VARCHAR(255),"
                                  "name_in_home_country VARCHAR(255),"
                                  "date_of_birth VARCHAR(255),"
                                  "place_of_birth__country VARCHAR(255),"
                                  "place_of_birth__city VARCHAR(255),"
                                  "age VARCHAR(255),"
                                  "height VARCHAR(255),"
                                  "citizenship VARCHAR(255),"
                                  "position VARCHAR(255),"
                                  "image_url VARCHAR(255),"
                                  "player_agent__href VARCHAR(255),"
                                  "player_agent__name VARCHAR(255),"
                                  "foot VARCHAR(255),"
                                  "joined VARCHAR(255),"
                                  "contract_expires VARCHAR(255),"
                                  "day_of_last_contract_extension VARCHAR(255),"
                                  "outfitter VARCHAR(255),"
                                  "current_market_value VARCHAR(255),"
                                  "highest_market_value VARCHAR(255),"
                                  "highest_market_value_date VARCHAR(255),"
                                  "market_value_last_change VARCHAR(255),"
                                  "market_value_details_url VARCHAR(255),"
                                  "social_media VARCHAR(1024),"
                                  "transfer_history__fee_sum VARCHAR(255),"
                                  "transfer_history__formatted_fee_sum VARCHAR(255),"
                                  "career_stats__total_appearances VARCHAR(255),"
                                  "career_stats__total_goals VARCHAR(255),"
                                  "career_stats__total_assists VARCHAR(255),"
                                  "career_stats__total_minutes_per_goal VARCHAR(255),"
                                  "career_stats__total_minutes_played VARCHAR(255),"
                                  "created_at TIMESTAMPTZ,"
                                  "updated_at TIMESTAMPTZ )")
            self.pgconn.commit()
            self.pgcursor.execute("CREATE TABLE IF NOT EXISTS player_valuations "
                                  "(id SERIAL PRIMARY KEY,"
                                  "player_id INT,"
                                  "x VARCHAR(255),"
                                  "y VARCHAR(255),"
                                  "mw VARCHAR(255),"
                                  "datum_mw VARCHAR(255),"
                                  "verein VARCHAR(255),"
                                  "age VARCHAR(255),"
                                  "wappen VARCHAR(255),"
                                  "created_at TIMESTAMPTZ,"
                                  "updated_at TIMESTAMPTZ, "
                                  "UNIQUE (player_id, datum_mw))")
            self.pgconn.commit()
            self.pgcursor.execute("CREATE TABLE IF NOT EXISTS player_transfer_history "
                                  "(id SERIAL PRIMARY KEY,"
                                  "player_id INT,"
                                  "url VARCHAR(255),"
                                  "from_club_emblem_1x VARCHAR(255),"
                                  "from_club_emblem_2x VARCHAR(255),"
                                  "from_club_emblem_mobile VARCHAR(255),"
                                  "from_club_name VARCHAR(255),"
                                  "from_country_flag VARCHAR(255),"
                                  "from_href VARCHAR(255),"
                                  "from_is_special VARCHAR(255),"
                                  "from_latitude VARCHAR(255),"
                                  "from_longitude VARCHAR(255),"
                                  "to_club_emblem_1x VARCHAR(255),"
                                  "to_club_emblem_2x VARCHAR(255),"
                                  "to_club_emblem_mobile VARCHAR(255),"
                                  "to_club_name VARCHAR(255),"
                                  "to_country_flag VARCHAR(255),"
                                  "to_href VARCHAR(255),"
                                  "to_is_special VARCHAR(255),"
                                  "to_latitude VARCHAR(255),"
                                  "to_longitude VARCHAR(255),"
                                  "future_transfer VARCHAR(255),"
                                  "date VARCHAR(255),"
                                  "date_unformatted VARCHAR(255),"
                                  "upcoming VARCHAR(255),"
                                  "season VARCHAR(255),"
                                  "market_value VARCHAR(255),"
                                  "fee VARCHAR(255),"
                                  "show_upcoming_header VARCHAR(255),"
                                  "show_reset_header VARCHAR(255),"
                                  "created_at TIMESTAMPTZ,"
                                  "updated_at TIMESTAMPTZ,"
                                  "UNIQUE (player_id, url) )")
            self.pgconn.commit()
            self.pgcursor.execute("CREATE TABLE IF NOT EXISTS player_career_stats "
                                  "(id SERIAL PRIMARY KEY,"
                                  "player_id INT,"
                                  "competition_id INT,"
                                  "competition_href VARCHAR(255),"
                                  "appearances_href VARCHAR(255),"
                                  "appearances_number VARCHAR(255),"
                                  "goals VARCHAR(255),"
                                  "assists VARCHAR(255),"
                                  "minutes_per_goal VARCHAR(255),"
                                  "minutes_played VARCHAR(255),"
                                  "created_at TIMESTAMPTZ,"
                                  "updated_at TIMESTAMPTZ,"
                                  "UNIQUE (player_id, competition_href) )")
            self.pgconn.commit()
            self.pgcursor.execute("CREATE TABLE IF NOT EXISTS player_national_team_career "
                                  "(id SERIAL PRIMARY KEY,"
                                  "player_id INT,"
                                  "club_id INT,"
                                  "club_href VARCHAR(255),"
                                  "country_name VARCHAR(255),"
                                  "country_flag_url VARCHAR(255),"
                                  "debut_date VARCHAR(255),"
                                  "debut_href VARCHAR(255),"
                                  "matches_number VARCHAR(255),"
                                  "matches_href VARCHAR(255),"
                                  "tore_number VARCHAR(255),"
                                  "tore_href VARCHAR(255),"
                                  "created_at TIMESTAMPTZ,"
                                  "updated_at TIMESTAMPTZ,"
                                  "UNIQUE (player_id, club_href) )")
            self.pgconn.commit()

    def close_spider(self, spider):
        self.pgconn.close()

    def process_item(self, item, spider):
        item_dic = ItemAdapter(item).asdict()
        if spider.name == 'competitions':
            sql = "insert into competition (href, tag, country_id, country_name, country_code, competition_type, created_at, updated_at) " \
                  "values('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', now(), now()) " \
                  "on conflict (href) do update " \
                  "set tag = excluded.tag, " \
                  "country_id = excluded.country_id, " \
                  "country_name = excluded.country_name, " \
                  "country_code = excluded.country_code, " \
                  "competition_type = excluded.competition_type, " \
                  "updated_at = now()".format(
                    quote(item_dic['href']),
                    quote(item_dic['tag']),
                    item_dic.get('country_id') or '',
                    quote(item_dic.get('country_name')),
                    quote(item_dic.get('country_code')),
                    quote(item_dic['competition_type'])
                  )
            self.pgcursor.execute(sql)
            self.pgconn.commit()
        elif spider.name == 'clubs':
            self.pgcursor.execute("select id from competition where href='{0}'".format(item_dic['parent']['href']))
            result = self.pgcursor.fetchone()
            competition_id = result[0] if result else None
            
            sql = "insert into club (href, competition_id, code, name, coach_name, average_age, foreigners_number" \
                  ", foreigners_percentage, national_team_players, net_transfer_record, squad_size, stadium_name" \
                  ", stadium_seats, total_market_value, created_at, updated_at) " \
                  "values('{0}', {1}, '{2}', '{3}', '{4}', {5}, {6}, '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', now(), now()) " \
                  "on conflict (href) do update " \
                  "set competition_id = excluded.competition_id, " \
                  "code = excluded.code, " \
                  "name = excluded.name, " \
                  "coach_name = excluded.coach_name, " \
                  "average_age = excluded.average_age, " \
                  "foreigners_number = excluded.foreigners_number, " \
                  "foreigners_percentage = excluded.foreigners_percentage, " \
                  "national_team_players = excluded.national_team_players, " \
                  "net_transfer_record = excluded.net_transfer_record, " \
                  "squad_size = excluded.squad_size, " \
                  "stadium_name = excluded.stadium_name, " \
                  "stadium_seats = excluded.stadium_seats, " \
                  "total_market_value = excluded.total_market_value, " \
                  "updated_at = now()".format(
                    quote(item_dic['href']),
                    competition_id if competition_id else 'NULL',
                    quote(item_dic['code']),
                    quote(item_dic['name']),
                    quote(item_dic['coach_name']),
                    item_dic['average_age'] or 'NULL',
                    item_dic['foreigners_number'] or 'NULL',
                    item_dic['foreigners_percentage'] or '',
                    item_dic['national_team_players'] or '',
                    quote(item_dic['net_transfer_record']),
                    item_dic['squad_size'] or '',
                    quote(item_dic['stadium_name']),
                    quote(item_dic['stadium_seats']),
                    quote(item_dic['total_market_value'])
                  )
            self.pgcursor.execute(sql)
            self.pgconn.commit()
        elif spider.name == 'clubs':
            self.pgcursor.execute("select id from competition where href='{0}'".format(item_dic['parent']['href']))
            result = self.pgcursor.fetchone()
            competition_id = result[0] if result else None

            sql = "insert into club (href, competition_id, code, name, coach_name, average_age, foreigners_number" \
                  ", foreigners_percentage, national_team_players, net_transfer_record, squad_size, stadium_name" \
                  ", stadium_seats, total_market_value, created_at, updated_at) " \
                  "values('{0}', {1}, '{2}', '{3}', '{4}', {5}, {6}, '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', now(), now()) " \
                  "on conflict (href) do update " \
                  "set competition_id = excluded.competition_id, " \
                  "code = excluded.code, " \
                  "name = excluded.name, " \
                  "coach_name = excluded.coach_name, " \
                  "average_age = excluded.average_age, " \
                  "foreigners_number = excluded.foreigners_number, " \
                  "foreigners_percentage = excluded.foreigners_percentage, " \
                  "national_team_players = excluded.national_team_players, " \
                  "net_transfer_record = excluded.net_transfer_record, " \
                  "squad_size = excluded.squad_size, " \
                  "stadium_name = excluded.stadium_name, " \
                  "stadium_seats = excluded.stadium_seats, " \
                  "total_market_value = excluded.total_market_value, " \
                  "updated_at = now()".format(
                    quote(item_dic['href']),
                    competition_id if competition_id else 'NULL',
                    quote(item_dic['code']),
                    quote(item_dic['name']),
                    quote(item_dic['coach_name']),
                    item_dic['average_age'] or 'NULL',
                    item_dic['foreigners_number'] or 'NULL',
                    item_dic['foreigners_percentage'] or '',
                    item_dic['national_team_players'] or '',
                    quote(item_dic['net_transfer_record']),
                    item_dic['squad_size'] or '',
                    quote(item_dic['stadium_name']),
                    quote(item_dic['stadium_seats']),
                    quote(item_dic['total_market_value'])
                  )
            self.pgcursor.execute(sql)
            self.pgconn.commit()
        elif spider.name == 'games':
            home_club_id = None
            away_club_id = None
            if item_dic.get('home_club') and item_dic['home_club'].get('href'):
                self.pgcursor.execute("select id from club where href='{0}'".format(item_dic['home_club']['href']))
                result = self.pgcursor.fetchone()
                home_club_id = result[0] if result else None
            if item_dic.get('away_club') and item_dic['away_club'].get('href'):
                self.pgcursor.execute("select id from club where href='{0}'".format(item_dic['away_club']['href']))
                result = self.pgcursor.fetchone()
                away_club_id = result[0] if result else None

            sql = "insert into game (href, tm_game_id, home_club_href, home_club_id, home_club_type, home_club_position, home_manager_name" \
                  ", away_club_href, away_club_id, away_club_type, away_club_position, away_manager_name" \
                  ", result, matchday, date, stadium, attendance, referee, created_at, updated_at) " \
                  "values('{0}', '{1}', '{2}', {3}, '{4}', '{5}', '{6}', '{7}', {8}, '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}', '{16}', '{17}', now(), now()) " \
                  "on conflict (href) do update " \
                  "set tm_game_id = excluded.tm_game_id, " \
                  "home_club_href = excluded.home_club_href, " \
                  "home_club_id = excluded.home_club_id, " \
                  "home_club_type = excluded.home_club_type, " \
                  "home_club_position = excluded.home_club_position, " \
                  "home_manager_name = excluded.home_manager_name, " \
                  "away_club_href = excluded.away_club_href, " \
                  "away_club_id = excluded.away_club_id, " \
                  "away_club_type = excluded.away_club_type, " \
                  "away_club_position = excluded.away_club_position, " \
                  "away_manager_name = excluded.away_manager_name, " \
                  "result = excluded.result, " \
                  "matchday = excluded.matchday, " \
                  "date = excluded.date, " \
                  "stadium = excluded.stadium, " \
                  "attendance = excluded.attendance, " \
                  "referee = excluded.referee, " \
                  "updated_at = now()".format(
                quote(item_dic['href']),
                quote(item_dic['game_id']),
                quote(item_dic['home_club'].get('href')) if item_dic.get('home_club') else '',
                home_club_id if home_club_id else 'NULL',
                quote(item_dic['home_club'].get('type')) if item_dic.get('home_club') else '',
                quote(item_dic['home_club_position']),
                quote(item_dic['home_manager'].get('name')) if item_dic.get('home_manager') else '',
                quote(item_dic['away_club'].get('href')) if item_dic.get('away_club') else '',
                away_club_id if away_club_id else 'NULL',
                quote(item_dic['away_club'].get('type')) if item_dic.get('away_club') else '',
                quote(item_dic['away_club_position']),
                quote(item_dic['away_manager'].get('name')) if item_dic.get('away_manager') else '',
                quote(item_dic['result']),
                quote(item_dic['matchday']),
                quote(item_dic['date']),
                quote(item_dic['stadium']),
                quote(item_dic['attendance']),
                quote(item_dic['referee'])
            )
            self.pgcursor.execute(sql)
            self.pgconn.commit()
        elif spider.name == 'players':
            self.pgcursor.execute("select id from club where href='{0}'".format(item_dic['current_club']['href']))
            result = self.pgcursor.fetchone()
            current_club_id = result[0] if result else None
            sql = "insert into player (href, code, current_club_id, name, last_name, number, name_in_home_country" \
                  ", date_of_birth, place_of_birth__country, place_of_birth__city, age, height, citizenship, position" \
                  ", image_url, player_agent__href, player_agent__name, foot, joined, contract_expires" \
                  ", day_of_last_contract_extension, outfitter, current_market_value, highest_market_value" \
                  ", highest_market_value_date, market_value_last_change, market_value_details_url, social_media" \
                  ", transfer_history__fee_sum, transfer_history__formatted_fee_sum, career_stats__total_appearances" \
                  ", career_stats__total_goals, career_stats__total_assists, career_stats__total_minutes_per_goal, career_stats__total_minutes_played" \
                  ", created_at, updated_at) " \
                  "values('{0}', '{1}', {2}, '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}'" \
                  ", '{13}', '{14}', '{15}', '{16}', '{17}', '{18}', '{19}', '{20}', '{21}', '{22}', '{23}', '{24}', '{25}'" \
                  ", '{26}', '{27}', '{28}', '{29}', '{30}', '{31}', '{32}', '{33}', '{34}', now(), now()) " \
                  "on conflict (href) do update " \
                  "set code = excluded.code, " \
                  "current_club_id = excluded.current_club_id, " \
                  "name = excluded.name, " \
                  "last_name = excluded.last_name, " \
                  "number = excluded.number, " \
                  "name_in_home_country = excluded.name_in_home_country, " \
                  "date_of_birth = excluded.date_of_birth, " \
                  "place_of_birth__country = excluded.place_of_birth__country, " \
                  "place_of_birth__city = excluded.place_of_birth__city, " \
                  "age = excluded.age, " \
                  "height = excluded.height, " \
                  "citizenship = excluded.citizenship, " \
                  "position = excluded.position, " \
                  "player_agent__href = excluded.player_agent__href, " \
                  "player_agent__name = excluded.player_agent__name, " \
                  "foot = excluded.foot, " \
                  "joined = excluded.joined, " \
                  "contract_expires = excluded.contract_expires, " \
                  "day_of_last_contract_extension = excluded.day_of_last_contract_extension, " \
                  "outfitter = excluded.outfitter, " \
                  "current_market_value = excluded.current_market_value, " \
                  "highest_market_value = excluded.highest_market_value, " \
                  "highest_market_value_date = excluded.highest_market_value_date, " \
                  "market_value_last_change = excluded.market_value_last_change, " \
                  "market_value_details_url = excluded.market_value_details_url, " \
                  "social_media = excluded.social_media, " \
                  "transfer_history__fee_sum = excluded.transfer_history__fee_sum, " \
                  "transfer_history__formatted_fee_sum = excluded.transfer_history__formatted_fee_sum, " \
                  "career_stats__total_appearances = excluded.career_stats__total_appearances, " \
                  "career_stats__total_goals = excluded.career_stats__total_goals, " \
                  "career_stats__total_assists = excluded.career_stats__total_assists, " \
                  "career_stats__total_minutes_per_goal = excluded.career_stats__total_minutes_per_goal, " \
                  "career_stats__total_minutes_played = excluded.career_stats__total_minutes_played, " \
                  "updated_at = now() returning id".format(
                    quote(item_dic['href']),
                    quote(item_dic['code']),
                    current_club_id if current_club_id else 'NULL',
                    quote(item_dic['name']),
                    quote(item_dic['last_name']),
                    quote(item_dic['number']),
                    quote(item_dic['name_in_home_country']),
                    quote(item_dic['date_of_birth']),
                    quote(item_dic['place_of_birth']['country']),
                    quote(item_dic['place_of_birth']['city']),
                    quote(item_dic['age']),
                    quote(item_dic['height']),
                    quote(item_dic['citizenship']),
                    quote(item_dic['position']),
                    quote(item_dic['image_url']),
                    quote(item_dic['player_agent']['href']),
                    quote(item_dic['player_agent']['name']),
                    quote(item_dic['foot']),
                    quote(item_dic['joined']),
                    quote(item_dic['contract_expires']),
                    quote(item_dic['day_of_last_contract_extension']),
                    quote(item_dic['outfitter']),
                    quote(item_dic['market_value_history'].get('current')),
                    quote(item_dic['market_value_history'].get('highest')),
                    quote(item_dic['market_value_history'].get('highest_date')),
                    quote(item_dic['market_value_history'].get('last_change')),
                    quote(item_dic['market_value_history'].get('details_url')),
                    quote(json.dumps(item_dic['social_media'])) if item_dic.get('social_media') else '',
                    quote(item_dic['transfer_history'].get('feeSum')),
                    quote(item_dic['transfer_history'].get('formattedFeeSum')),
                    item_dic['career_stats']['total']['appearances'] if item_dic['career_stats'].get('total') and item_dic['career_stats']['total'].get('appearances') else '',
                    item_dic['career_stats']['total']['goals'] if item_dic['career_stats'].get('total') and item_dic['career_stats']['total'].get('goals') else '',
                    item_dic['career_stats']['total']['assists'] if item_dic['career_stats'].get('total') and item_dic['career_stats']['total'].get('assists') else '',
                    item_dic['career_stats']['total']['minutes_per_goal'] if item_dic['career_stats'].get('total') and item_dic['career_stats']['total'].get('minutes_per_goal') else '',
                    item_dic['career_stats']['total']['minutes_played'] if item_dic['career_stats'].get('total') and item_dic['career_stats']['total'].get('minutes_played') else '',
                  )
            self.pgcursor.execute(sql)
            self.pgconn.commit()
            player_id = self.pgcursor.fetchone()[0]

            for entry in item_dic['market_value_history'].get('list', []):
                sql = "insert into player_valuations (player_id, x, y, mw, datum_mw, verein, age, wappen, created_at, updated_at) " \
                      "values({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', now(), now()) " \
                      "on conflict (player_id, datum_mw) do update " \
                      "set x = excluded.x, " \
                      "y = excluded.y, " \
                      "mw = excluded.mw, " \
                      "verein = excluded.verein, " \
                      "age = excluded.age, " \
                      "wappen = excluded.wappen, " \
                      "updated_at = now()".format(
                        player_id,
                        quote(entry.get('x')),
                        quote(entry.get('y')),
                        quote(entry.get('mw')),
                        quote(entry.get('datum_mw')),
                        quote(entry.get('verein')),
                        quote(entry.get('age')),
                        quote(entry.get('wappen')),
                      )
                self.pgcursor.execute(sql)

            for entry in item_dic['transfer_history'].get('transfers', []):
                sql = "insert into player_transfer_history (player_id, url, from_club_emblem_1x, from_club_emblem_2x, from_club_emblem_mobile" \
                      ", from_club_name, from_country_flag, from_href, from_is_special, from_latitude, from_longitude" \
                      ", to_club_emblem_1x, to_club_emblem_2x, to_club_emblem_mobile, to_club_name, to_country_flag, to_href, to_is_special, to_latitude, to_longitude" \
                      ", future_transfer, date, date_unformatted, upcoming, season, market_value, fee, show_upcoming_header, show_reset_header, created_at, updated_at) " \
                      "values({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}', '{16}'" \
                      ", '{17}', '{18}', '{19}', '{20}', '{21}', '{22}', '{23}', '{24}', '{25}', '{26}', '{27}', '{28}', now(), now()) " \
                      "on conflict (player_id, url) do update " \
                      "set from_club_emblem_1x = excluded.from_club_emblem_1x, " \
                      "from_club_emblem_2x = excluded.from_club_emblem_2x, " \
                      "from_club_emblem_mobile = excluded.from_club_emblem_mobile, " \
                      "from_club_name = excluded.from_club_name, " \
                      "from_country_flag = excluded.from_country_flag, " \
                      "from_href = excluded.from_href, " \
                      "from_is_special = excluded.from_is_special, " \
                      "from_latitude = excluded.from_latitude, " \
                      "from_longitude = excluded.from_longitude, " \
                      "to_club_emblem_1x = excluded.to_club_emblem_1x, " \
                      "to_club_emblem_2x = excluded.to_club_emblem_2x, " \
                      "to_club_emblem_mobile = excluded.to_club_emblem_mobile, " \
                      "to_club_name = excluded.to_club_name, " \
                      "to_country_flag = excluded.to_country_flag, " \
                      "to_href = excluded.to_href, " \
                      "to_is_special = excluded.to_is_special, " \
                      "to_latitude = excluded.to_latitude, " \
                      "to_longitude = excluded.to_longitude, " \
                      "future_transfer = excluded.future_transfer, " \
                      "date = excluded.date, " \
                      "date_unformatted = excluded.date_unformatted, " \
                      "upcoming = excluded.upcoming, " \
                      "season = excluded.season, " \
                      "market_value = excluded.market_value, " \
                      "fee = excluded.fee, " \
                      "show_upcoming_header = excluded.show_upcoming_header, " \
                      "show_reset_header = excluded.show_reset_header, " \
                      "updated_at = now()".format(
                        player_id,
                        quote(entry.get('url')),
                        quote(entry['from'].get('clubEmblem-1x')) if entry.get('from') else '',
                        quote(entry['from'].get('clubEmblem-2x')) if entry.get('from') else '',
                        quote(entry['from'].get('clubEmblemMobile')) if entry.get('from') else '',
                        quote(entry['from'].get('clubName')) if entry.get('from') else '',
                        quote(entry['from'].get('countryFlag')) if entry.get('from') else '',
                        quote(entry['from'].get('href')) if entry.get('from') else '',
                        quote(entry['from'].get('isSpecial')) if entry.get('from') else '',
                        quote(entry['from'].get('latitude')) if entry.get('from') else '',
                        quote(entry['from'].get('longitude')) if entry.get('from') else '',
                        quote(entry['to'].get('clubEmblem-1x')) if entry.get('to') else '',
                        quote(entry['to'].get('clubEmblem-2x')) if entry.get('to') else '',
                        quote(entry['to'].get('clubEmblemMobile')) if entry.get('to') else '',
                        quote(entry['to'].get('clubName')) if entry.get('to') else '',
                        quote(entry['to'].get('countryFlag')) if entry.get('to') else '',
                        quote(entry['to'].get('href')) if entry.get('to') else '',
                        quote(entry['to'].get('isSpecial')) if entry.get('to') else '',
                        quote(entry['to'].get('latitude')) if entry.get('to') else '',
                        quote(entry['to'].get('longitude')) if entry.get('to') else '',
                        quote(entry.get('futureTransfer')),
                        quote(entry.get('date')),
                        quote(entry.get('dateUnformatted')),
                        quote(entry.get('upcoming')),
                        quote(entry.get('season')),
                        quote(entry.get('marketValue')),
                        quote(entry.get('fee')),
                        quote(entry.get('showUpcomingHeader')),
                        quote(entry.get('showResetHeader')),
                      )
                self.pgcursor.execute(sql)

            for entry in item_dic['career_stats'].get('list', []):
                competition_id = None
                if entry.get('competition') and entry['competition'].get('href'):
                    self.pgcursor.execute("select id from competition where href='{0}'".format(entry['competition']['href']))
                    result = self.pgcursor.fetchone()
                    competition_id = result[0] if result else None

                sql = "insert into player_career_stats (player_id, competition_id, competition_href, appearances_href, appearances_number" \
                      ", goals, assists, minutes_per_goal, minutes_played, created_at, updated_at) " \
                      "values({0}, {1}, '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', now(), now()) " \
                      "on conflict (player_id, competition_href) do update " \
                      "set competition_id = excluded.competition_id, " \
                      "appearances_href = excluded.appearances_href, " \
                      "appearances_number = excluded.appearances_number, " \
                      "goals = excluded.goals, " \
                      "assists = excluded.assists, " \
                      "minutes_per_goal = excluded.minutes_per_goal, " \
                      "minutes_played = excluded.minutes_played, " \
                      "updated_at = now()".format(
                        player_id,
                        competition_id if competition_id else 'NULL',
                        quote(entry['competition'].get('href')) if entry.get('competition') else '',
                        quote(entry['appearances'].get('href')) if entry.get('appearances') else '',
                        quote(entry['appearances'].get('number')) if entry.get('appearances') else '',
                        quote(entry.get('goals')),
                        quote(entry.get('assists')),
                        quote(entry.get('minutes_per_goal')),
                        quote(entry.get('minutes_played')),
                      )
                self.pgcursor.execute(sql)

            for entry in item_dic.get('national_team_career', []):
                club_id = None
                if entry.get('nationtal_team') and entry['nationtal_team'].get('club_href'):
                    self.pgcursor.execute(
                        "select id from club where href='{0}'".format(entry['nationtal_team']['club_href']))
                    result = self.pgcursor.fetchone()
                    club_id = result[0] if result else None

                sql = "insert into player_national_team_career (player_id, club_id, club_href, country_name, country_flag_url" \
                      ", debut_date, debut_href, matches_number, matches_href, tore_number, tore_href, created_at, updated_at) " \
                      "values({0}, {1}, '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', now(), now()) " \
                      "on conflict (player_id, club_href) do update " \
                      "set club_id = excluded.club_id, " \
                      "country_name = excluded.country_name, " \
                      "country_flag_url = excluded.country_flag_url, " \
                      "debut_date = excluded.debut_date, " \
                      "debut_href = excluded.debut_href, " \
                      "matches_number = excluded.matches_number, " \
                      "matches_href = excluded.matches_href, " \
                      "tore_number = excluded.tore_number, " \
                      "tore_href = excluded.tore_href, " \
                      "updated_at = now()".format(
                        player_id,
                        club_id if club_id else 'NULL',
                        quote(entry['nationtal_team'].get('club_href')) if entry.get('nationtal_team') else '',
                        quote(entry['nationtal_team'].get('country_name')) if entry.get('nationtal_team') else '',
                        quote(entry['nationtal_team'].get('country_flag_url')) if entry.get('nationtal_team') else '',
                        quote(entry['debut'].get('date')) if entry.get('debut') else '',
                        quote(entry['debut'].get('href')) if entry.get('debut') else '',
                        quote(entry['matches'].get('number')) if entry.get('matches') else '',
                        quote(entry['matches'].get('href')) if entry.get('matches') else '',
                        quote(entry['tore'].get('number')) if entry.get('tore') else '',
                        quote(entry['tore'].get('href')) if entry.get('tore') else '',
                      )
                self.pgcursor.execute(sql)
            self.pgconn.commit()
        return item
