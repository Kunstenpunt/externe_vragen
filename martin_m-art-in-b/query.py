import psycopg2
from configparser import ConfigParser
from pandas import DataFrame

# lees de inloggegevens in
cfg = ConfigParser()
cfg.read("db.cfg")

# connecteer met de databank
knst = psycopg2.connect(host=cfg['db']['host'], port=cfg['db']['port'],
                        database=cfg['db']['db'], user=cfg['db']['user'],
                        password=cfg['db']['pwd'])
knst.set_client_encoding('UTF-8')
knst_cur = knst.cursor()

sql = """
SELECT DISTINCT
  venues.name,
  venues.address_line_1,
  venues.postal_code,
  venues.city
FROM
  production.shows,
  production.show_types,
  production.venues,
  production.countries,
  production.date_isaars
WHERE
  shows.date_id = date_isaars.id AND
  shows.show_type_id = show_types.id AND
  venues.id = shows.venue_id AND
  countries.id = venues.country_id AND
  date_isaars.year > 2012 AND
  show_types.name_nl = 'première' AND
  countries.name_nl = 'België';
"""

knst_cur.execute(sql)
df = DataFrame(knst_cur.fetchall(), columns=["Ruimte", "Straat en nummer", "Postcode", "Stad"])
df.to_csv("ruimtes met premieres na 2012.csv")