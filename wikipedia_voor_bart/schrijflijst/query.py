import psycopg2
from configparser import ConfigParser
from pandas import DataFrame

# lees de inloggegevens in
cfg = ConfigParser()
cfg.read("../db.cfg")

# connecteer met de databank
knst = psycopg2.connect(host=cfg['db']['host'], port=cfg['db']['port'],
                        database=cfg['db']['db'], user=cfg['db']['user'],
                        password=cfg['db']['pwd'])
knst.set_client_encoding('UTF-8')
knst_cur = knst.cursor()

# Lijst van alle Belgische organisaties die sinds 1993 als hoofdproducent (functies 'gezelschap' en 'producent') aan een
# productie verbonden zijn, als het kan ook met een count van het aantal relaties met een productie.

sql = """
SELECT
  organisations.name,
  locations.city_nl, countries.name_nl, locations.zip_code,
  count(production.productions.id)
FROM
  production.organisations,
  production.productions,
  production.relationships,
  production.seasons,
  production.functions,
  production.countries,
  production.locations
WHERE
  organisations.id = relationships.organisation_id AND
  organisations.country_id = countries.id AND
  productions.season_id = seasons.id AND
  relationships.production_id = productions.id AND
  functions.id = relationships.function_id AND
  seasons.start_year >= 1993 AND
  organisations.location_id = locations.id AND
  countries.id = locations.country_id AND
  functions.name_nl IN ('gezelschap', 'producent') AND
  countries.name_en = 'Belgium'
GROUP BY
  production.organisations.name, locations.city_nl, countries.name_nl, locations.zip_code
"""

lijst_van_organisaties = []
knst_cur.execute(sql)
for row in knst_cur.fetchall():
    lijst_van_organisaties.append(list(row))

DataFrame(lijst_van_organisaties, columns=["Lijst van organisaties", "Stad", "Land", "Postcode", "Count van producties"]).to_csv("../schrijflijst/lijst_van_organisaties.csv")

# lijst van alle personen die aan deze producties (1) verbonden zijn

sql = """
SELECT DISTINCT
  production.productions.id
FROM
  production.organisations,
  production.productions,
  production.relationships,
  production.seasons,
  production.functions,
  production.countries
WHERE
  organisations.id = relationships.organisation_id AND
  organisations.country_id = countries.id AND
  productions.season_id = seasons.id AND
  relationships.production_id = productions.id AND
  functions.id = relationships.function_id AND
  seasons.start_year >= 1993 AND
  functions.name_nl IN ('gezelschap', 'producent') AND
  countries.name_en = 'Belgium'
"""

personen = set()
personen_done = set()
knst_cur.execute(sql)
producties = knst_cur.fetchall()
for i, productionid in enumerate(producties):
    if i % 1000 == 0:
        print(i, "of", len(producties))
    sql = """
    SELECT DISTINCT
      people.id, people.full_name, gender_id, locations.city_nl, countries.name_nl, locations.zip_code
    FROM
      production.productions,
      production.relationships,
      production.people,
      production.locations,
      production.countries
    WHERE
      relationships.production_id = productions.id AND
      people.id = relationships.person_id AND
      locations.id = people.location_id AND
      countries.id = locations.country_id AND
      productions.id = {0}
    """.format(productionid[0])
    knst_cur.execute(sql)
    people_in_production = knst_cur.fetchall()
    for uid, name, gender, stad, land, zip in people_in_production:
            sql = """
            SELECT DISTINCT COUNT(relationships.production_id)
            FROM production.relationships
            WHERE relationships.person_id = {0}
            """.format(uid)
            knst_cur.execute(sql)
            c = knst_cur.fetchone()[0]
            personen.add((name, gender, stad, land, zip, c))

DataFrame(list(personen), columns=["Lijst van personen", "Gender", "Stad", "Land", "Postcode", "Count van producties"]).to_csv("lijst_van_personen.csv")