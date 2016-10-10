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

# Lijst van alle Belgische organisaties die sinds 1993 als hoofdproducent (functies 'gezelschap' en 'producent') aan een
# productie verbonden zijn, als het kan ook met een count van het aantal relaties met een productie.

sql = """
SELECT
  organisations.name,
  count(production.productions.id)
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
GROUP BY
  production.organisations.name
"""

lijst_van_organisaties = []
knst_cur.execute(sql)
for row in knst_cur.fetchall():
    lijst_van_organisaties.append(list(row))

DataFrame(lijst_van_organisaties, columns=["Lijst van organisaties", "Count van producties"]).to_csv("lijst_van_organisaties.csv")

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

knst_cur.execute(sql)
producties = knst_cur.fetchall()
for i, productionid in enumerate(producties):
    if i % 1000 == 0:
        print(i, "of", len(producties))
    sql = """
    SELECT
      people.full_name,
      count(production.productions.id)
    FROM
      production.productions,
      production.relationships,
      production.people
    WHERE
      relationships.production_id = productions.id AND
      people.id = relationships.person_id AND
      productions.id = {0}
    GROUP BY
      production.people.full_name;
    """.format(productionid[0])
    knst_cur.execute(sql)
    for row in knst_cur.fetchall():
        personen.add(row)

DataFrame(list(personen), columns=["Lijst van personen", "Aantal producties"]).to_csv("lijst_van_personen.csv")