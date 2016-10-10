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
SELECT DISTINCT
  production.organisations.name
FROM
  production.organisations,
  production.countries,
  production.productions,
  production.relationships,
  production.functions,
  production.date_isaars
WHERE
  organisations.country_id = countries.id AND
  organisations.id = relationships.organisation_id AND
  relationships.production_id = productions.id AND
  relationships.function_id = functions.id AND
  date_isaars.id = productions.begin_date_id AND
  countries.name_en = 'Belgium' AND
  (functions.name_nl = 'gezelschap' OR
  functions.name_nl = 'producent') AND
  date_isaars.year >= 1993;
"""

lijst_van_organisaties = []
knst_cur.execute(sql)
for row in knst_cur.fetchall():
    lijst_van_organisaties.append(row[0])

DataFrame(lijst_van_organisaties, columns=["Lijst van organisaties"]).to_csv("lijst_van_organisaties.csv")

# lijst van alle personen die aan deze producties (1) verbonden zijn

sql = """
SELECT DISTINCT
  production.productions.id
FROM
  production.organisations,
  production.countries,
  production.productions,
  production.relationships,
  production.functions,
  production.date_isaars
WHERE
  organisations.country_id = countries.id AND
  organisations.id = relationships.organisation_id AND
  relationships.production_id = productions.id AND
  relationships.function_id = functions.id AND
  date_isaars.id = productions.begin_date_id AND
  countries.name_en = 'Belgium' AND
  (functions.name_nl = 'gezelschap' OR
  functions.name_nl = 'producent') AND
  date_isaars.year >= 1993;
"""

personen = set()

knst_cur.execute(sql)
producties = knst_cur.fetchall()
for productionid in producties:
    sql = """
    SELECT
      people.full_name
    FROM
      production.productions,
      production.relationships,
      production.people
    WHERE
      relationships.production_id = productions.id AND
      people.id = relationships.person_id AND
      productions.id = {0};
    """.format(productionid[0])
    knst_cur.execute(sql)
    for row in knst_cur.fetchall():
        personen.add(row[0])

DataFrame(list(personen), columns=["Lijst van personen"]).to_csv("lijst_van_personen.csv")