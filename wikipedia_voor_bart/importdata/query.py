import psycopg2
from configparser import ConfigParser
from pandas import DataFrame
from datetime import datetime

class datakunstenbetriples():
    def __init__(self):
        cfg = ConfigParser()
        cfg.read("../db.cfg")
        knst = psycopg2.connect(host=cfg['db']['host'], port=cfg['db']['port'],
                                database=cfg['db']['db'], user=cfg['db']['user'],
                                password=cfg['db']['pwd'])
        knst.set_client_encoding('UTF-8')
        self.cur = knst.cursor()

    def get_functie_en_organisatie(self, productie_id):
        sql = """
        SELECT
            relationships.production_id,
            functions.id,
            functions.name_nl,
            organisations.id,
            organisations.name
        FROM
            production.relationships,
            production.functions,
            production.productions,
            production.organisations
        WHERE
            productions.id = relationships.production_id AND
            relationships.function_id = functions.id AND
            organisations.id = relationships.organisation_id AND
            relationships.production_id = {0};
        """.format(productie_id)
        self.cur.execute(sql)
        return DataFrame(self.cur.fetchall(), columns=["productie_id", "functie_id", "functie", "organisatie_id", "organisatie"])

    def get_functie_en_persoon(self, productie_id):
        sql = """
                SELECT
                    relationships.production_id,
                    functions.id,
                    functions.name_nl,
                    people.id,
                    people.full_name
                FROM
                    production.relationships,
                    production.people,
                    production.functions,
                    production.productions
                WHERE
                    productions.id = relationships.production_id AND
                    relationships.function_id = functions.id AND
                    people.id = relationships.person_id AND
                    relationships.production_id = {0};
                """.format(productie_id)
        self.cur.execute(sql)
        return DataFrame(self.cur.fetchall(), columns=["productie_id", "functie_id", "functie", "persoon_id", "persoon"])

    def get_premieredatum(self, productie_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day
        FROM
          production.productions,
          production.shows,
          production.show_types,
          production.date_isaars
        WHERE
          shows.date_id = date_isaars.id AND
          shows.show_type_id = show_types.id AND
          show_types.name_nl = 'première' AND
          shows.production_id = productions.id AND
          productions.id = {0};
        """.format(productie_id)
        self.cur.execute(sql)
        dt = self.cur.fetchone()
        return DataFrame([[productie_id, "premièredatum", datetime(dt[0], dt[1], dt[2])]], columns=["productie_id", "relatietype", "datum"])

triples = datakunstenbetriples()
pfo = triples.get_functie_en_organisatie(460936)
pfp = triples.get_functie_en_persoon(460936)
ppremieredatum = triples.get_premieredatum(460936)

print(pfo)
print(pfp)
print(ppremieredatum)