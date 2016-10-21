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

    def get_gelinkte_theaterteksten(self, productie_id):
        sql = """
        SELECT
          relationships.production_id,
          book_titles.class_name,
          book_titles.title_nl,
          book_titles.id
        FROM
          production.book_titles,
          production.relationships
        WHERE
          relationships.book_title_id = book_titles.id AND
          book_titles.class_name = 'TheaterText' AND
          relationships.production_id = {0};
        """.format(productie_id)
        self.cur.execute(sql)
        return DataFrame(self.cur.fetchall(), columns=["productie_id", "document_type", "document_titel", "document_id"])

    def get_speelperiode(self, productie_id):
        # is dit een herneming?
        sql = """
        SELECT productions.rerun_of_id, seasons.start_year, seasons.end_year
        FROM production.productions, production.seasons
        WHERE productions.id = {0} AND productions.season_id = seasons.id
        """.format(productie_id)
        self.cur.execute(sql)
        herneming_van, startjaar, eindjaar = self.cur.fetchone()

        # dan zoek naar herneming van de moederproductie
        if herneming_van is not None:
            productie_id = herneming_van

            # de jaren oproepen van de moederproductie
            sql = """
            SELECT seasons.start_year, seasons.end_year
            FROM production.productions, production.seasons
            WHERE productions.id = {0} AND productions.season_id = seasons.id
            """.format(productie_id)
            self.cur.execute(sql)
            startjaar, eindjaar = self.cur.fetchone()

        # hernemingen van rootproductie
        sql = """
        SELECT
          seasons.start_year,
          seasons.end_year
        FROM
          production.productions,
          production.seasons
        WHERE
          productions.season_id = seasons.id AND
          productions.rerun_of_id = {0};
        """.format(productie_id)
        self.cur.execute(sql)
        jaren = set([startjaar, eindjaar])
        for item in self.cur.fetchall():
            jaren.add(item[0])
            jaren.add(item[1])

        lines = []
        for jaar in jaren:
            lines.append([productie_id, "speeljaar", jaar])
        return DataFrame(lines, columns=["productie_id", "relatie", "speeljaar"])

triples = datakunstenbetriples()
pfo = triples.get_functie_en_organisatie(448736)
pfp = triples.get_functie_en_persoon(448736)
ppremieredatum = triples.get_premieredatum(448736)
theaterteksten = triples.get_gelinkte_theaterteksten(448736)
speelperiode = triples.get_speelperiode(448738)

pfp.to_csv("gelinkte personen.csv")
pfo.to_csv("gelinkte organisaties.csv")
ppremieredatum.to_csv("premieredatum.csv")
theaterteksten.to_csv("theaterteksten.csv")
speelperiode.to_csv("speelperiode.csv")
