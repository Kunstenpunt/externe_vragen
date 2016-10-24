import psycopg2
from configparser import ConfigParser
from pandas import DataFrame, concat
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
        os = self.cur.fetchall()
        return DataFrame([[o[0], o[2] + "_" + str(o[1]), o[4] + "_" + str(o[3])] for o in os], columns=["productie_id", "relatie", "value"])

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
        os = self.cur.fetchall()
        return DataFrame([[o[0], o[2] + "_" + str(o[1]), o[4] + "_" + str(o[3])] for o in os], columns=["productie_id", "relatie", "value"])

    def get_premieredatum_en_locatie(self, productie_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day,
            venues.name,
            venues.city
        FROM
          production.productions,
          production.shows,
          production.show_types,
          production.date_isaars,
          production.venues
        WHERE
          shows.venue_id = venues.id AND
          shows.date_id = date_isaars.id AND
          shows.show_type_id = show_types.id AND
          show_types.name_nl = 'première' AND
          shows.production_id = productions.id AND
          productions.id = {0};
        """.format(productie_id)
        self.cur.execute(sql)
        dt = self.cur.fetchone()
        return DataFrame([[productie_id, "premiere", datetime(dt[0], dt[1], dt[2]).isoformat() + " (" + dt[3] + ", " + dt[4] + ")"]], columns=["productie_id", "relatie", "value"])

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
        tts = self.cur.fetchall()
        return DataFrame([[str(tt[0]), tt[1], tt[2] + "_" + str(tt[3])] for tt in tts], columns=["productie_id", "relatie", "value"])

    @staticmethod
    def _jarenlijst_naar_periode(jarenlijst):
        jaren = sorted(jarenlijst)
        periodes = []
        periode = {"start": jaren[0], "einde": jaren[0]}
        for jaar in jaren[1:]:
            if (jaar - periode["einde"] - 1) == 0:
                periode["einde"] = jaar
            else:
                periodes.append(periode)
                periode = {"start": jaar, "einde": jaar}
        periodes.append(periode)
        return periodes

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
        periodes = self._jarenlijst_naar_periode(jaren)
        lines = []
        for periode in periodes:
            lines.append([productie_id, "speelperiode", str(periode["start"]) + "-" + str(periode["einde"])])
        return DataFrame(lines, columns=["productie_id", "relatie", "value"])

    def get_productiegegevens(self, productie_id):
        pfo = triples.get_functie_en_organisatie(productie_id)
        pfp = triples.get_functie_en_persoon(productie_id)
        ppremieredatumlocatie = triples.get_premieredatum_en_locatie(productie_id)
        theaterteksten = triples.get_gelinkte_theaterteksten(productie_id)
        speelperiode = triples.get_speelperiode(productie_id)
        return concat([pfo, pfp, ppremieredatumlocatie, theaterteksten, speelperiode])

triples = datakunstenbetriples()
productiegegevens = triples.get_productiegegevens(448736)
productiegegevens.to_csv("productiegegevens.csv")

