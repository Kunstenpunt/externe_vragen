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

    def get_productie_titel(self, productie_id):
        sql = """
        SELECT
            productions.title
        FROM
            production.productions
        WHERE productions.id = {0}
        """.format(productie_id)
        self.cur.execute(sql)
        os = self.cur.fetchall()
        return DataFrame([[productie_id, "titel", o[0]] for o in os], columns=["productie_id", "relatie", "value"])

    def get_productie_functie_en_organisatie(self, productie_id):
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

    def get_productie_functie_en_persoon(self, productie_id):
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

    def get_productie_premieredatum_en_locatie(self, productie_id):
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

    def get_productie_theaterteksten(self, productie_id):
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

    def get_productie_speelperiode(self, productie_id):
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
        tit = triples.get_productie_titel(productie_id)
        pfo = triples.get_productie_functie_en_organisatie(productie_id)
        pfp = triples.get_productie_functie_en_persoon(productie_id)
        ppremieredatumlocatie = triples.get_productie_premieredatum_en_locatie(productie_id)
        theaterteksten = triples.get_productie_theaterteksten(productie_id)
        speelperiode = triples.get_productie_speelperiode(productie_id)
        return concat([tit, pfo, pfp, ppremieredatumlocatie, theaterteksten, speelperiode])

    def get_persoongegevens(self, persoon_id):
        naam = triples.get_persoon_naam(persoon_id)
        geboortedatum = triples.get_persoon_geboortedatum(persoon_id)
        sterftedatum = triples.get_persoon_sterftedatum(persoon_id)
        locatie = triples.get_persoon_locatie(persoon_id)
        land = triples.get_persoon_land(persoon_id)
        gender = triples.get_persoon_gender(persoon_id)
        website = triples.get_persoon_website(persoon_id)
        subsidies = triples.get_persoon_subsidies(persoon_id)
        theaterteksten = triples.get_persoon_theaterteksten(persoon_id)
        return concat([naam, geboortedatum, sterftedatum, locatie, land, gender, website, subsidies, theaterteksten])

    def get_persoon_naam(self, persoon_id):
        sql = """
        SELECT
            people.full_name
        FROM
            production.people
        WHERE people.id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "naam", os[0]]], columns=["persoon_id", "relatie", "value"])

    def get_persoon_geboortedatum(self, persoon_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day
        FROM
            production.people
        INNER JOIN
            production.date_isaars
        ON
            people.birthdate_id = date_isaars.id
        WHERE people.id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "geboortedatum", datetime(os[0], os[1], os[2]) if os else "NA"]], columns=["persoon_id", "relatie", "value"])

    def get_persoon_sterftedatum(self, persoon_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day
        FROM
            production.people
        INNER JOIN
            production.date_isaars
        ON
            people.death_date_id = date_isaars.id
        WHERE people.id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "sterftedatum", datetime(os[0], os[1], os[2]) if os else "NA"]], columns=["persoon_id", "relatie", "value"])

    def get_persoon_locatie(self, persoon_id):
        sql = """
        SELECT
            locations.city_nl
        FROM
            production.people
        INNER JOIN
            production.locations
        ON
            people.location_id = locations.id
        WHERE people.id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "locatie", os[0]]], columns=["persoon_id", "relatie", "value"])

    def get_persoon_land(self, persoon_id):
        sql = """
        SELECT
            countries.name_nl
        FROM
            production.people
        INNER JOIN
            production.countries
        ON
            people.country_id = countries.id
        WHERE people.id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "land", os[0]]], columns=["persoon_id", "relatie", "value"])

    def get_persoon_gender(self, persoon_id):
        sql = """
        SELECT
            genders.name_nl
        FROM
            production.people
        INNER JOIN
            production.genders
        ON
            people.gender_id = genders.id
        WHERE people.id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "gender", os[0]]], columns=["persoon_id", "relatie", "value"])

    def get_persoon_website(self, persoon_id):
        sql = """
        SELECT
            people.url
        FROM
            production.people
        WHERE people.id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "website", os[0]]], columns=["persoon_id", "relatie", "value"])

    def get_persoon_subsidies(self, persoon_id):
        sql = """
        SELECT
            subsidy_types.title_nl,
            grants.period
        FROM
            production.grants
        INNER JOIN
            production.subsidy_types
        ON
            grants.subsidy_type_id = subsidy_types.id
        WHERE grants.person_id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "subsidie (jaar)", os[0] + " (" + str(os[1]) + ")" if os else "NA"]], columns=["persoon_id", "relatie", "value"])

    def get_persoon_theaterteksten(self, persoon_id):
        sql = """
        SELECT
          relationships.person_id,
          book_titles.class_name,
          book_titles.title_nl,
          book_titles.id
        FROM
          production.book_titles,
          production.relationships
        WHERE
          relationships.book_title_id = book_titles.id AND
          book_titles.class_name = 'TheaterText' AND
          relationships.person_id = {0};
        """.format(persoon_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(tt[0]), tt[1], tt[2] + "_" + str(tt[3])] for tt in tts],
                         columns=["persoon_id", "relatie", "value"])

    def get_organisatiegegevens(self, organisatie_id):
        naam = triples.get_organisatie_naam(organisatie_id)
        oprichtingsdatum = triples.get_organisatie_oprichtingsdatum(organisatie_id)
        einddatum = triples.get_organisatie_einddatum(organisatie_id)
        start_activiteiten = triples.get_organisatie_activiteitenstart(organisatie_id)
        einde_activiteiten = triples.get_organisatie_activiteiteneinde(organisatie_id)
        locatie = triples.get_organisatie_locatie(organisatie_id)
        organisatierelaties = triples.get_organisatie_relaties(organisatie_id)
        website = triples.get_organisatie_website(organisatie_id)
        subsidies = triples.get_organisatie_subsidies(organisatie_id)
        theaterteksten = triples.get_organisatie_theaterteksten(organisatie_id)
        return concat([naam, oprichtingsdatum, einddatum, start_activiteiten, einde_activiteiten, locatie, organisatierelaties, website, archiefwebsite, subsidies, theaterteksten])

    def get_organisatie_naam(self, organisatie_id):
        sql = """
        SELECT
            organisations.name
        FROM
            production.organisatie
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "naam", os[0]]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_oprichtingsdatum(self, organisatie_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day
        FROM
            production.organisations
        INNER JOIN
            production.date_isaars
        ON
            organisations.creation_date_id = date_isaars.id
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "oprichtingsdatum", datetime(os[0], os[1], os[2]) if os else "NA"]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_einddatum(self, organisatie_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day
        FROM
            production.organisations
        INNER JOIN
            production.date_isaars
        ON
            organisations.cancellation_date_id = date_isaars.id
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "einddatum", datetime(os[0], os[1], os[2]) if os else "NA"]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_activiteitenstart(self, organisatie_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day
        FROM
            production.organisations
        INNER JOIN
            production.date_isaars
        ON
            organisations.start_activities_date_id = date_isaars.id
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "activiteitenstart", datetime(os[0], os[1], os[2]) if os else "NA"]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_activiteiteneinde(self, organisatie_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day
        FROM
            production.organisations
        INNER JOIN
            production.date_isaars
        ON
            organisations.end_activities_date_id = date_isaars.id
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "activiteiteneinde", datetime(os[0], os[1], os[2]) if os else "NA"]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_locatie(self, organisatie_id):
        sql = """
        SELECT
            organisations.city
        FROM
            production.organisatie
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "locatie", os[0]]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_relaties(self, organisatie_id):
        pass

    def get_organisatie_website(self, organisatie_id):
        pass

    def get_organisatie_subsidies(self, organisatie_id):
        pass

    def get_organisatie_theaterteksten(self, organisatie_id):
        pass

    def get_theatertekstgegevens(self, theatertekst_id):
        titel = triples.get_theatertekst_titel(theatertekst_id)
        auteurs = triples.get_theatertekst_auteurs(theatertekst_id)
        vertalers = triples.get_theatertekst_vertalers(theatertekst_id)
        uitgevers = triples.get_theatertekst_uitgevers(theatertekst_id)
        isbn_ean = triples.get_theatertekst_isbn_ean(theatertekst_id)

    def get_theatertekst_isbn_ean(self, theatertekst_id):
        pass

    def get_theatertekst_uitgevers(self, theatertekst_id):
        pass

    def get_theatertekst_vertalers(self, theatertekst_id):
        pass

    def get_theatertekst_auteurs(self, theatertekst_id):
        pass

    def get_theatertekst_titel(self, theatertekst_id):
        pass

triples = datakunstenbetriples()
a = triples.get_productiegegevens(448736)
b = triples.get_productiegegevens(451020)
c = triples.get_productiegegevens(438835)
d = triples.get_productiegegevens(451382)
e = triples.get_productiegegevens(440557)
productiegegevens = concat([a, b, c, d, e])
productiegegevens.to_csv("productiegegevens.csv", index=False)

f = triples.get_persoongegevens(1878826)
g = triples.get_persoongegevens(1909965)
h = triples.get_persoongegevens(1878235)
i = triples.get_persoongegevens(1879952)
j = triples.get_persoongegevens(1884464)
persoongegevens = concat([f, g, h, i, j])
persoongegevens.to_csv("persoongegevens.csv", index=False)

k = triples.get_organisatiegegevens(370945)
l = triples.get_organisatiegegevens(363214)
m = triples.get_organisatiegegevens(363497)
n = triples.get_organisatiegegevens(379035)
o = triples.get_organisatiegegevens(374188)
organisatiegegevens = concat([k, l, m, n, o])
organisatiegegevens.to_csv("organisatiegegevens.csv", index=False)

# df = concat([productiegegevens, persoongegevens])
# theaterteksten_ids = set(df[df["relatie"] == "TheaterText"]["value"].values)
# theatertekstgegevens = []
# for tid in theaterteksten_ids:
#     tid = tid.split("_")[-1]
#     theatertekstgegevens.append(triples.get_theatertekstgegevens(tid))
# concat(theatertekstgegevens).to_csv("theatertekstgegevens.csv", index=False)
