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

    def get_productie_premieredatum(self, productie_id):
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
        return DataFrame([[productie_id, "premiere_datum", datetime(dt[0], dt[1], dt[2]).isoformat()]], columns=["productie_id", "relatie", "value"])

    def get_productie_premierelocatie(self, productie_id):
        sql = """
        SELECT
            date_isaars.year,
            date_isaars.month,
            date_isaars.day,
            venues.name,
            venues.id
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
        return DataFrame([[productie_id, "premiere_locatie", dt[3] + "_" + str(dt[4])]], columns=["productie_id", "relatie", "value"])


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
        ppremieredatum = triples.get_productie_premieredatum(productie_id)
        ppremierelocatie = triples.get_productie_premierelocatie(productie_id)
        speelperiode = triples.get_productie_speelperiode(productie_id)
        return concat([tit, pfo, pfp, ppremieredatum, ppremierelocatie, speelperiode])

    def get_persoongegevens(self, persoon_id):
        naam = triples.get_persoon_naam(persoon_id)
        geboortedatum = triples.get_persoon_geboortedatum(persoon_id)
        sterftedatum = triples.get_persoon_sterftedatum(persoon_id)
        locatie = triples.get_persoon_locatie(persoon_id)
        land = triples.get_persoon_land(persoon_id)
        gender = triples.get_persoon_gender(persoon_id)
        website = triples.get_persoon_website(persoon_id)
        archiefwebsite = triples.get_persoon_archiefwebsite(persoon_id)
        return concat([naam, geboortedatum, sterftedatum, locatie, land, gender, website, archiefwebsite])

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

    def get_persoon_archiefwebsite(self, persoon_id):
        sql = """
        SELECT
            people.archive_url
        FROM
            production.people
        WHERE people.id = {0}
        """.format(persoon_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[persoon_id, "archiefwebsite", os[0]]], columns=["persoon_id", "relatie", "value"])

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
        land = triples.get_organisatie_land(organisatie_id)
        organisatierelaties = triples.get_organisatie_relaties(organisatie_id)
        website = triples.get_organisatie_website(organisatie_id)
        archiefwebsite = triples.get_organisatie_archiefwebsite(organisatie_id)
        return concat([naam, oprichtingsdatum, einddatum, start_activiteiten, einde_activiteiten, locatie, land, organisatierelaties, website, archiefwebsite])

    def get_organisatie_naam(self, organisatie_id):
        sql = """
        SELECT
            organisations.name
        FROM
            production.organisations
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
        return DataFrame([[organisatie_id, "oprichtingsdatum", datetime(os[0], os[1] if os[1] else 1, os[2] if os[2] else 1) if os else "NA"]], columns=["organisatie_id", "relatie", "value"])

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
            production.organisations
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "locatie", os[0]]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_land(self, organisatie_id):
        sql = """
        SELECT
            countries.name_nl
        FROM
            production.organisations
        INNER JOIN
            production.countries
        ON
            organisations.country_id = countries.id
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "land", os[0] if os else "NA"]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_relaties(self, organisatie_id):
        sql = """
        SELECT
          relationships.organisation_from_id,
          relationships.organisation_relation_type_id,
          organisation_relation_types.from_name_nl,
          organisations.name
        FROM
          production.relationships,
          production.organisation_relation_types,
          production.organisations
        WHERE
          relationships.organisation_relation_type_id = organisation_relation_types.id AND
          relationships.organisation_from_id = organisations.id AND
          relationships.organisation_to_id = {0};""".format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchall()
        df_to = DataFrame([[organisatie_id, o[2] + "_" + str(o[1]), o[3] + "_" + str(o[0])] for o in os], columns=["organisatie_id", "relatie", "value"])

        sql = """
        SELECT
          relationships.organisation_to_id,
          relationships.organisation_relation_type_id,
          organisation_relation_types.to_name_nl,
          organisations.name
        FROM
          production.relationships,
          production.organisation_relation_types,
          production.organisations
        WHERE
          relationships.organisation_relation_type_id = organisation_relation_types.id AND
          relationships.organisation_to_id = organisations.id AND
          relationships.organisation_from_id = {0};""".format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchall()
        df_from = DataFrame([[organisatie_id, o[2] + "_" + str(o[1]), o[3] + "_" + str(o[0])] for o in os], columns=["organisatie_id", "relatie", "value"])
        return concat([df_to, df_from])

    def get_organisatie_website(self, organisatie_id):
        sql = """
        SELECT
            organisations.url
        FROM
            production.organisations
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "website", os[0]]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_archiefwebsite(self, organisatie_id):
        sql = """
        SELECT
            organisations.archive_url
        FROM
            production.organisations
        WHERE organisations.id = {0}
        """.format(organisatie_id)
        self.cur.execute(sql)
        os = self.cur.fetchone()
        return DataFrame([[organisatie_id, "archiefwebsite", os[0]]], columns=["organisatie_id", "relatie", "value"])

    def get_organisatie_theaterteksten(self, organisatie_id):
        sql = """
        SELECT
          relationships.organisation_id,
          book_titles.class_name,
          book_titles.title_nl,
          book_titles.id
        FROM
          production.book_titles,
          production.relationships
        WHERE
          relationships.book_title_id = book_titles.id AND
          book_titles.class_name = 'TheaterText' AND
          relationships.organisation_id = {0};
        """.format(organisatie_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(tt[0]), tt[1], tt[2] + "_" + str(tt[3])] for tt in tts], columns=["organisatie_id", "relatie", "value"])

    def get_theatertekstgegevens(self, theatertekst_id):
        titel = triples.get_theatertekst_titel(theatertekst_id)
        uitgevers = triples.get_theatertekst_uitgevers(theatertekst_id)
        isbn_ean = triples.get_theatertekst_isbn_ean(theatertekst_id)
        organisaties = triples.get_theatertekst_organisaties(theatertekst_id)
        personen = triples.get_theatertekst_personen(theatertekst_id)
        producties = triples.get_theatertekst_producties(theatertekst_id)
        return concat([titel, uitgevers, isbn_ean, organisaties, personen, producties])

    def get_theatertekst_isbn_ean(self, theatertekst_id):
        sql = """SELECT
                   book_titles.id,
                   book_titles.ean
                 FROM
                   production.book_titles
                 WHERE
                   book_titles.id = {0};""".format(theatertekst_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(tt[0]), "isbn_ean", str(tt[1])] for tt in tts],
                         columns=["theatertekst_id", "relatie", "value"])

    def get_theatertekst_uitgevers(self, theatertekst_id):
        sql = """SELECT
                    impressums.publisher,
                    impressums.id

                FROM
                    production.book_titles,
                    production.impressums,
                    production.book_title_impressums
                WHERE
                    book_titles.id = book_title_impressums.book_title_id
                AND
                    book_title_impressums.impressum_id = impressums.id
                AND
                    book_titles.id = {0};""".format(theatertekst_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(theatertekst_id), "uitgever_id", str(tt[0]) + "_" + str(tt[1])] for tt in tts],
                         columns=["theatertekst_id", "relatie", "value"])

    def get_theatertekst_titel(self, theatertekst_id):
        sql = """SELECT
                   book_titles.id,
                   book_titles.title_nl
                 FROM
                   production.book_titles
                 WHERE
                   book_titles.id = {0};""".format(theatertekst_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(tt[0]), "titel", str(tt[1])] for tt in tts],
                         columns=["theatertekst_id", "relatie", "value"])

    def get_theatertekst_organisaties(self, theatertekst_id):
        sql = """SELECT
                   book_titles.id,
                   relationships.organisation_id
                 FROM
                   production.book_titles,
                   production.relationships
                 WHERE
                   relationships.book_title_id = book_titles.id
                 AND
                   book_titles.id = {0};""".format(theatertekst_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(tt[0]), "over_organisation_id", str(tt[1])] for tt in tts if tt[1]],
                         columns=["theatertekst_id", "relatie", "value"])

    def get_theatertekst_personen(self, theatertekst_id):
        sql = """SELECT
                  book_titles.id,
                  people.id,
                  people.full_name,
                  functions.name_nl
                FROM
                  production.book_titles,
                  production.relationships,
                  production.people,
                  production.functions
                WHERE
                  relationships.book_title_id = book_titles.id AND
                  relationships.person_id = people.id AND
                  relationships.function_id = functions.id AND
                  book_titles.id = {0} ;""".format(theatertekst_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(tt[0]), tt[3], str(tt[2]) + "_" + str(tt[1])] for tt in tts],
                         columns=["theatertekst_id", "relatie", "value"])

    def get_theatertekst_producties(self, theatertekst_id):
        sql = """SELECT
                    book_titles.id,
                    relationships.production_id
                 FROM
                    production.book_titles,
                    production.relationships
                WHERE
                    relationships.book_title_id = book_titles.id
                AND
                    book_titles.id = {0};""".format(theatertekst_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(tt[0]), "over_productie_id", str(tt[1])] for tt in tts if tt[1]],
                         columns=["theatertekst_id", "relatie", "value"])

    def get_subsidie_ids(self):
        sql = """
        SELECT id
        FROM production.grants
        """
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return [tt[0] for tt in tts]

    def get_subsidiegegevens(self, subsidie_id):
        subsidiesysteem = triples.get_subsidie_systeem(subsidie_id)
        persoon = triples.get_subsidie_persoon(subsidie_id)
        organisatie = triples.get_subsidie_organisatie(subsidie_id)
        beginjaar = triples.get_subsidie_beginjaar(subsidie_id)
        eindjaar = triples.get_subsidie_eindjaar(subsidie_id)
        subsidietype = triples.get_subsidie_type(subsidie_id)
        subsidiecommissie = triples.get_subsidie_commissie(subsidie_id)
        instantie = triples.get_subsidie_instantie(subsidie_id)
        return concat([subsidiesysteem, persoon, organisatie, beginjaar, eindjaar, subsidietype, subsidiecommissie, instantie])

    def get_subsidie_instantie(self, subsidie_id):
        sql = """
        SELECT
          subsidy_sponsors.id,
          subsidy_sponsors.title_nl,
          date_isaars.year
        FROM
          production.grants
        JOIN production.subsidy_sponsors
        ON grants.subsidy_sponsor_id = subsidy_sponsors.id
        LEFT JOIN production.date_isaars
        ON date_isaars.id = grants.begin_date_id
        WHERE
          grants.id = {0}""".format(subsidie_id)
        self.cur.execute(sql)
        tt = self.cur.fetchone()
        beginjaar = 2014 if not tt[2] else tt[2]
        if 1993 <= beginjaar < 2006:
            corrected_instantie = "Podiumkunstendecreet_10"
        elif 1975 <= beginjaar < 1993:
            corrected_instantie = "Theaterdecreet_11"
        else:
            corrected_instantie = "Kunstendecreet_1"
        return DataFrame([[str(subsidie_id), "subsidiërende instantie", corrected_instantie]],
                         columns=["subsidie_id", "relatie", "value"])

    def get_subsidie_systeem(self, subsidie_id):
        sql = """
                SELECT
                    grant_systems.id,
                    grant_systems.description_nl
                FROM
                  production.grants,
                  production.grant_systems
                WHERE
                  grants.grant_system_id = grant_systems.id AND
                  grants.id = {0}""".format(subsidie_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(subsidie_id), "subsidiesysteem", str(tt[1]).strip() + "_" + str(tt[0])] for tt in tts],
                         columns=["subsidie_id", "relatie", "value"])

    def get_subsidie_persoon(self, subsidie_id):
        sql = """
                SELECT
                    people.id,
                    people.full_name
                FROM
                  production.grants,
                  production.people
                WHERE
                  grants.person_id = people.id AND
                  grants.id = {0}""".format(subsidie_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(subsidie_id), "persoon", str(tt[1]) + "_" + str(tt[0])] for tt in tts],
                         columns=["subsidie_id", "relatie", "value"])

    def get_subsidie_organisatie(self, subsidie_id):
        sql = """
                SELECT
                    organisations.id,
                    organisations.name
                FROM
                  production.grants,
                  production.organisations
                WHERE
                  grants.organisation_id = organisations.id AND
                  grants.id = {0}""".format(subsidie_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(subsidie_id), "organisatie", str(tt[1]) + "_" + str(tt[0])] for tt in tts],
                         columns=["subsidie_id", "relatie", "value"])


    def get_subsidie_beginjaar(self, subsidie_id):
        sql = """
                SELECT
                  date_isaars.year
                FROM
                  production.grants,
                  production.date_isaars
                WHERE
                  grants.begin_date_id = date_isaars.id AND
                  grants.id = {0};""".format(subsidie_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(subsidie_id), "beginjaar", str(tt[0])] for tt in tts],
                         columns=["subsidie_id", "relatie", "value"])

    def get_subsidie_eindjaar(self, subsidie_id):
        sql = """
                SELECT
                  date_isaars.year
                FROM
                  production.grants,
                  production.date_isaars
                WHERE
                  grants.end_date_id = date_isaars.id AND
                          grants.id = {0};""".format(subsidie_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(subsidie_id), "eindjaar", str(tt[0])] for tt in tts],
                         columns=["subsidie_id", "relatie", "value"])

    def get_subsidie_type(self, subsidie_id):
        sql = """
                SELECT
                    subsidy_types.id,
                    subsidy_types.title_nl
                FROM
                  production.grants,
                  production.subsidy_types
                WHERE
                  grants.subsidy_type_id = subsidy_types.id AND
                  grants.id = {0}""".format(subsidie_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(subsidie_id), "subsidietype", str(tt[1]) + "_" + str(tt[0])] for tt in tts],
                         columns=["subsidie_id", "relatie", "value"])

    def get_subsidie_commissie(self, subsidie_id):
        sql = """
                SELECT
                    subsidy_committees.id,
                    subsidy_committees.title_nl
                FROM
                  production.grants,
                  production.subsidy_committees
                WHERE
                  grants.subsidy_committee_id = subsidy_committees.id AND
                  grants.id = {0}""".format(subsidie_id)
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return DataFrame([[str(subsidie_id), "subsidiecomite", str(tt[1]) + "_" + str(tt[0])] for tt in tts],
                         columns=["subsidie_id", "relatie", "value"])

    def get_premierelocatie_ids(self):
        sql = """
            SELECT DISTINCT
              shows.venue_id
            FROM
              production.shows
            JOIN production.date_isaars
            ON shows.date_id = date_isaars.id
            WHERE
              shows.show_type_id = 441 AND
              date_isaars.year >= 1993
            """
        self.cur.execute(sql)
        tts = self.cur.fetchall()
        return [tt[0] for tt in tts if tt[0] is not None]

    def get_premierelocatiegegevens(self, premierelocatie_id):
        naam = triples.get_premierelocatie_naam(premierelocatie_id)
        adres = triples.get_premierelocatie_adres(premierelocatie_id)
        stad = triples.get_premierelocatie_stad(premierelocatie_id)
        zip = triples.get_premierelocatie_zip(premierelocatie_id)
        land = triples.get_premierelocatie_land(premierelocatie_id)
        return concat([naam, adres, stad, zip, land])

    def get_premierelocatie_naam(self, premierelocatie_id):
        sql = """
            SELECT
                venues.name
            FROM
              production.venues
            WHERE
              venues.id = {0}""".format(premierelocatie_id)
        self.cur.execute(sql)
        tt = self.cur.fetchone()
        return DataFrame([[str(premierelocatie_id), "naam", str(tt[0])]],
                         columns=["premierelocatie_id", "relatie", "value"])

    def get_premierelocatie_adres(self, premierelocatie_id):
        sql = """
            SELECT
                venues.address_line_1
            FROM
              production.venues
            WHERE
              venues.id = {0}""".format(premierelocatie_id)
        self.cur.execute(sql)
        tt = self.cur.fetchone()
        return DataFrame([[str(premierelocatie_id), "adres", str(tt[0])]],
                         columns=["premierelocatie_id", "relatie", "value"])

    def get_premierelocatie_stad(self, premierelocatie_id):
        sql = """
            SELECT
                locations.city_nl
            FROM
              production.venues,
              production.locations
            WHERE
              locations.id = venues.location_id AND
              venues.id = {0}""".format(premierelocatie_id)
        self.cur.execute(sql)
        tt = self.cur.fetchone()
        return DataFrame([[str(premierelocatie_id), "stad", str(tt[0])]],
                         columns=["premierelocatie_id", "relatie", "value"])

    def get_premierelocatie_zip(self, premierelocatie_id):
        sql = """
            SELECT
                locations.zip_code
            FROM
              production.venues,
              production.locations
            WHERE
              locations.id = venues.location_id AND
              venues.id = {0}""".format(premierelocatie_id)
        self.cur.execute(sql)
        tt = self.cur.fetchone()
        return DataFrame([[str(premierelocatie_id), "postcode", str(tt[0])]],
                         columns=["premierelocatie_id", "relatie", "value"])


    def get_premierelocatie_land(self, premierelocatie_id):
        sql = """
            SELECT
                countries.name_nl
            FROM
              production.venues,
              production.locations,
              production.countries
            WHERE
              locations.id = venues.location_id AND
              countries.id = locations.country_id AND
              venues.id = {0}""".format(premierelocatie_id)
        self.cur.execute(sql)
        tt = self.cur.fetchone()
        if tt is not None:
            return DataFrame([[str(premierelocatie_id), "land", str(tt[0])]],
                         columns=["premierelocatie_id", "relatie", "value"])

triples = datakunstenbetriples()
# a = triples.get_productiegegevens(448736)
# b = triples.get_productiegegevens(451020)
# c = triples.get_productiegegevens(438835)
# d = triples.get_productiegegevens(451382)
# e = triples.get_productiegegevens(440557)
# productiegegevens = concat([a, b, c, d, e])
# productiegegevens["productie_id"] = productiegegevens["productie_id"].apply(int)
# productiegegevens.to_csv("productiegegevens.csv", index=False)

premierelocaties_list = []
for premierelocatie_id in triples.get_premierelocatie_ids():
    print(premierelocatie_id)
    a = triples.get_premierelocatiegegevens(premierelocatie_id)
    premierelocaties_list.append(a)
premierelocaties = concat(premierelocaties_list)
premierelocaties.to_csv("premierelocaties.csv", index=False)

# f = triples.get_persoongegevens(1878826)
# g = triples.get_persoongegevens(1909965)
# h = triples.get_persoongegevens(1878235)
# i = triples.get_persoongegevens(1879952)
# j = triples.get_persoongegevens(1884464)
# persoongegevens = concat([f, g, h, i, j])
# persoongegevens["persoon_id"] = persoongegevens["persoon_id"].apply(int)
# persoongegevens.to_csv("persoongegevens.csv", index=False)
#
# k = triples.get_organisatiegegevens(370945)
# l = triples.get_organisatiegegevens(363214)
# m = triples.get_organisatiegegevens(363497)
# n = triples.get_organisatiegegevens(379035)
# o = triples.get_organisatiegegevens(374188)
# organisatiegegevens = concat([k, l, m, n, o])
# organisatiegegevens["organisatie_id"] = organisatiegegevens["organisatie_id"].apply(int)
# organisatiegegevens.to_csv("organisatiegegevens.csv", index=False)
#
# p = triples.get_theatertekstgegevens(428000)
# q = triples.get_theatertekstgegevens(412806)
# r = triples.get_theatertekstgegevens(426162)
# s = triples.get_theatertekstgegevens(415016)
# t = triples.get_theatertekstgegevens(427945)
# theatertekstgegevens = concat([p, q, r, s, t])
# theatertekstgegevens.to_csv("theatertekstgegevens.csv", index=False)
#
# subsidiegegevens_list = []
# for subsidie_id in triples.get_subsidie_ids():
#     print(subsidie_id)
#     u = triples.get_subsidiegegevens(subsidie_id)
#     subsidiegegevens_list.append(u)
# subsidiegegevens = concat(subsidiegegevens_list)
# subsidiegegevens.to_csv("subsidiegegevens.csv", index=False)
