import psycopg2
from configparser import ConfigParser
from pandas import DataFrame

with open("prod_ids","r") as f:
    prod_ids = ",".join([item.strip() for item in f.read().split("\n")])

cfg = ConfigParser()
cfg.read("db.cfg")
knst = psycopg2.connect(host=cfg['db']['host'], port=cfg['db']['port'],
                        database=cfg['db']['db'], user=cfg['db']['user'],
                        password=cfg['db']['pwd'])
knst.set_client_encoding('UTF-8')
cur = knst.cursor()

sql = """

SELECT DISTINCT org.id, org.name,
rel_org_date_creation.day, rel_org_date_creation.month, rel_org_date_creation.year ,
rel_org_date_cancel.day, rel_org_date_cancel.month, rel_org_date_cancel.year ,
rel_org_date_start_act.day, rel_org_date_start_act.month, rel_org_date_start_act.year,
rel_org_date_end_act.day, rel_org_date_end_act.month, rel_org_date_end_act.year,
rel_org_countries.name_nl, org.archive_url,
rel_org_location.zip_code, org.url, rel_org_location.city_nl


FROM production.organisations AS org

LEFT JOIN production.countries AS rel_org_countries
ON org.country_id = rel_org_countries.id

LEFT JOIN production.date_isaars AS rel_org_date_creation
ON org.creation_date_id = rel_org_date_creation.id

LEFT JOIN production.date_isaars AS rel_org_date_cancel
ON org.cancellation_date_id = rel_org_date_cancel.id

LEFT JOIN production.date_isaars AS rel_org_date_start_act
ON org.start_activities_date_id = rel_org_date_start_act.id

LEFT JOIN production.date_isaars AS rel_org_date_end_act
ON org.end_activities_date_id = rel_org_date_end_act.id

LEFT JOIN production.locations AS rel_org_location
ON org.location_id = rel_org_location.id

LEFT JOIN production.organisation_types_organisations AS org_types
ON org.id = org_types.organisation_id

LEFT JOIN production.relationships as rel
ON rel.organisation_id = org.id


WHERE rel.production_id IN ({0})

""".format(prod_ids)

print(sql)

from datetime import date
from pandas import isnull

cur.execute(sql)
organisations = cur.fetchall()
df = DataFrame(organisations, columns = ["ID","Organisation_name", "creation day", "creation month", "creation year", "end_day", "end_month", "end_year", "start_act_day",
                                        "start_act_month", "start_act_year", "end_act_day","end_act_month","end_act_year", "country", "archief_website", "zip_code", "website", "stad"])

df['Creation_date'] = df[df.columns[2:5]].apply(lambda x: date(int(x[2]) if not isnull(x[2]) else 1,
                                                               int(x[1]) if not isnull(x[1]) else 1,
                                                               int(x[0]) if not isnull(x[0]) else 1), axis=1)
df['End_date'] = df[df.columns[5:8]].apply(lambda x: date(int(x[2]) if not isnull(x[2]) else 1,
                                                               int(x[1]) if not isnull(x[1]) else 1,
                                                               int(x[0]) if not isnull(x[0]) else 1), axis=1)
df['Start_activities_date'] = df[df.columns[8:11]].apply(lambda x: date(int(x[2]) if not isnull(x[2]) else 1,
                                                               int(x[1]) if not isnull(x[1]) else 1,
                                                               int(x[0]) if not isnull(x[0]) else 1), axis=1)
df['End_activities_date'] = df[df.columns[11:14]].apply(lambda x: date(int(x[2]) if not isnull(x[2]) else 1,
                                                               int(x[1]) if not isnull(x[1]) else 1,
                                                               int(x[0]) if not isnull(x[0]) else 1), axis=1)

df = df.drop(['creation day', 'creation month', 'creation year', "end_day", "end_month", "end_year", "start_act_day",
                                        "start_act_month", "start_act_year", "end_act_day","end_act_month","end_act_year"], axis=1)


sql_2 = """

SELECT DISTINCT org_from.id, org_from.name, org_rel_types.from_name_nl, org_to.id, org_to.name

FROM production.organisations AS org_from

LEFT JOIN production.organisation_relations org_rel
ON org_from.id = org_rel.from_id

LEFT JOIN production.organisations AS org_to
ON org_rel.to_id = org_to.id

LEFT JOIN production.organisation_relation_types AS org_rel_types
ON org_rel_types.id = org_rel.organisation_relation_type_id

LEFT JOIN production.organisation_types_organisations AS org_types
ON org_from.id = org_types.organisation_id

LEFT JOIN production.relationships as rel
ON rel.organisation_id = org_from.id


WHERE rel.production_id IN ({0})

""".format(prod_ids)

cur.execute(sql_2)
organisations = cur.fetchall()
df_2 = DataFrame(organisations, columns=["from_id", "from_name", "relation_name", "to_id", "to_name"])


relations_between_orgs = []
for row in df_2.iterrows():
    from_name = row[1]["from_id"]
    relation = row[1]["relation_name"]
    to_name = row[1]["to_name"]
    relations_between_orgs.append({"index": from_name, relation: to_name})
df_2_relations = DataFrame(relations_between_orgs)
df_2_relations_nafilled_and_index_resetted = df_2_relations.fillna(0).reset_index().drop(["level_0", None], axis=1)
df_2_relations_nafilled_and_index_resetted.head()

dfinal = df.merge(df_2_relations_nafilled_and_index_resetted, how='inner', left_on="ID", right_on='index')


dfinal= dfinal.drop("index", axis=1)
dfinal= dfinal.drop("Fusie van", axis=1)
dfinal = dfinal.drop("Soirée Composée van", axis=1)
dfinal = dfinal.drop("archief_website", axis=1)

for index, serie in dfinal.iterrows():
    for kolomnaam in serie.index:
        value = serie[kolomnaam]
        if isinstance(value, list) and len(value) == 1:
            dfinal.at[index, kolomnaam] = value[0]
        if isinstance(value, list) and len(value) > 1:
            new_df = DataFrame([serie]*len(value)).reset_index(drop=True)
            for new_df_index, new_df_row in new_df.iterrows():
                new_df.at[new_df_index, kolomnaam] = value[new_df_index]
            dfinal = dfinal.append(new_df, ignore_index=True)

dfinal.to_csv("kunstenpunt_organisaties_metadata_test_2.csv", encoding="utf-8")
