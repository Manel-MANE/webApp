import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import plotly.graph_objects as go
import ast


def connect_to_bq():
    # Create API client.
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )

    # client = storage.Client(credentials=credentials)
    client = bigquery.Client(credentials=credentials)
    return client


def liste_bornes(client):
    query = """ SELECT distinct (ID_PDC_local) FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.bornes` 
where ID_PDC_local !='null' and ID_PDC_local !='ID PDC local' """
    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    df = pd.DataFrame(rows)
    return df


def taux_occupation(data):

    for ind in data.index:
        id = data['ID_PDC_local'][ind]
        # element = {}
        # element['id']

        taux_par_borne = taux_occupation_par_borne(bq_client, id)
        taux_par_borne = taux_par_borne.to_dict()
        # list_element = {}
        # list_element['id'] = id
        # list_element['status'] = taux_par_borne


def filtrer_stations_selon_status(client, status):

    if status != '':
        condition = """  AND statut_pdc= '""" + status + """'"""
        query = """  SELECT distinct(id_pdc), coordonneesxy from 
        `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` as d1
        WHERE id_pdc!='NULL' AND coordonneesxy!= 'null' """ + condition + """ AND 
        timestamp = (SELECT MAX(timestamp) FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` 
        as d2 WHERE d1.id_pdc = d2.id_pdc) """

    else:
        query = """  SELECT distinct(id_pdc), coordonneesxy from 
        `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` 
        WHERE id_pdc!='NULL' AND coordonneesxy!= 'null' """
    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    df = pd.DataFrame(rows)
    # coordonneesxy are written with a single quote --> cannot be parsed in json (because it requires double quotes"
    # we need ast.literal_eval to traverse the expression
    df['coordonneesxy'] = df['coordonneesxy'].map(lambda d: ast.literal_eval(d))
    df_new = df.join(pd.DataFrame(df['coordonneesxy'].to_dict()).T)
    coord = df_new[["lat", "lon"]]
    return coord


def map_with_filter(client, status):
    stations_geo_coord = filtrer_stations_selon_status(client, status)
    st.map(stations_geo_coord)


def status_choice(option):
    if option == "Toutes":
        return ""
    elif option == "Disponibles":
        return "Disponible"
    elif option == "En charge":
        return "Occupé (en charge)"
    elif option == "En maintenance":
        return "En maintenance"


def taux_occupation_par_borne(client, id_pdc):

    # id_pdc = "FR*V75*E9001*02*1"
    query = """  SELECT statut_pdc , timestamp, last_updated, timestamp_diff(timestamp, 
    last_updated, second) as duration  FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record` 
    WHERE id_pdc = '""" + id_pdc + """' ORDER BY timestamp DESC  """
    query_job = client.query(query)
    result = query_job.result()
    rows = [dict(row) for row in result]
    df = pd.DataFrame(rows)
    last_updated_ref = df.at[0, 'last_updated']  # access the last_updated timestamp value
    ind = 0
    while ind < df.shape[0] - 1:
        if df['last_updated'][ind+1] == last_updated_ref:
            # df.drop(index=ind)
            df = df.drop(labels=ind+1, axis=0)
            df.reset_index(drop=True, inplace=True)
        elif df['last_updated'][ind+1] != last_updated_ref:
            last_updated_ref = df['last_updated'][ind+1]  # update the last_updated value
            ind += 1

    sum = df.groupby(['statut_pdc']).sum('duration')
    return sum


def pmr_2roues_filter_stations(client, status, pmr, deux_roues):
    if status != '':
        condition = """  AND statut_pdc= '""" + status + """'"""
        sub_query1 = """ with status as ( SELECT distinct(id_pdc), coordonneesxy from 
        `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` as d1
        WHERE id_pdc!='NULL' AND coordonneesxy!= 'null' """ + condition + """ AND 
        timestamp = (SELECT MAX(timestamp) FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` 
        as d2 WHERE d1.id_pdc = d2.id_pdc)), """

    else:
        sub_query1 = """  with status as (SELECT distinct(id_pdc), coordonneesxy from 
        `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` 
        WHERE id_pdc!='NULL' AND coordonneesxy!= 'null'), """

    pmr_filter = ""
    deux_roues_filter = ""
    if pmr:
        pmr_filter = """ and Accessibilite_PMR = 'Réservé PMR' """
    if deux_roues:
        deux_roues_filter = """  and Stationnement_2_roues= 'True' """
    sub_query2 = """ filters as (select distinct(ID_PDC_local), Accessibilite_PMR, Stationnement_2_roues
    from `acn-gcp-octo-hapi.ev_stations_datawarehouse.bornes`"""
    filter = "where ID_PDC_local!='null' " + pmr_filter + deux_roues_filter + " ) "
    jointure = """select id_pdc, coordonneesxy, Accessibilite_PMR, Stationnement_2_roues 
    from status as s inner join filters as f on s.id_pdc = f.ID_PDC_local """
    query = sub_query1 + sub_query2 + filter + jointure
    print(query)

    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    df = pd.DataFrame(rows)

    df['coordonneesxy'] = df['coordonneesxy'].map(lambda d: ast.literal_eval(d))
    df_new = df.join(pd.DataFrame(df['coordonneesxy'].to_dict()).T)
    coord = df_new[["lat", "lon"]]
    return coord


def caracteristiques_station(client, id_station):
    # id_pdc = "FR*V75*E9001*02*1"
    query = """  SELECT * FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.bornes` 
    WHERE ID_Station = '""" + id_station + """' """
    query_job = client.query(query)
    result = query_job.result()
    rows = [dict(row) for row in result]
    df = pd.DataFrame(rows)
    pmr = False
    deux_roues = False


def plot_taux_occupation(dataframe):
    fig = go.Figure(
      go.Pie(
          labels=dataframe.index,
          values=dataframe['duration'],

          hoverinfo="label+percent",
          textinfo="percent"
      ))
    # return(sum)
    st.plotly_chart(fig)


def taux_occupation_journalier(client, id_borne):

    query = """  with total_duration as (SELECT dte, sum(duration) as total_duration from (
Select statut_pdc, dte, most_recent_state_timestamp,state_starting_timestamp, 
timestamp_diff(most_recent_state_timestamp, state_starting_timestamp, second) as duration from (
SELECT statut_pdc , EXTRACT(DATE FROM timestamp) as dte, max(timestamp) as most_recent_state_timestamp,
min(last_updated) as state_starting_timestamp 
FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` 
WHERE timestamp is not null and last_updated is not null and id_pdc= '""" + id_borne + """' 
group by statut_pdc,dte)) 
group by dte), 

exploitation_duration as (Select statut_pdc, dte, most_recent_state_timestamp,state_starting_timestamp,   
timestamp_diff(most_recent_state_timestamp, state_starting_timestamp, second) as duration from (
SELECT statut_pdc , EXTRACT(DATE FROM timestamp) as dte, max(timestamp) as most_recent_state_timestamp, 
min(last_updated) as state_starting_timestamp
FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` 
WHERE timestamp is not null and last_updated is not null and id_pdc= '""" + id_borne + """' 
and statut_pdc='Occupé (en charge)'
group by statut_pdc,dte))
SELECT 
 t.dte, t.total_duration, e.statut_pdc, e.duration
FROM total_duration AS t
inner  JOIN exploitation_duration AS e
ON t.dte = e.dte
order by t.dte Desc
 """
    query_job = client.query(query)
    result = query_job.result()
    rows = [dict(row) for row in result]
    df = pd.DataFrame(rows)
    exploitation_rate = df['duration'] / df['total_duration'] * 100
    df['exploitation_rate'] = exploitation_rate
    exploitation_rate_per_day = df[['dte', 'exploitation_rate']]
    exploitation_rate_per_day.rename(columns={'dte': 'Date', 'exploitation_rate': 'Taux'}, inplace=True)
    return exploitation_rate_per_day


def bornes_en_maintenance(client):
    query = """  SELECT distinct(id_pdc), adresse_station  from 
    `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` 
    as d1 WHERE id_pdc!='NULL' AND coordonneesxy!= 'null'  AND statut_pdc= 'En maintenance' 
    AND  timestamp = (SELECT MAX(timestamp) FROM 
    `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched` 
    as d2 WHERE d1.id_pdc = d2.id_pdc) """
    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    liste_bornes_en_panne = pd.DataFrame(rows)
    liste_bornes_en_panne.rename(columns={'id_pdc': 'ID Station', 'adresse_station': 'Adresse'}, inplace=True)
    return liste_bornes_en_panne


def bornes_les_plus_en_panne(client):
    query = """ with total_down_time as ( select id_pdc, sum(down_time) as total_down_time from (
    select id_pdc , last_updated, max (duration) as down_time from (
    SELECT id_pdc, adresse_station,  timestamp, last_updated, timestamp_diff(timestamp, last_updated, hour) 
    as duration FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record`
    WHERE  statut_pdc='En maintenance' ORDER BY id_pdc, timestamp DESC) group by last_updated, 
    id_pdc ORDER BY id_pdc) group by id_pdc ), adresses as ( select distinct(id_pdc), adresse_station from
    `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record_enriched`   )
    select t.id_pdc, a.adresse_station, t.total_down_time from total_down_time as t inner join 
    adresses as a on t.id_pdc = a.id_pdc order by t.total_down_time DESC """
    query_job = client.query(query)
    result = query_job.result()
    rows = [dict(row) for row in result]
    df = pd.DataFrame(rows)
    top_down_time = df[0:10]
    return(top_down_time)


def taux_utilisation_par_borne(client):
    query = """  with total_sessions_per_station as (
    select id_pdc, sum(session_duration) as total_sum_duration_per_status from (
    Select id_pdc, statut_pdc, max(duration) as session_duration from (
    SELECT id_pdc, statut_pdc , timestamp, last_updated, timestamp_diff(timestamp, last_updated, second) 
    as duration  FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record` WHERE id_pdc != 'null' 
    ORDER BY id_pdc, timestamp DESC) 
    group by last_updated, statut_pdc, id_pdc
    order by id_pdc , last_updated desc ) 
    group by id_pdc
    order by id_pdc
    ) ,
    
    total_exploitaion_duration_per_station as (
    select id_pdc, sum(session_duration) as total_exploitaion_duration from (
    Select id_pdc, statut_pdc, max(duration) as session_duration from (
    SELECT id_pdc, statut_pdc , timestamp, last_updated, timestamp_diff(timestamp, last_updated, second) 
    as duration  FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record` WHERE id_pdc != 'null' 
    and statut_pdc='Occupé (en charge)' ORDER BY id_pdc, timestamp DESC) 
    group by last_updated, statut_pdc, id_pdc
    order by id_pdc , last_updated desc ) 
    group by id_pdc
    order by id_pdc
    )
    
    select id_pdc , total_sum_duration_per_status, total_exploitaion_duration, 
    total_exploitaion_duration/total_sum_duration_per_status *100 as rate from (
    select t.id_pdc, total_sum_duration_per_status, total_exploitaion_duration from 
    total_sessions_per_station as t inner join total_exploitaion_duration_per_station as e on t.id_pdc = e.id_pdc) 
    order by rate asc

  """
    query_job = client.query(query)
    result = query_job.result()
    rows = [dict(row) for row in result]
    df = pd.DataFrame(rows)
    exploitation_rate = df['total_exploitaion_duration'] / df['total_sum_duration_per_status'] * 100
    df['expoloitation_rate'] = exploitation_rate
    return df


def taux_utilisation_par_station(client):
    query = """   with total_sessions_per_station as (
    select id_pdc, sum(session_duration) as total_sum_duration_per_status from (
    Select id_pdc, statut_pdc, max(duration) as session_duration from (
    SELECT id_pdc, statut_pdc , timestamp, last_updated, timestamp_diff(timestamp, last_updated, second) 
    as duration  FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record` WHERE id_pdc != 'null' 
    ORDER BY id_pdc, timestamp DESC) 
    group by last_updated, statut_pdc, id_pdc
    order by id_pdc , last_updated desc ) 
    group by id_pdc
    order by id_pdc
    ) ,
    
    total_exploitaion_duration_per_station as (
    select id_pdc, sum(session_duration) as total_exploitaion_duration from (
    Select id_pdc, statut_pdc, max(duration) as session_duration from (
    SELECT id_pdc, statut_pdc , timestamp, last_updated, timestamp_diff(timestamp, last_updated, second) 
    as duration  FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record` WHERE id_pdc != 'null' 
    and statut_pdc='Occupé (en charge)' ORDER BY id_pdc, timestamp DESC) 
    group by last_updated, statut_pdc, id_pdc
    order by id_pdc , last_updated desc ) 
    group by id_pdc
    order by id_pdc
    ), 

    bornes_par_station as ( select distinct(id_pdc_local), ID_Station, Adresse_station
    from `acn-gcp-octo-hapi.ev_stations_datawarehouse.bornes`
    where ID_PDC_local !='null')
    
    select ID_Station, exploitation_duration/sessions_duration * 100 as rate from (
    select ID_Station, sum (total_sum_duration_per_status) as sessions_duration,
    sum(total_exploitaion_duration )  as exploitation_duration
    from (
    select r.id_pdc, Adresse_station, ID_Station, total_sum_duration_per_status, 
    total_exploitaion_duration from bornes_par_station as b inner join (
    select t.id_pdc, total_sum_duration_per_status, total_exploitaion_duration
    from total_sessions_per_station as t inner join total_exploitaion_duration_per_station as 
    e on t.id_pdc = e.id_pdc ) as r 
    on r.id_pdc = b.id_pdc_local)
    group by ID_Station)
    order by rate desc """

    query_job = client.query(query)
    result = query_job.result()
    rows = [dict(row) for row in result]
    df = pd.DataFrame(rows)
    return df


def stations_plus_populaires(client):
    taux_utili_par_station = taux_utilisation_par_station(client)
    top_stations_plus_populaires = taux_utili_par_station[0:10]
    return top_stations_plus_populaires


def stations_moins_populaires(client):
    taux_utili_par_station = taux_utilisation_par_station(client)
    top_stations_moins_populaires = taux_utili_par_station.iloc[-10:]
    return top_stations_moins_populaires

bq_client = connect_to_bq()
df = pmr_2roues_filter_stations(bq_client,'Disponible', True, False)
print(df)
# taux_utilisation_par_borne(bq_client)
# taux_occupation_par_borne(bq_client, "FR*V75*EPX01*05*1")
# bornes_en_maintenance(bq_client)
# bornes_les_plus_en_panne(bq_client)

