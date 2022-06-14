import streamlit as st
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
from datetime import datetime
import plotly.graph_objects as go


def connect_to_bq():

    # Create API client.
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )

    #client = storage.Client(credentials=credentials)
    client = bigquery.Client(credentials=credentials)
    return client


def stations_par_arronsdissement(client):

    query_job = client.query("""SELECT Code_INSEE_commune, COUNT( DISTINCT ID_Station) FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.bornes` 
WHERE ID_PDC_local IS NOT NULL
GROUP BY Code_INSEE_commune""")
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    df = pd.DataFrame(rows, columns=['Code_INSEE_commune', 'f0_'])

    #df = pd.DataFrame(rows)
    df.columns = ['Arrondissement','Nbr des bornes']

    print(df)
    # Bar Chart
    st.bar_chart(df['Nbr des bornes'])


def pmr(client):
    numbers = []
    query_2 = """ SELECT COUNT( DISTINCT ID_PDC_local) FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.bornes` 
    WHERE ID_PDC_local IS NOT NULL AND Accessibilite_PMR="Réservé PMR" """

    query_1 = """ SELECT COUNT( DISTINCT ID_PDC_local) FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.bornes` 
    WHERE ID_PDC_local IS NOT NULL AND Accessibilite_PMR="Non accessible" """
    query_job_1 = client.query(query_1)
    non_pmr = query_job_1.result()
    non_pmr = [dict(row) for row in non_pmr]
    df = pd.DataFrame(non_pmr)
    non_pmr = df['f0_'].astype(int)
    numbers.append(non_pmr)


    query_job_2 = client.query(query_2)
    nb_pmr = query_job_2.result()
    nb_pmr = [dict(row) for row in nb_pmr]
    df = pd.DataFrame(nb_pmr)

    nb_pmr = df['f0_'].astype(int)
    numbers.append(nb_pmr)

    labels = ['Réservé PMR', 'Non accessible']
    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=numbers,
            hoverinfo="label+percent",
            textinfo="value"
        ))

    st.plotly_chart(fig)


    #print("pourcentage pmr", nb_pmr*100/non_pmr)


def status(client):
    query = """ SELECT id_pdc, statut_pdc, timestamp, last_updated From `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record`WHERE id_pdc="FR*V75*E9004*01*1"
    """
    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    df = pd.DataFrame(rows, columns=['id_pdc', 'statut_pdc', 'timestamp', 'last_updated'])
    print(df)




client = connect_to_bq()
left_column, right_column = st.columns(2)
# You can use a column just like st.sidebar:
left_column.button('Driver')



# Or even better, call Streamlit functions inside a "with" block:
with right_column:

    st.title('Bélib Insights')
    st.subheader('Bornes par arrondissement')

    #stations_par_arronsdissement(client)
    #pmr()
    pmr(client)










