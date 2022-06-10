import streamlit as st
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import plotly.express as px

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

    df = pd.DataFrame(rows)
    df.columns = ['Arrondissement' ,'Nbr des bornes']
    #print(df)
    #return(df)
    #st.subheader('Bornes par arrondissement')
    #st.write(weekly_data)
    # Bar Chart
    st.bar_chart(df)
    #st.bar_chart(df['Nbr des bornes'])
    #return rows
    #st.bar_chart(data=df, width=0, height=0, use_container_width=True)


client = connect_to_bq()
left_column, right_column = st.columns(2)
# You can use a column just like st.sidebar:
left_column.button('Driver')



# Or even better, call Streamlit functions inside a "with" block:
with right_column:

    st.title('Bélib Insights')
    st.subheader('Bornes par arrondissement')

    stations_par_arronsdissement(client)
    chosen = st.radio(
        'Sorting hat',
        ("Gryffindor", "Ravenclaw", "Hufflepuff", "Slytherin"))
    st.write(f"You are in {chosen} house!")








