import streamlit as st
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import plotly.express as px
import pandas as pd
#import matplotlib.pyplot as plt

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

#client = storage.Client(credentials=credentials)
client = bigquery.Client(credentials=credentials)
# Retrieve file contents.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.@st.experimental_memo(ttl=600)
def read_file(bucket_name, file_path):
    bucket = client.bucket(bucket_name)
    content = bucket.blob(file_path).download_as_string().decode("utf-8")
    return content

bucket_name = "ev_stations"
file_path = "20220427-162751.csv"

#content = read_file(bucket_name, file_path)

# Print results.
#print(content)


def stations_par_arronsdissement():

    query_job = client.query("""SELECT Code_INSEE_commune, COUNT( DISTINCT ID_Station) FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.bornes` 
WHERE ID_PDC_local IS NOT NULL
GROUP BY Code_INSEE_commune""")
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]

    df = pd.DataFrame(rows)
    print(df)

    st.subheader('Bornes par arrondissement')
    #st.write(weekly_data)
    # Bar Chart
    st.bar_chart(df['f0_'])
    #return rows
    #st.bar_chart(data=df, width=0, height=0, use_container_width=True)

def _fetch_data_bigquery():
    query_job = client.query(
        """ SELECT id_pdc, COUNT(id_pdc) FROM `acn-gcp-octo-hapi.ev_stations_datawarehouse.status_record` 
            GROUP BY id_pdc """
    )

    results = query_job.result()  # Waits for job to complete.
    print(results)
    for row in results:
        st.write(row)
        print(row)
    """
      Take SQL query in Standard SQL and returns a Pandas DataFrame of results
      ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql
    """

st.title('Bélib Insights')
stations_par_arronsdissement()
