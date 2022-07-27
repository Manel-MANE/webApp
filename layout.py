# Contents of ~/my_app/streamlit_app.py
import streamlit as st
import pages
import functions
from main import page_insights


client = functions.connect_to_bq()


def conducteur():

    st.markdown("# 🔍 Trouver une station Bélib"")
   
    st.sidebar.markdown( "# 🚗 Vue conducteur")
    pages.page_conducteur()


def insights():
    st.markdown("# 📊️ Vue Aménageur ")
    pages.page_provider()


def maintenance():
    st.markdown("# 🛠️ maintenance ")
    st.sidebar.markdown("# 🛠️ maintenance  ")
    pages.page_maintenance()


def previsions():
    st.markdown("# 🔮 Prévisions ")
    st.sidebar.markdown("# 🔮 Prévisions  ")


page_names_to_funcs = {

    "Vue Aménageur": insights,
    "Vue Maintenance": maintenance,
    "Vue Conducteur": conducteur,
    "Prévisions": previsions
}

selected_page = st.sidebar.selectbox("Sélectionner une page", page_names_to_funcs.keys())
page_names_to_funcs[selected_page]()
