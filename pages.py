import functions
import streamlit as st
import matplotlib.pyplot as plt
import altair as alt
import pandas as pd


def page_conducteur():
    status = st.selectbox(
         'Etat actuel des bornes',
         ('Toutes', 'Disponibles', 'En charge', 'En maintenance'), key='conducteur')

    # st.set_page_config(layout="wide")
    filtrer = functions.status_choice(status)
    pmr = st.checkbox('Afficher uniquement les points de charge accessibles PMR')
    deux_roues = st.checkbox('Afficher uniquement les points de charge 2 roues')

    c1, c2 = st.columns((3, 1))
    with c1:
        # number = map_with_filter(client, option)
        coord_geo = functions.pmr_2roues_filter_stations(bq_client, filtrer, pmr, deux_roues)
        if not coord_geo.empty:

            number = coord_geo.shape[0]
            st.map(coord_geo)
        else:
            st.subheader("Oupsii pas de bornes avec vos critères actuellement ! ")

    with c2:
        if not coord_geo.empty:
            c2.metric("Nombre de bornes ", number)


def status_real_time_visualization():
    option = st.selectbox(
         'Etat actuel des bornes',
         ('Toutes', 'Disponibles', 'En charge', 'En maintenance'), key='rt_viz')

    # st.set_page_config(layout="wide")
    option = functions.status_choice(option)
    c1, c2 = st.columns((3, 1))
    with c1:
        # number = map_with_filter(client, option)
        coord_geo = functions.filtrer_stations_selon_status(bq_client, option)
        number = coord_geo.shape[0]
        st.map(coord_geo)
        # taux_occupation_par_borne(client, "FR*V75*E9001*02*1")
    with c2:
        c2.metric("Nombres des bornes", number)
        if option != "":
            c2.metric("Pourcentage", round(number*100/2093), "%")


def plot_repartition_temps(bq_client, id_borne):
    status_repartition = functions.taux_occupation_par_borne(bq_client, id_borne)

    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = status_repartition.index
    print(labels)
    duree = status_repartition['duration']
    colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99']
    fig1, ax1 = plt.subplots()
    ax1.pie(duree, labels=labels, autopct='%1.1f%%',
            shadow=False, startangle=90, colors=colors, pctdistance=0.85)
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig1 = plt.gcf()
    fig1.gca().add_artist(centre_circle)
    plt.tight_layout()
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    st.pyplot(fig1)
    # st.write('The current movie title is', title)


def page_caracteristiques_par_borne(bq_client):
    id_borne = st.text_input(label='Entrer ID Borne', value="FR*V75*E9001*02*1")
    st.title('Caractéristiques de la borne ', id_borne)
    plot_repartition_temps(bq_client, id_borne)
    functions.taux_occupation_par_borne(bq_client, id_borne)


def page_maintenance():
    # functions.map_with_filter(bq_client, "En maintenance")
    st.title("Liste des bornes en maintenance")
    bornes_en_panne = functions.liste_bornes(bq_client)
    st.dataframe(bornes_en_panne)
    st.title("Les 10 bornes les plus en panne")
    st.write("""Ce classement est fait sur la base du temps d'arrêt total de chaque borne """)
    top_down_time = functions.bornes_les_plus_en_panne(bq_client)
    top_down_time = pd.DataFrame(top_down_time)
    top_down_time.rename(columns={'id_pdc': 'ID Borne', 'total_down_time': 'Temps d arrêt'}, inplace=True)
    print(top_down_time)
    chart = alt.Chart(top_down_time).mark_bar().encode(
    alt.X('ID Borne', sort=alt.EncodingSortField(field="Temps d arrêt", op="count", order='ascending')),
    alt.Y('Temps d arrêt')
    )
    st.altair_chart(chart, use_container_width=True)


def plot_top_ten_stations():
    top_ten = functions.stations_plus_populaires(bq_client)
    top_ten = pd.DataFrame(top_ten)
    print(top_ten)
    top_ten.rename(columns={'ID_Station': 'ID Station', 'rate': 'Taux d utilisation'}, inplace=True)

    chart = alt.Chart(top_ten).mark_bar().encode(
        alt.X('ID Station', sort=alt.EncodingSortField(field="Taux d utilisation", op="count", order='descending')),
        alt.Y('Taux d utilisation')
    )
    st.altair_chart(chart, use_container_width=True)


def plot_worst_ten_stations():
    top_ten = functions.stations_moins_populaires(bq_client)
    top_ten = pd.DataFrame(top_ten)
    print(top_ten)
    top_ten.rename(columns={'ID_Station': 'ID Station', 'rate': 'Taux d utilisation'}, inplace=True)

    chart = alt.Chart(top_ten).mark_bar().encode(
        alt.X('ID Station', sort=alt.EncodingSortField(field="Taux d utilisation", op="count", order='ascending')),
        alt.Y('Taux d utilisation')
    )
    st.altair_chart(chart, use_container_width=True)


def get_bar_chart(source, x, y):
    hover = alt.selection_single(
        fields=[x],
        nearest=True,
        on="mouseover",
        empty="none",
    )


    # Draw a rule at the location of the selection
    tooltips = (
        alt.Chart(source)
        .mark_rule()
        .encode(
            x=x,
            y=y,
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip(x, title=x),
                alt.Tooltip(y, title=y),
            ],
        )
        .add_selection(hover)
    )

    return (tooltips).interactive()

def get_chart(data):
    hover = alt.selection_single(
        fields=["Date"],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    lines = (
        alt.Chart(data, title="Taux d'occupation journalier")
        .mark_line()
        .encode(
            x="Date",
            y="Taux",
            # color="symbol",
            # strokeDash="symbol",
        )
    )

    # Draw points on the line, and highlight based on selection
    points = lines.transform_filter(hover).mark_circle(size=65)

    # Draw a rule at the location of the selection
    tooltips = (
        alt.Chart(data)
        .mark_rule()
        .encode(
            x="Date",
            y="Taux",
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip("Date", title="Date"),
                alt.Tooltip("Taux", title="Taux d'utilisation"),
            ],
        )
        .add_selection(hover)
    )

    return (lines + points + tooltips).interactive()


def page_provider():

    status_real_time_visualization()
    # st.title(" 📊️ Bélib Insights")

    c1, c2 = st.columns((1, 1))
    with c1:
        st.subheader("Les Stations les plus populaires")
        st.write(" Top 10 des stations en termes de taux d'utilisation")
        plot_top_ten_stations()
    with c2:
        st.subheader("Les Stations les moins utilisées")
        st.write(" Top 10 des stations les moins utilisées")
        plot_worst_ten_stations()


    st.subheader('Focus par borne')
    id_borne = st.text_input(label='Entrer ID Borne', value="FR*V75*E9001*02*1")
    st.subheader('Caractéristiques de la borne ', id_borne)
    st.write("Taux d'utilisation journalier de la borne", id_borne)
    data = functions.taux_occupation_journalier(bq_client, id_borne)
    if not data.empty:
        data = pd.DataFrame(data, columns=["Date", "Taux"])
        chart = get_chart(data)
        st.altair_chart(chart, use_container_width=True)
        st.subheader('Répartition du temps global')
        plot_repartition_temps(bq_client, id_borne)
    else:
        st.subheader("Oupssii ID invalide ! ")
    # functions.taux_occupation_par_borne(bq_client, id_borne)


bq_client = functions.connect_to_bq()

page_conducteur()
