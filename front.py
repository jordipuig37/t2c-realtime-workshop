import streamlit as st
import snowflake.connector
import folium
from streamlit_folium import folium_static
import pandas as pd
import numpy as np
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from folium.plugins import MarkerCluster

from python_scripts.bike_distribution import (
    color_gradient,
    calculate_entropy,
    create_distance_matrix,
    plan_continuous_route
)

st. set_page_config(layout="wide")

# def get_broken_dockers():
#     with snowflake.connector.connect(**st.secrets["snowflake"]) as my_cnx:
#         with my_cnx.cursor() as my_cur:
#             my_cur.execute("SELECT * FROM V_BROKEN_DOCKERS WHERE BROKEN_DOCKS > 1 LIMIT 10")
#             # meta = my_cur.description
#             data =  my_cur.fetchall()
#             return pd.DataFrame(data, columns=list(map(lambda x: x[0], my_cur.description)))

def get_broken_dockers():  # [ ] change back to normal
    return pd.read_csv("brokendocks.csv")


def build_complete_map(broken_docks):
    map = folium.Map(location=(41.3933173, 2.1812483),
                zoom_start=12, tiles="openstreetmap")

    # Add a marker to the map at the specified location
    marker_cluster = MarkerCluster().add_to(map)
    for _, station in broken_docks.iterrows():
        tooltip_text = f"""
            <b>{station['STATION_NAME']}</b><br>
            Broken Dockers: {station['BROKEN_DOCKS']}
        """
        # f"{station['STATION_NAME']} \n Broken Dockers: {station['BROKEN_DOCKS']}"
        folium.Marker(
            [station['LAT'], station['LON']],
            popup=folium.Popup(tooltip_text, max_width=100)
        ).add_to(marker_cluster)
    return map


def distribution_page():
        
    st.title("Streamlit Example: Bike Redistribution")
    st.write("This visualization helps plan the redistribution of bikes to ensure all stations are optimally stocked.")

    bus_capacity = 20

    broken_docks = get_broken_dockers()
    mean_broken_docks = broken_docks['BROKEN_DOCKS'].mean()
    std_dev_broken_docks = broken_docks['BROKEN_DOCKS'].std()

    m = folium.Map(location=[41.3933173, 2.1812483], zoom_start=12, tiles='openstreetmap')
    for id, station in broken_docks.iterrows():
        color = color_gradient(station['BROKEN_DOCKS'], mean_broken_docks, std_dev_broken_docks)
        folium.Circle(location=[station["LAT"], station["LON"]], radius=60, color=color, fill_color=color, tooltip=f"{station['STATION_NAME']}").add_to(m)

    route, updated_stations = plan_continuous_route(broken_docks, bus_capacity)
    steps_description = "Bike Redistribution Plan:\n"
    for start, end, amount, action, bus_load, start_bikes, end_bikes in route:
        start_lat_lon = broken_docks[broken_docks['STATION_NAME'] == start][['LAT', 'LON']].values[0]
        end_lat_lon = broken_docks[broken_docks['STATION_NAME'] == end][['LAT', 'LON']].values[0]
        folium.PolyLine([start_lat_lon, end_lat_lon], color="black", weight=2.5, opacity=0.8).add_to(m)
        steps_description += f"Bus moved from {start} to {end}, {action}. Bus now carries {bus_load} bikes. {start} had {start_bikes} bikes, now has {end_bikes} bikes.\n"

    st.text(steps_description)

    # Display the original map
    st.subheader("Original Distribution")
    folium_static(m)


def broken_docks_page():
    st.title("Broken Docks")
    col1, col2 = st.columns([2, 1])

    broken_docks = get_broken_dockers()
    map = build_complete_map(broken_docks)

    with col1:
        st.subheader("City Bike Stations Map")
        folium_static(map, height=400)

    with col2:  # Display metadata in the second column
        st.subheader("Station List")
        st.dataframe(broken_docks[["STATION_NAME", "BROKEN_DOCKS"]], hide_index=True)
        # for _, station in broken_docks.iterrows():
        #     st.write(f"**{station['STATION_NAME']}**: Broken Docks: {station['BROKEN_DOCKS']}")
            # st.write(f"More metadatas: {station['BROKEN_DOCKS']}")


def main():
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.radio("Go to", ["Broken Docks", "Bike Distribution"])

    if app_mode == "Broken Docks":
        broken_docks_page()
    elif app_mode == "Bike Distribution":
        distribution_page()


if __name__ == "__main__":
    main()
