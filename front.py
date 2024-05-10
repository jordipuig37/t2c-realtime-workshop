import streamlit as st
import folium
from streamlit_folium import folium_static
import pandas as pd
from folium.plugins import MarkerCluster
import matplotlib.pyplot as plt

from logic import (
    get_station_data,
    color_gradient,
    calculate_entropy,
    plan_continuous_route,
    my_icon_create_function,
    get_broken_dockers
)

st. set_page_config(page_title="T2C Real Time Workshop", layout="wide")


def build_broken_docks_map(broken_docks):
    map = folium.Map(location=(41.3933173, 2.1812483),
                zoom_start=12, tiles="openstreetmap")

    for _, station in broken_docks.iterrows():
        tooltip_text = f"""
            <b>{station['STATION_NAME']}</b><br>
            Broken Dockers: {station['BROKEN_DOCKS']}
        """
        # f"{station['STATION_NAME']} \n Broken Dockers: {station['BROKEN_DOCKS']}"
        folium.Marker(
            [station['LAT'], station['LON']],
            popup=folium.Popup(tooltip_text, max_width=100)
        ).add_to(map)
    return map


def distribution_page():
    # ----------------- BASIC STRUCTURE -----------------

    st.title("Bike Redistribution")
    st.write("This visualization helps plan the redistribution of bikes to ensure all stations are optimally stocked.")

    bus_capacity = 20
    color_treshold = 0.1
        
    # ----------------- DATA -----------------
    station_data = get_station_data()
    route, updated_stations = plan_continuous_route(station_data, bus_capacity)

    
    # ----------------- TODO: Calculate distances properly, otherwise route looks chaotic (you can try uncomment) -----------------
    # Load the routes from the file during inference
    # with open("nearby_routes.json", "r") as file:
    #     loaded_routes = json.load(file)
    #     all_routes = {eval(k): v for k, v in loaded_routes.items()}
    steps_description = "Bike Redistribution Plan:\n"
    
    
    # ----------------- MAPS -----------------
    # -------- Map pre + route --------
    m = folium.Map(location=[41.3933173, 2.1812483], zoom_start=12, tiles='openstreetmap')
    for _, station in station_data.iterrows():
        color = color_gradient(station['RATIO'])
        folium.Circle(
            location=[station["LAT"], station["LON"]],
            radius=60,
            color=color,
            fill_color=color,
            tooltip=f"Station ID: {station['STATION_NAME']} - Bikes Available: {station['NUM_BIKES_AVAILABLE']}").add_to(m)
        
    for start, end, amount, action in route:        
        start_lat_lon = station_data[station_data['STATION_ID'] == start][['LAT', 'LON']].values[0]
        end_lat_lon = station_data[station_data['STATION_ID'] == end][['LAT', 'LON']].values[0]
        folium.PolyLine(
            [start_lat_lon, end_lat_lon],
            color="black",
            weight=2.5,
            opacity=0.8
        ).add_to(m)
        steps_description += f"Bus {action}, then moved from Station {start} to Station {end}.\n"

    # -------- Map post rute --------
    m_updated = folium.Map(location=[41.3933173, 2.1812483], zoom_start=12, tiles='openstreetmap')
    marker_cluster = MarkerCluster(icon_create_function=my_icon_create_function, options={"disableClusteringAtZoom":14}).add_to(m_updated)
    for _, station in updated_stations.iterrows():
        color = color_gradient(station['NUM_BIKES_AVAILABLE'] / station['TOTAL_CAPACITY'], treshold=color_treshold)
        folium.Circle(
            location=[station["LAT"], station["LON"]],
            radius=60,
            color=color,
            fill_color=color,
            weight = station['NUM_BIKES_AVAILABLE'] / station['TOTAL_CAPACITY']+2,
            tooltip=f"Station ID: {station['STATION_ID']} - Bikes Available: {station['NUM_BIKES_AVAILABLE']}"
        ).add_to(marker_cluster)

    # ----------------- HISTOGRAMS -----------------
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Original Distribution")
        folium_static(m, height=400)

        fig1, ax1 = plt.subplots()
        ax1.hist(station_data['RATIO'], bins=40)
        ax1.set_xlabel("Ratio")
        ax1.set_ylabel("Frequency")
        ax1.set_title("Original Distribution Histogram")
        ax1.set_xlim(0, 1)
        st.pyplot(fig1)
    with col2:
        st.subheader("Updated Distribution")
        folium_static(m_updated, height=400)

        fig2, ax2 = plt.subplots()
        ax2.hist(updated_stations['NUM_BIKES_AVAILABLE'] / updated_stations['TOTAL_CAPACITY'], bins=40)
        ax2.set_xlabel("Ratio")
        ax2.set_ylabel("Frequency")
        ax2.set_title("Updated Distribution Histogram")
        ax2.set_xlim(0, 1)
        st.pyplot(fig2)


    # ----------------- ENTROPY (really useful for us; star metric) -----------------
    initial_entropy = station_data['RATIO'].apply(calculate_entropy).sum()
    final_entropy = updated_stations['NUM_BIKES_AVAILABLE'].divide(updated_stations['TOTAL_CAPACITY']).apply(calculate_entropy).sum()
    
    st.write(f"Initial Entropy: {initial_entropy:.4f}, Final Entropy: {final_entropy:.4f}, Entropy Gain: {final_entropy - initial_entropy:.4f}")

    # ----------------- ROUTE DETAILS (can be cleaned and presented more nicely; used for debugging so far) -----------------
    with st.expander("Route Details"):
        st.write("Updated stations:")
        st.write(updated_stations)

        st.write("Route:")
        st.write(route)

        st.text(steps_description)


def broken_docks_page():
    st.title("Broken Docks")
    col1, col2 = st.columns([2, 1])
    broken_docks = get_broken_dockers()
    map = build_broken_docks_map(broken_docks)
    with col1:
        st.subheader("City Bike Stations Map")
        folium_static(map, height=400)

    with col2:  # Display metadata in the second column
        st.subheader("Station List")
        st.dataframe(broken_docks[["STATION_NAME", "BROKEN_DOCKS"]])


def main():
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.radio("Go to", ["Bike Distribution", "Broken Docks"])

    if app_mode == "Broken Docks":
        broken_docks_page()
    elif app_mode == "Bike Distribution":
        distribution_page()


if __name__ == "__main__":
    main()
