# Import python packages
import streamlit as st
import snowflake.connector
import folium
from streamlit_folium import folium_static
import pandas as pd


def get_broken_dockers(my_cnx):
    with my_cnx.cursor() as my_cur:
        my_cur.execute("SELECT * FROM V_BROKEN_DOCKERS WHERE BROKEN_DOCKS > 1 LIMIT 10")
        meta = my_cur.description
        data =  my_cur.fetchall()
        return pd.DataFrame(data, columns=list(map(lambda x: x[0], my_cur.description)))


def broken_docks_page():
    # Write directly to the app
    st.title("Broken Docks")


    with snowflake.connector.connect(**st.secrets["snowflake"]) as my_cnx:
        broken_docks = get_broken_dockers(my_cnx)

        m = folium.Map(location=(41.3933173, 2.1812483),
                    zoom_start=12, tiles="openstreetmap")

        # Add a marker to the map at the specified location
        for id, station in broken_docks.iterrows():
            folium.Circle(
                location=[station["LAT"], station["LON"]],
                radius=30,
                color="red",
                fill_color="red",
                tooltip=station["STATION_NAME"]
            ).add_to(m)

        # Display the map
        folium_static(m)


def screen_two():
    st.title("Screen Two")
    st.write("This is the second screen.")


def main():
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.radio("Go to", ["Screen One", "Screen Two"])

    if app_mode == "Screen One":
        broken_docks_page()
    elif app_mode == "Screen Two":
        screen_two()

if __name__ == "__main__":
    main()