import streamlit as st
import snowflake.connector
import folium
from streamlit_folium import folium_static
import pandas as pd
from folium.plugins import MarkerCluster

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


def screen_two():
    st.title("Screen Two")
    st.write("This is the second screen.")


def main():
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.radio("Go to", ["Broken Docks", "Screen Two"])

    if app_mode == "Broken Docks":
        broken_docks_page()
    elif app_mode == "Screen Two":
        screen_two()

if __name__ == "__main__":
    main()