import streamlit as st
import snowflake.connector
import folium
from streamlit_folium import folium_static
import pandas as pd
import numpy as np
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp


def get_broken_dockers(my_cnx):
    with my_cnx.cursor() as my_cur:
        my_cur.execute("SELECT * FROM V_BROKEN_DOCKERS WHERE BROKEN_DOCKS > 1 LIMIT 20")
        data = my_cur.fetchall()
        return pd.DataFrame(data, columns=[x[0] for x in my_cur.description])

def color_gradient(value, mean_value, std_dev):
    if abs(value - mean_value) <= 0.5 * std_dev:
        return "#0BDA51"  # Green if within 1 std of the average
    elif value < mean_value:
        ratio = (mean_value - value) / (3 * std_dev)
        blue = int(255 * ratio)
        return f"#{0:02x}{0:02x}{blue:02x}"  # Bluer the more below the average
    else:
        ratio = (value - mean_value) / (3 * std_dev)
        red = int(255 * ratio)
        return f"#{red:02x}{0:02x}{0:02x}"  # Redder the more above the average

def calculate_entropy(broken_docks, mean_broken_docks):
    max_capacity = 2 * mean_broken_docks
    probabilities = broken_docks / max_capacity
    entropy = -np.sum([p * np.log2(p) for p in probabilities if p > 0])
    return entropy

def create_distance_matrix(stations):
    num_stations = len(stations)
    distance_matrix = np.zeros((num_stations, num_stations))
    
    stations['LAT'] = stations['LAT'].astype(float)
    stations['LON'] = stations['LON'].astype(float)
    
    for i in range(num_stations):
        for j in range(num_stations):
            if i != j:
                station1 = stations.iloc[i]
                station2 = stations.iloc[j]
                distance_matrix[i][j] = ((station1['LAT'] - station2['LAT'])**2 + (station1['LON'] - station2['LON'])**2)**0.5
    
    return distance_matrix

def plan_continuous_route(stations, bus_capacity, threshold=0.1, entropy_weight=1.0, distance_weight=1.0):
    num_stations = len(stations)
    target = stations['BROKEN_DOCKS'].mean()
    stations['TARGET'] = target
    stations['NEED'] = stations['TARGET'] - stations['BROKEN_DOCKS']
    
    # Filter out stations that are already "well"
    stations_to_visit = stations[abs(stations['NEED']) > threshold * target].reset_index(drop=True)
    num_stations_to_visit = len(stations_to_visit)
    
    if num_stations_to_visit == 0:
        st.write("All stations are already well-balanced.")
        return [], stations
    
    distance_matrix = create_distance_matrix(stations_to_visit)
    
    manager = pywrapcp.RoutingIndexManager(num_stations_to_visit, 1, 0)
    routing = pywrapcp.RoutingModel(manager)
    
    def combined_cost_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        
        current_station = stations_to_visit.iloc[from_node]
        next_station = stations_to_visit.iloc[to_node]
        
        current_station_index = stations.index[stations['STATION_NAME'] == current_station['STATION_NAME']][0]
        next_station_index = stations.index[stations['STATION_NAME'] == next_station['STATION_NAME']][0]
        
        current_entropy = calculate_entropy(stations['BROKEN_DOCKS'], mean_broken_docks)
        
        if current_station['NEED'] < 0:
            amount_to_move = min(bus_capacity - bus_load, abs(current_station['NEED']), stations.at[current_station_index, 'BROKEN_DOCKS'])
            amount_to_move = int(amount_to_move)
            stations.at[current_station_index, 'BROKEN_DOCKS'] -= amount_to_move
            stations.at[next_station_index, 'BROKEN_DOCKS'] += amount_to_move
        else:
            amount_to_move = min(bus_load, current_station['NEED'], bus_capacity - stations.at[next_station_index, 'BROKEN_DOCKS'])
            amount_to_move = int(amount_to_move)
            stations.at[current_station_index, 'BROKEN_DOCKS'] += amount_to_move
            stations.at[next_station_index, 'BROKEN_DOCKS'] -= amount_to_move
        
        next_entropy = calculate_entropy(stations['BROKEN_DOCKS'], mean_broken_docks)
        entropy_diff = next_entropy - current_entropy
        
        distance = distance_matrix[from_node][to_node]
        
        combined_cost = -entropy_weight * entropy_diff + distance_weight * distance
        return combined_cost
    
    combined_cost_callback_index = routing.RegisterTransitCallback(combined_cost_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(combined_cost_callback_index)
    
    dimension_name = 'Capacity'
    routing.AddDimension(
        combined_cost_callback_index,
        0,  # no slack
        bus_capacity,  # vehicle maximum capacity
        True,  # start cumul to zero
        dimension_name)
    capacity_dimension = routing.GetDimensionOrDie(dimension_name)
    
    for node in range(1, num_stations_to_visit):
        index = manager.NodeToIndex(node)
        if stations_to_visit.iloc[node]['NEED'] > 0:
            capacity_dimension.CumulVar(index).SetMax(bus_capacity)
        else:
            capacity_dimension.CumulVar(index).SetMin(0)
    
    for node in range(num_stations_to_visit):
        routing.AddDisjunction([manager.NodeToIndex(node)], 0)
    
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
    search_parameters.local_search_metaheuristic = (routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.time_limit.seconds = 30
    
    assignment = routing.SolveWithParameters(search_parameters)
    
    if assignment:
        route = []
        index = routing.Start(0)
        bus_load = 0
        visited_stations = set()
        
        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            next_index = assignment.Value(routing.NextVar(index))
            next_node = manager.IndexToNode(next_index)
            
            current_station = stations_to_visit.iloc[node]
            next_station = stations_to_visit.iloc[next_node]
            
            if current_station['STATION_NAME'] in visited_stations:
                index = next_index
                continue
            
            visited_stations.add(current_station['STATION_NAME'])
            
            current_station_index = stations.index[stations['STATION_NAME'] == current_station['STATION_NAME']][0]
            next_station_index = stations.index[stations['STATION_NAME'] == next_station['STATION_NAME']][0]
            
            if current_station['NEED'] < 0:
                amount_to_move = min(bus_capacity - bus_load, abs(current_station['NEED']), stations.at[current_station_index, 'BROKEN_DOCKS'])
                amount_to_move = int(amount_to_move)
                bus_load += amount_to_move
                action = f"Picked up {amount_to_move} bikes"
            else:
                amount_to_move = min(bus_load, current_station['NEED'], bus_capacity - stations.at[next_station_index, 'BROKEN_DOCKS'])
                amount_to_move = int(amount_to_move)
                bus_load -= amount_to_move
                action = f"Dropped off {amount_to_move} bikes"
            
            stations.at[current_station_index, 'BROKEN_DOCKS'] += amount_to_move if current_station['NEED'] > 0 else -amount_to_move
            
            route.append((current_station['STATION_NAME'], next_station['STATION_NAME'], amount_to_move, action, bus_load, current_station['BROKEN_DOCKS'], stations.at[current_station_index, 'BROKEN_DOCKS']))
            
            index = next_index
        
        return route, stations
    else:
        st.write("No solution found.")
        return [], stations

st.title("Streamlit Example: Bike Redistribution")
st.write("This visualization helps plan the redistribution of bikes to ensure all stations are optimally stocked.")

bus_capacity = st.slider("Bus Capacity", min_value=1, max_value=100, value=20, step=1)

with snowflake.connector.connect(**st.secrets["snowflake"]) as my_cnx:
    broken_docks = get_broken_dockers(my_cnx)
    mean_broken_docks = broken_docks['BROKEN_DOCKS'].mean()
    std_dev_broken_docks = broken_docks['BROKEN_DOCKS'].std()
    initial_entropy = calculate_entropy(broken_docks['BROKEN_DOCKS'], mean_broken_docks)

    m = folium.Map(location=[41.3933173, 2.1812483], zoom_start=12, tiles='openstreetmap')
    for id, station in broken_docks.iterrows():
        color = color_gradient(station['BROKEN_DOCKS'], mean_broken_docks, std_dev_broken_docks)
        folium.Circle(location=[station["LAT"], station["LON"]], radius=60, color=color, fill_color=color, tooltip=f"{station['STATION_NAME']} - Broken Docks: {station['BROKEN_DOCKS']}").add_to(m)

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

    # Create and display a map of the post-route distribution
    m_updated = folium.Map(location=[41.3933173, 2.1812483], zoom_start=12, tiles='openstreetmap')
    for id, station in updated_stations.iterrows():
        color = color_gradient(station['BROKEN_DOCKS'], mean_broken_docks, std_dev_broken_docks)
        folium.Circle(location=[station["LAT"], station["LON"]], radius=60, color=color, fill_color=color, tooltip=f"{station['STATION_NAME']} - Broken Docks: {station['BROKEN_DOCKS']}").add_to(m_updated)
    
    st.subheader("Updated Distribution")
    folium_static(m_updated)

    final_entropy = calculate_entropy(updated_stations['BROKEN_DOCKS'], mean_broken_docks)
    st.write(f"Initial Entropy: {initial_entropy:.4f}")
    st.write(f"Final Entropy: {final_entropy:.4f}")

    print("Original stations:")
    print(broken_docks)
    print()
    print("Updated stations:")
    print(updated_stations)
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
