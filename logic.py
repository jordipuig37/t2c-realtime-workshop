import pandas as pd
import numpy as np

def get_station_data(my_cnx):
    with my_cnx.cursor() as my_cur:
        # Retrieve data from F_LAST_UPDATED_STATUS
        my_cur.execute("SELECT * FROM F_LAST_UPDATED_STATUS")
        status_data = my_cur.fetchall()
        status_columns = [x[0] for x in my_cur.description]

        # Retrieve LAT and LON from V_BROKEN_DOCKERS
        my_cur.execute("SELECT STATION_ID, LAT, LON FROM V_BROKEN_DOCKERS")
        docker_data = my_cur.fetchall()
        docker_columns = [x[0] for x in my_cur.description]

        # Combine the data based on STATION_ID
        combined_data = []
        for status_row in status_data:
            station_id = status_row[status_columns.index('STATION_ID')]
            num_bikes_available = status_row[status_columns.index('NUM_BIKES_AVAILABLE')]
            num_docks_available = status_row[status_columns.index('NUM_DOCKS_AVAILABLE')]

            # Skip rows where the sum of NUM_BIKES_AVAILABLE and NUM_DOCKS_AVAILABLE is zero
            if num_bikes_available + num_docks_available == 0:
                continue

            docker_row = next((row for row in docker_data if row[docker_columns.index('STATION_ID')] == station_id), None)
            if docker_row:
                lat = docker_row[docker_columns.index('LAT')]
                lon = docker_row[docker_columns.index('LON')]
                combined_row = status_row + (lat, lon)
                combined_data.append(combined_row)

        # Create a DataFrame with the combined data
        columns = status_columns + ['LAT', 'LON']
        
        df = pd.DataFrame(combined_data, columns=columns)
        df.drop(columns=['LAST_REPORTED', 'IS_CHARGING_STATION', 'STATUS', 'IS_INSTALLED', 'IS_RENTING', 'IS_RETURNING', 'TRAFFIC', 'TST_REC', 'NUM_BIKES_AVAILABLE_MECHANICAL', 'NUM_BIKES_AVAILABLE_EBIKE'], inplace=True)
        
        df['TOTAL_CAPACITY'] = df['NUM_BIKES_AVAILABLE'] + df['NUM_DOCKS_AVAILABLE']
        df['RATIO'] = df['NUM_BIKES_AVAILABLE'] / df['TOTAL_CAPACITY']
        
        return df


def color_gradient(ratio, treshold = 0.1):
    
    min_th = 0.5 - treshold
    max_th = 0.5 + treshold
    
    if min_th <= ratio <= max_th:
        return "#00FF00"  # Green
    elif ratio < min_th:
        blue = int(255 * (min_th - ratio) / min_th)
        green = int(255 * ratio / min_th)
        return f"#{0:02x}{green:02x}{blue:02x}"
    else:
        red = int(255 * (ratio - max_th) / min_th)
        green = int(255 * (1 - (ratio - max_th) / min_th))
        return f"#{red:02x}{green:02x}{0:02x}"

def calculate_entropy(ratio):
    if ratio == 0 or ratio == 1:
        return 0
    return -ratio * np.log2(ratio) - (1 - ratio) * np.log2(1 - ratio)

def create_distance_matrix(stations):
    num_stations = len(stations)
    distance_matrix = np.zeros((num_stations, num_stations))
    
    for i in range(num_stations):
        for j in range(num_stations):
            if i != j:
                station1 = stations.iloc[i]
                station2 = stations.iloc[j]
                lat1 = float(stations.loc[station1.name, 'LAT'])
                lon1 = float(stations.loc[station1.name, 'LON'])
                lat2 = float(stations.loc[station2.name, 'LAT'])
                lon2 = float(stations.loc[station2.name, 'LON'])
                distance_matrix[i][j] = ((lat1 - lat2)**2 + (lon1 - lon2)**2)**0.5
    
    return distance_matrix
    


def plan_continuous_route(in_stations, bus_capacity, th_high=0.0, th_low=0.1):
    stations = in_stations.copy()
    num_stations = len(stations)

    stations['NEED'] = (stations['TOTAL_CAPACITY'] * 0.45 - stations['NUM_BIKES_AVAILABLE']).round()

    # Filter out stations that are already "well-balanced"
    stations_to_visit = stations[(stations['RATIO'] > th_high) | (stations['RATIO'] < th_low)].reset_index(drop=True)
    num_stations_to_visit = len(stations_to_visit)

    if num_stations_to_visit == 0:
        print("All stations are already well-balanced.")
        return [], stations

    # Initialize variables
    route = []
    bus_load = 0

    # Randomly choose the initial station with negative need
    np.random.seed(42)
    
    negative_need_stations = stations_to_visit[stations_to_visit['NEED'] < 0]
    if not negative_need_stations.empty:
        current_station_index = np.random.choice(negative_need_stations.index)
    else:
        current_station_index = np.random.choice(stations_to_visit.index)
    current_station = stations_to_visit.loc[current_station_index]
    
    stations_to_visit = stations_to_visit.drop(current_station_index).reset_index(drop=True)
    num_stations_to_visit -= 1

    while num_stations_to_visit > 0:
        
        # Update the bus load and station bike counts based on the action
        if current_station['NEED'] < 0:
            # Pick up bikes from the current station
            amount_to_move = min(bus_capacity - bus_load, abs(current_station['NEED']))
            amount_to_move = min(amount_to_move, current_station['NUM_BIKES_AVAILABLE'])
            
            # amount_to_move = round(amount_to_move * 1.2)
            
            bus_load += amount_to_move
            current_station['NUM_BIKES_AVAILABLE'] -= amount_to_move
            current_station['NUM_DOCKS_AVAILABLE'] += amount_to_move
            action = f"Picked up {int(amount_to_move)} bikes"
        else:
            # Drop off bikes at the current station
            amount_to_move = min(bus_load, current_station['NEED'])
            amount_to_move = min(amount_to_move, current_station['NUM_DOCKS_AVAILABLE'])
            amount_to_move = round(amount_to_move / 1.7)
            
            bus_load -= amount_to_move
            current_station['NUM_BIKES_AVAILABLE'] += amount_to_move
            current_station['NUM_DOCKS_AVAILABLE'] -= amount_to_move
            action = f"Dropped off {int(amount_to_move)} bikes"

        # Update the ratio and need for the current station
        current_station['RATIO'] = current_station['NUM_BIKES_AVAILABLE'] / current_station['TOTAL_CAPACITY']
        current_station['NEED'] = round(current_station['TOTAL_CAPACITY'] * 0.5 - current_station['NUM_BIKES_AVAILABLE'])
        
        # Update the corresponding station in the stations DataFrame
        stations.loc[stations['STATION_ID'] == current_station['STATION_ID'], 'NUM_BIKES_AVAILABLE'] = current_station['NUM_BIKES_AVAILABLE']
        stations.loc[stations['STATION_ID'] == current_station['STATION_ID'], 'NUM_DOCKS_AVAILABLE'] = current_station['NUM_DOCKS_AVAILABLE']
        stations.loc[stations['STATION_ID'] == current_station['STATION_ID'], 'RATIO'] = current_station['RATIO']
        stations.loc[stations['STATION_ID'] == current_station['STATION_ID'], 'NEED'] = current_station['NEED']
        
        if current_station['RATIO'] < 0 or current_station['RATIO'] > 1:
            print(f"invalid ratio, num bikes: {current_station['NUM_BIKES_AVAILABLE']}, num docks: {current_station['NUM_DOCKS_AVAILABLE']}, total capacity: {current_station['TOTAL_CAPACITY']}")
        
        
        # Find the nearest unvisited station to the current station
        distances = np.sqrt((stations_to_visit['LAT'] - current_station['LAT'])**2 +
                            (stations_to_visit['LON'] - current_station['LON'])**2)
        nearest_station_index = distances.idxmin()
        nearest_station = stations_to_visit.loc[nearest_station_index]

        
        # Add the current move to the route
        route.append((current_station['STATION_ID'], nearest_station['STATION_ID'], int(amount_to_move), action))

        # Move to the nearest station
        current_station = nearest_station
        stations_to_visit = stations_to_visit.drop(nearest_station_index).reset_index(drop=True)
        num_stations_to_visit -= 1

    return route, stations