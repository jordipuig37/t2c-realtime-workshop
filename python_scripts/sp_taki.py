# from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import networkx as nx
import numpy as np
import snowflake.connector


def create_data_model(cnx: snowflake.connector.SnowflakeConnection):
    """Stores the data for the problem."""
    data = {}

    # Example data (replace with your actual data)
    data['num_vehicles'] = 1
    data['depot'] = 0
    my_cur = cnx.cursor()
    my_cur.execute("SELECT station_id, lat, lon FROM M_BICING_STATIONS")

    data["stations"] =  my_cur.fetchall()
    data['distance_matrix'] = np.array([
        [0, 1, 2],  # Example distance matrix (replace with actual distances)
        [1, 0, 3],
        [2, 3, 0]
    ])

    return data

def create_graph(data):
    """Creates a graph using NetworkX."""
    G = nx.Graph()
    
    # Add nodes (stations)
    for station in data['stations']:
        station_id, lat, lon, _, _ = station
        G.add_node(station_id, pos=(lat, lon))
    
    # Add edges (connections between stations)
    num_stations = len(data['stations'])
    for i in range(num_stations):
        for j in range(i + 1, num_stations):
            distance = data['distance_matrix'][i][j]
            G.add_edge(i, j, weight=distance)
    
    return G

secrets = st.secrets

def main():
    """Entry point of the program."""
    # Create the data model
    with snowflake.connector.connect(**secrets) as snwf_cnx:
        data = create_data_model(snwf_cnx)
    # Create the graph using NetworkX
    G = create_graph(data)
    
    # Solve the vehicle routing problem using Google OR-Tools
    manager = pywrapcp.RoutingIndexManager(
            len(data['distance_matrix']),
            data['num_vehicles'],
            data['depot']
    )
    routing = pywrapcp.RoutingModel(manager)

    def distance_callback(from_index, to_index):
        """Returns the distance between the two nodes."""
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['distance_matrix'][from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Solve, returns a solution if any.
    assignment = routing.SolveWithParameters(search_parameters)
    # if assignment:
    #     # Print solution
    #     print_solution(data, manager, routing, assignment)
    # else:
    #     print('No solution found!')


if __name__ == '__main__':
    main()
