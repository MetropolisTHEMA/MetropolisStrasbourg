#%% IMPORTS
import os
import json

import numpy as np
import pandas as pd
import geopandas as gpd
import itertools

#%% PARAMETERS
# Path to the directory where the node and edge files are stored.
ROAD_NETWORK_DIR = "./output/OSM_to_edges_nodes"
# Path to the file where the trip data is stored.
TRIPS_FILE = "./output/ShortestPath/final_results.csv"
# Path to the directory where the simulation input should be stored.
OUTPUT_DIR = "./output/next_run/"
# Vehicle length in meters.
VEHICLE_LENGTH = 10.0 * 10.0
# Vehicle passenger-car equivalent.
VEHICLE_PCE = 10.0 * 1.0
# Period in which the departure time of the trip is chosen.
PERIOD = [3.0 * 3600.0, 10.0 * 3600.0]
# Capacity of the different edge road types.
CAPACITY = {
    1: 2000,
    2: 2000,
    3: 1500,
    4: 800,
    5: 600,
    6: 600,
    7: 600,
    8: 1500,
    9: 1500,
    10: 1500,
    11: 800,
    12: 600,
    13: 300,
    14: 300,
    15: 300,
}
# If True, enable entry bottleneck using capacity defined by `CAPACITY`.
ENTRY_BOTTLENECK = True
# If True, enable exit bottleneck using capacity defined by `CAPACITY`.
EXIT_BOTTLENECK = False
# Value of time in the car, in euros / hour.
ALPHA = 15.0
# Value of arriving early at destination, in euros / hour.
BETA = 7.5
# Value of arriving late at destination, in euros / hour.
GAMMA = 30.0
# Time window for on-time arrival, in seconds.
DELTA = 0.0
# If True, departure time is endogenous.
ENDOGENOUS_DEPARTURE_TIME = True
# Value of μ for the departure-time model (if ENDOGENOUS_DEPARTURE_TIME is True).
DT_MU = 3.0
# How t* is computed given the observed arrival time.
def T_STAR_FUNC(ta):
    return ta
CONST_TT = {
    3: 10,
    4: 15,
    5: 20,
    6: 25,
    7: 30,
    8: 35,
    9: 40,
    10: 45,
}
# Seed for the random number generators.
SEED = 13081996
RNG = np.random.default_rng(SEED)


# Parameters to use for the simulation.
PARAMETERS = {
    "period": PERIOD,
    "init_iteration_counter": 1,
    "learning_model": {
        "type": "Exponential",
        "value": {
            "alpha": 0.99,
        },
    },
    "stopping_criteria": [
        {
            "type": "MaxIteration",
            "value": 5,
        },
    ],
    "update_ratio": 1.0,
    "random_seed": SEED,
    "network": {
        "road_network": {
            "recording_interval": 300.0
        }      },
    "nb_threads" : 0 # default 0: uses all possible threads

}


#%% PRE_PROCESS
def pre_preocess(nodes, edges, trips, save="GeoJSON", savedir="Metro_Input"):

    #add the traveled through edges to the network:
    access_edges_ids = list(itertools.chain.from_iterable(trips[trips.start_access_time>0]["start_access"]))+list(itertools.chain.from_iterable(trips[trips.finish_access_time>0]["finish_access"]))
    Used_edges_ids = set(itertools.chain.from_iterable(trips[trips["has_residential"]>0]["only_main"]))
    Used_edges = edges.loc[edges.id.isin(Used_edges_ids)][edges.main_network]
    Used_nodes_ids = set(Used_edges.source + Used_edges.target)

    edges.loc[edges.id.isin(Used_edges_ids),"main_network"]=True
    nodes.loc[nodes.id.isin(Used_nodes_ids),"main_network"]=True
    
    #After adding new edges to the main network, we need to reindex it again
    print("Index reset")
    nodes.reset_index(drop=True, inplace=True)
    edges.reset_index(drop=True, inplace=True)
    edges["id"]=edges.index
    #edges.drop(columns="id", inplace=True)
    node_id_map = nodes["id"].to_frame().reset_index().set_index("id")
    nodes["id"]=nodes.index
    edges = edges.merge(node_id_map, left_on="source", right_index=True).drop(columns=["source"]).rename(
        columns={"index": "source"}
    )
    edges = edges.merge(node_id_map, left_on="target", right_index=True).drop(columns=["target"]).rename(
        columns={"index": "target"}
    ).sort_index()

    if save != "":
        edges.to_file(os.path.join(savedir,"metro_edges."+save), driver=save)
        nodes.to_file(os.path.join(savedir,"metro_nodes."+save), driver=save)
    return nodes, edges

#%% GRAPH
def generate_road_network(edges):
    print("Creating Metropolis road network")
    metro_edges = list()
    for _, row in edges.iterrows():
        edge = [
            row["source"],
            row["target"],
            {
                "id": int(row["id"]),
                "base_speed": float(row["speed"]) / 3.6,
                "length": float(row["length"]),
                "lanes": int(row["lanes"]),
                "speed_density": {
                    "type": "FreeFlow",
                },
            },
        ]
        if capacity := CAPACITY.get(row["road_type"]):
            
            edge[2]["bottleneck_flow"] = capacity / 3600.0
            
        if const_tt := CONST_TT.get(row["neighbor_count"]):
            edge[2]["constant_travel_time"] = const_tt
        metro_edges.append(edge)

    graph = {
        "edges": metro_edges,
    }

    vehicles = [
        {
            "length": VEHICLE_LENGTH,
            "pce": VEHICLE_PCE,
            "speed_function": {
                "type": "Base",
            },
        }
    ]

    road_network = {
        "graph": graph,
        "vehicles": vehicles,
    }
    return road_network

#%% AGENTS
def generate_agents(trips):
    print("Generating agents")
    random_u = iter(RNG.uniform(size=len(trips)))
    agents = list()
    maxp = trips.person_id.max()
    maxt = trips.id.max()
    for person_id, idx in trips.groupby('person_id').groups.items():
        legs = list()
        prev_ta = None
        for key, trip in trips.loc[idx].iterrows():
            print(f"person_id : {person_id}/{maxp}    trip: {trip['id']}/{maxt}", end="\r")
                    
            t_star = T_STAR_FUNC(trip["arrival_time"])
            
            if trip["road_leg"]:
                leg={"class" : {
                    "type": "Road",
                    "value": {
                        "origin": int(trip['O_node']),
                        "destination": int(trip['D_node']),
                        "vehicle": 0,
                            }
                    }}
                leg["travel_utility"]= {
                    "type": "Polynomial",
                    "value": {
                        "b": -ALPHA / 3600.0,
                    }
                }                
                leg["schedule_utility"]= {
                    "type": "AlphaBetaGamma",
                    "value": {
                        "beta": BETA / 3600.0,
                        "gamma": GAMMA / 3600.0,
                        "t_star_high": t_star + DELTA / 2.0 - trip["D_access_time"],
                        "t_star_low": t_star - DELTA / 2.0 - trip["D_access_time"],
                        }
                    }
                if not prev_ta is None:
                    # Set stopping time of previous leg.
                    legs[-1]["stopping_time"] = trip['departure_time'] - prev_ta + trip["O_access_time"]
                prev_ta = trip['arrival_time'] - trip["D_access_time"]
                
            else:
                leg = {
                    "class": {
                    "type": "Virtual",
                    "value": trip["travel_time_sp"]} 
                    }             
                if not prev_ta is None:
                    # Set stopping time of previous leg.
                    legs[-1]["stopping_time"] = trip['departure_time'] - prev_ta
                prev_ta = trip['arrival_time']
            legs.append(leg)
        if ENDOGENOUS_DEPARTURE_TIME:
            departure_time_model = {
                "type": "ContinuousChoice",
                "value": {
                    "period": PERIOD,
                    "choice_model": {
                        "type": "Logit",
                        "value": {
                            "u": next(random_u),
                            "mu": DT_MU,
                        },
                    },
                },
            }
        else:
            departure_time_model = {
                "type": "Constant",
                "value": trips.loc[idx[0], "departure_time"],
            }
        car_mode = {
            "type": "Trip",
            "value": {
                "legs": legs,
                "departure_time_model": departure_time_model,
            },
        }
        agent = {
            "id": person_id,
            "modes": [car_mode],
        }
        agents.append(agent)
    print("\n Done !")
    return agents
#%% WRITER
def write_data(OUTPUT_DIR,road_network,agents,PARAMETERS=PARAMETERS):
    print("Writing data...")
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    print("network")
    with open(os.path.join(OUTPUT_DIR, "network.json"), "w") as f:
        f.write(json.dumps(road_network))
    print("agents")
    with open(os.path.join(OUTPUT_DIR, "agents.json"), "w") as f:
        f.write(json.dumps(agents))
    print("parameters")
    with open(os.path.join(OUTPUT_DIR, "parameters.json"), "w") as f:
        f.write(json.dumps(PARAMETERS))
#%%
if __name__ == "__main__":
        
    print("Reading edges")
    edges = gpd.read_file(os.path.join(ROAD_NETWORK_DIR, "raw_edges.geojson"))
    
    print("Reading trips")
    trips = pd.read_csv(TRIPS_FILE)

    road_network = generate_road_network(edges[edges["main_network"]])

    agents = generate_agents(trips)

    write_data(OUTPUT_DIR, road_network, agents, PARAMETERS)


# %%
