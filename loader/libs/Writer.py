
import os
import json

import numpy as np
import pandas as pd
import pickle as pk
import geopandas as gpd
import itertools
import libs.Network as Net

def default_post_process(nodes, edges, trips, save="GeoJSON", savedir="Load_Outputs"):
    #add the traveled through edges to the network:
    # access_edges_ids = list(itertools.chain.from_iterable(trips[trips.start_access_time>0]["start_access"]))+list(itertools.chain.from_iterable(trips[trips.finish_access_time>0]["finish_access"]))
    print("Finding residential edges used on Road legs...")
    Used_edges_ids = set(itertools.chain.from_iterable(trips[trips["has_residential"]>0]["only_main"]))
    Used_edges = edges.loc[edges.id.isin(Used_edges_ids)][edges.main_network]
    Used_nodes_ids = set(Used_edges.source + Used_edges.target)

    print("Adding traveled edges to the main network...")
    edges.loc[edges.id.isin(Used_edges_ids),"main_network"]=True
    nodes.loc[nodes.id.isin(Used_nodes_ids),"main_network"]=True
    
    #After adding new edges to the main network, we need to reindex it again
    nodes, edges, ptrips = Net.reindex(nodes, edges, trips[['O_node', 'D_node', 'O_connect', 'D_connect']])
    
    trips['O_node'] = ptrips['O_node']
    trips['D_node'] = ptrips['D_node']
    trips['O_connect'] = ptrips['O_connect']
    trips['D_connect'] = ptrips['D_connect']

    edges = edges[edges.main_network]

    if save != "":
        print(f"Saving as {save}...")
        edges.to_file(os.path.join(savedir,"metro_edges."+save), driver=save)
        nodes.to_file(os.path.join(savedir,"metro_nodes."+save), driver=save)
    print("Keep pickles in temp...")
    edges.to_pickles("temp/metro_edges")
    nodes.to_pickles("temp/metro_nodes")
    
    print("Done!")
    return nodes, edges, trips

def disconnect_post_process(nodes, edges, trips, save="GeoJSON", savedir="Load_Outputs"):

    nodes, edges = Net.keep_weakly_connected(nodes[nodes.main_network], edges[edges.main_network])

    #We need to reindex it again
    nodes, edges, ptrips = Net.reindex(nodes, edges, trips['O_node', 'D_node', 'O_connect', 'D_connect',])
    
    if save != "":
        print(f"Saving as {save}...")
        edges.to_file(os.path.join(savedir,"metro_edges."+save), driver=save)
        nodes.to_file(os.path.join(savedir,"metro_nodes."+save), driver=save)
    print("Keep pickles in temp...")
    edges.to_pickles("temp/metro_edges")
    nodes.to_pickles("temp/metro_nodes")
    
    return nodes, edges, trips


def writer_inputs(writerconfig):
    ff = writerconfig["From Files"]
    print("Reading from files...")
    if ff["TRIPS_FILE"].endswith('csv'):
        trips = pd.read_csv(ff["TRIPS_FILE"], sep=ff["sep"])
    else:
        trips = pk.load(open(ff["TRIPS_FILE"], "rb"))

    if "driver" in  ff:
        edges = gpd.read_file(ff["EDGES_FILE"])
    else:
        edges = pk.load(open(ff["EDGES_FILE"], "rb"))  

    return edges, trips

    

def read_writer_parameters(writerconfig):
    global  PERIOD, SEED, SAMPLE, RNG, pre_sampled
    
    PARAMETERS = writerconfig["Parameters"]
    agent_param = writerconfig["Agents"]
    network_param = writerconfig["Road_Network"]

    PERIOD = PARAMETERS["period"]
    SAMPLE = float(writerconfig["SAMPLE"])
    SEED = int(writerconfig["RANDOM_SEED"])
    RNG = np.random.default_rng(SEED)
    pre_sampled = bool(writerconfig["Pre Sampled"])

    return PARAMETERS, agent_param, network_param

def generate_road_network(edges, network_param):

    CONST_TT  = network_param["Graph"]["CONST_TT"]

    if "VehiclesInfoFile" in network_param["Vehicles"]:
        print("Reading VehiclesInfoFile ...")
        pd.read_csv(network_param["Vehicles"]["VehiclesInfoFile"], sep = network_param["Vehicles"]["sep"])
    else: 
        VEHICLE_LENGTH =float(network_param["Vehicles"]["VEHICLE_LENGTH"]) * SAMPLE
        VEHICLE_PCE = float(network_param["Vehicles"]["VEHICLE_PCE"]) * SAMPLE
        vehicles = [
        {
            "length": VEHICLE_LENGTH,
            "pce": VEHICLE_PCE,
            "speed_function": {
                "type": "Base",
            },
        }
    ]  
    
    print("Creating Metropolis road network")
    metro_edges = list()
    for _, row in edges.iterrows():
        print(f"Edge : {_} / {len(edges)}", end="\r")
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
        if type(row["capacity"]) == int:
            edge[2]["bottleneck_flow"] = row["capacity"] / 3600.0
            
        if CONST_TT == "DEFAULT":
            edge[2]["constant_travel_time"] = row["neighbor_count"]
        metro_edges.append(edge)
    print(f"{_} edges processed!")

    graph = {
        "edges": metro_edges,
    }

    road_network = {
        "graph": graph,
        "vehicles": vehicles,
    }
    return road_network

def default_tstar_func(ta):
    return ta


def generate_agents(trips, agent_param):
    if agent_param["T_STAR_FUNC"]=="DEFAULT":
        T_STAR_FUNC = default_tstar_func

    ALPHA = float(agent_param["ALPHA"])
    BETA = float(agent_param["BETA"])
    GAMMA = float(agent_param["GAMMA"])
    DELTA = float(agent_param["DELTA"])
    ENDOGENOUS_DEPARTURE_TIME = bool(agent_param["ENDOGENOUS DEPARTURE TIME"])
    DT_MU = float(agent_param["DT_MU"])
   
    print("Generating agents")
    random_u = iter(RNG.uniform(size=len(trips)))
    agents = list()
    if pre_sampled == False:
        trips = trips.sample(frac = SAMPLE)
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

def write_data(OUTPUT_DIR,road_network,agents,PARAMETERS):
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
    with open(os.path.join("Metro_Inputs", "parameters.json"), "w") as f:
        f.write(json.dumps(PARAMETERS))