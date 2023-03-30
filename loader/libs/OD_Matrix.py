import os, sys, time, json
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from itertools import compress
import time
import pickle as pk
import subprocess


#add progress_apply to pandas for more readbility in code running
from tqdm import tqdm
tqdm.pandas()
#disable annoying warning forcing .loc
pd.options.mode.chained_assignment = None  # default='warn'

# Connection Eculidienne
def Load_trips(TRIP_FILE, outputdirpath, START_TIME, END_TIME, MODES, CRS):
    print("Reading trips")

    if TRIP_FILE.endswith('csv'):
        trips = pd.read_csv(TRIP_FILE)
        trips = trips.loc[
            trips['mode'].isin(MODES)
            & (trips['departure_time'] >= START_TIME)
            & (trips['arrival_time'] <= END_TIME)
        ]
        trips = gpd.GeoDataFrame(trips)
        print("Creating geometries")
        trips['origin'] = gpd.GeoSeries.from_xy(trips['x0'], trips['y0'], crs=CRS)
        trips['destination'] = gpd.GeoSeries.from_xy(trips['x1'], trips['y1'], crs=CRS)
    else:
        trips = gpd.read_file(TRIP_FILE, where=f"mode IN ({str(MODES).replace('[','(').replace(']',')')}) AND departure_time BETWEEN {START_TIME} AND {END_TIME} ")
        trips.to_crs(CRS, inplace=True)
        print("Reading origin / destination points")
        trips["origin"] = trips.geometry.apply(lambda g: Point(g.coords[0]))
        trips["destination"] = trips.geometry.apply(lambda g: Point(g.coords[-1]))
    trips["trip_index"]=trips.index
    trips["purpose"] = trips["preceding_purpose"] + " -> " + trips["following_purpose"]
    trips["travel_time"] = trips["arrival_time"] - trips["departure_time"]
    trips.drop(
        columns=[
            "preceding_activity_index",
            "following_activity_index",
            "is_first",
            "is_last",
            "geometry",
            "x0",
            "y0",
            "x1",
            "y1",
        ],
        inplace=True,
        errors='ignore',
    )
    trips.to_pickle(os.path.join("temp", "raw_car_time_trips"))
            
    return trips

def nearjoin(gdf, edges, CRS):
    edges = edges[edges["allow_od"]]

    edges["source_pt"]=edges["geometry"].apply(lambda g:Point( g.coords[0]))
    edges["target_pt"]=edges["geometry"].apply(lambda g:Point( g.coords[-1]))

    print("   Join nearest edges...")
    nearedges=gpd.sjoin_nearest(gdf.to_crs(CRS), 
                                edges[["id","source", "source_pt","target","target_pt","road_type", "geometry"]].to_crs(CRS),
                                distance_col="edge_dist", how="inner")
    nearedges = nearedges.drop_duplicates(subset=["trip_index"])

    print("   Compute distance node ...")
    #set source / target point distance
    print("   sources")
    nearedges["source_dist"]=nearedges.progress_apply(lambda e : e["geometry"].distance(e["source_pt"]), axis=1)
    print("   targets")
    nearedges["target_dist"]=nearedges.progress_apply(lambda e : e["geometry"].distance(e["target_pt"]), axis =1)
    # True for source closer, False fot target
    print("   Find nearest node...")
    nearedges["nearst"] = nearedges.progress_apply(lambda e :  (e["source_dist"]) < (e["target_dist"])  , axis=1)
    #set connector value acconrdingly
    print("   Assignating values:")
    print("     -when source is near")
    nearsource = nearedges.loc[nearedges["nearst"]==True]
    nearsource["connect"] = nearsource["source"]
    nearsource["connect_dist"] = nearsource["source_dist"]
    nearsource["geometry"] = nearsource["source_pt"]
    print("     -when target is near")
    neartarget = nearedges.loc[nearedges["nearst"]==False]
    neartarget["connect"] = neartarget["target"]
    neartarget["connect_dist"] = neartarget["target_dist"]
    neartarget["geometry"] = neartarget["target_pt"]
    print("   Merging")
    nearedges= gpd.GeoDataFrame(pd.concat([nearsource,neartarget], ), crs=nearsource.crs).sort_index()

    # print("     connector")
    # nearedges["connect"] = nearedges.progress_apply(lambda e : e["source"] if e.nearst else e["target"], axis=1)
    # print("     distance")
    # nearedges["connect_dist"] = nearedges.progress_apply(lambda e : e["source_dist"] if e.nearst else e["target_dist"], axis=1)
    # print("     point")
    # nearedges["geometry"] = nearedges.progress_apply(lambda e : e["source_pt"] if e.nearst else e["target_pt"], axis=1)

    nearedges.drop(
        columns=[
                "source",
                "source_pt",
                "source_dist",
                "target",
                "target_pt",
                "target_dist",
                "nearst"
            ],
            inplace=True,
        )

    return nearedges

def Connect_OD(outputdirpath, edges, trips, CRS): 

    chronos1A=time.time()

    #separation of Origin and Destination gdf:
    O = gpd.GeoDataFrame(trips[["trip_index"]],geometry=trips["origin"])
    D = gpd.GeoDataFrame(trips[["trip_index"]],geometry=trips["destination"])

    print("Joining Origins... ")
    nearO = nearjoin(O, edges, CRS)
    print("Joining Destinations...")
    nearD = nearjoin(D, edges, CRS)

    trips=trips.join(nearO[["connect","connect_dist","edge_dist"]].add_prefix("O_"), how="inner", on="trip_index")
    trips=trips.join(nearD[["connect","connect_dist","edge_dist"]].add_prefix("D_"), how="inner", on="trip_index")

    #delete trips with same O and D
    trips=trips[trips["O_connect"]!=trips["D_connect"]]

    #trips.to_csv(outputdirpath+"trips_OD.csv")
    trips.to_pickle(os.path.join("temp","trips_OD"))

    chronos1A=time.time()-chronos1A
    print(f"Le script a mis {chronos1A/60} minutes à tourner")

    return trips

#Connection ShortestPath
def prepare_query(trips):
    trips[['departime']]=0
    query = trips[["trip_index","O_connect","D_connect","departime"]].values.tolist()

    print("Writing data...")
    print("Queries")
    with open("temp/queries.json", "w") as f:
        f.write(json.dumps(query))
    
    print("Done!")

def prepare_graph(edges):
   
    print("Creating Graph")
    e = edges[["source","target","traveltime"]]
    graph = [*map(list, zip(*map(e.get, e)))]

    print("Writing data...")
    print("Graph")
    with open("temp/graph.json", "w") as f:
        f.write(json.dumps(graph))
    print("Done!")

def prepare_parameters(parameters = {"algorithm": "TCH", "output_route": True}):

    parameters["output_route"] = bool(parameters["output_route"])
    
    print("Parameters")
    with open("temp/parameters.json", "w") as f:
        f.write(json.dumps(parameters))
    
    print("Done!")

def run_shortestpath(edges, trips, script, parameters={'algorithm': 'TCH', 'output_route': 'True'}):

    prepare_query(trips)
    prepare_graph(edges)
    prepare_parameters(parameters)

    #Inputs of the shortestpath script

            #Path to the file where the queries to compute are stored
    q=  f"--queries  temp/queries.json "
            #Path to the file where the graph is stored
    g=  f"--graph temp/graph.json "
            #Path to the file where the results of the queries should be stored
    o=  f"--output temp/output.json "
            #Path to the file where the parameters are stored 
    p=  f"--parameters temp/parameters.json "

    #Run TCH script
    print("Run TCH script")
    #add ".exe" if on windows
    syst = ""
    if (sys.platform == "win32" and not script.endswith(".exe")):
        syst=".exe"

    command = f"{script}{syst} {q} {p} {o} {g} " 

    subprocess.run(command, shell=True)
    print(f"Saved in temp")

    with open('temp/output.json', 'r') as f:
        data = json.load(f)

    res = data["results"]
    res = pd.DataFrame(list(filter(None,res)), columns=["id", "travel_time", "route"]).set_index("id")

    return res

def find_connections(res, edges, trips, OUTDIR, penalty=5):
    print("Prepare edges data")
    valid_edges=set(edges[edges["main_network"]]["id"])
    edges=edges.set_index("id")
    edge_time=edges.traveltime.to_dict()
    edge_source=edges.source.to_dict()
    edge_target=edges.target.to_dict()

    res["id"] = res.index

    print("Find the valid edges on the route")
    res["main_part"] = res.route.progress_apply(lambda l : [x for x in l if x in valid_edges])
    print("Mask out the main / residential edges")
    res["mask"] = res.route.progress_apply(lambda l : [x in valid_edges for x in l ])
    print("Extract the position of each edges")
    res["mask"] = res["mask"].progress_apply(lambda l : list(compress(range(len(l)), l)))
    print("Keep first and last")
    res["mask"] = res["mask"].progress_apply(lambda l : [l[0],l[-1]] if len(l)!=0 else 0)
    print("Put trips that never use the main network on the side")
    no_main = res[res["mask"]==0]
    print(f"{len(no_main)} trips identified : no travel on the main network")
    res = res[res["mask"]!=0]

    print("extract connection edges and nodes into O_edge, D_edge, O_node and D_node columns")
    res["O_edge"]=res.main_part.progress_apply(lambda l : l[0] )
    res["D_edge"]=res.main_part.progress_apply(lambda l : l[-1])
    res["O_node"]=res.O_edge.progress_apply(lambda o : edge_source[o])
    res["D_node"]=res.D_edge.progress_apply(lambda d : edge_target[d])
    #res["O_node"]=res.O_edge.progress_apply(lambda o : int(edges[edges["id"]==o]["source"].values) )
    #res["D_node"]=res.D_edge.progress_apply(lambda d : int(edges[edges["id"]==d]["target"].values))
    
    print("Trim the acces trips to the main network, save them on the side")
    start = res["mask"].apply(lambda l : l[0]).to_dict()
    finish = res["mask"].apply(lambda l : l[-1]).to_dict()
    print("start_access:")
    res["start_access"]=res.progress_apply(lambda l: l.loc["route"][:start[l.loc["id"]]] , axis=1 )
    print("finish_access:")
    res["finish_access"]=res.progress_apply(lambda l: l.loc["route"][finish[l.loc["id"]]+1:] , axis=1 )
    print("main_route:")
    res["only_main"] = res.progress_apply(lambda l: l.loc["route"][start[l.loc["id"]]:finish[l.loc["id"]]+1] , axis=1 )
    print("Find how many residential edges are used in the middle of main trips")
    res["has_residential"] = res.progress_apply( lambda l : len(l.loc["only_main"])-len(l.loc["main_part"]), axis=1)

    print("Compute access times")
    print("start_access_time:")
    res["start_access_time"] = res.progress_apply(lambda l : np.sum([edge_time[x]+penalty for x in l.loc["start_access"]]), axis=1)
    print("finish_access_time:")
    res["finish_access_time"] = res.progress_apply(lambda l : np.sum([edge_time[x]+penalty for x in l.loc["finish_access"]]), axis=1)

    res["road_leg"]=True
    no_main["road_leg"]=False
    trips["O_connect_time"]=trips["O_connect_dist"]/(30/3.6)
    trips["D_connect_time"]=trips["D_connect_dist"]/(30/3.6)

    r_legs=res.join(trips, lsuffix="_sp")
    
    r_legs["O_access_time"]=r_legs["O_connect_time"]+r_legs["start_access_time"]
    r_legs["D_access_time"]=r_legs["D_connect_time"]+r_legs["finish_access_time"]

    v_legs = no_main[['id','road_leg','travel_time','route']].join(trips, lsuffix="_sp")

    trips = pd.DataFrame(pd.concat([r_legs,v_legs], )).sort_index()
    trips.to_csv(os.path.join(OUTDIR, "metro_trips.csv"))
    
    return trips


