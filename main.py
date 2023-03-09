#%% 
# IMPORT SCRIPTS
import os, sys, time
#base libraries
import numpy as np
import pandas as pd
import geopandas as gpd
import pickle as pk
#scrpit functions
import Load_data as script0
import Connect_OD_to_network as script1
import Link_main_network as script2
import Find_main_connect_nodes as script3

#add progress_apply to pandas for more readbility in code running
from tqdm import tqdm
tqdm.pandas()
#disable annoying warning forcing .loc
pd.options.mode.chained_assignment = None  # default='warn'
# if you want to plt.show thing in command run
from matplotlib import pyplot as plt


#%% PARAMETERS
#Input files
OSM_FILE= r"C:\Users\theot\OneDrive\TheseMaster\Datapipeline\alsace-latest.osm.pbf"
TRIPS_FILE= r"C:\Users\theot\OneDrive\TheseMaster\Data\Equasim\output_100%\strasbourg_trips.gpkg"

#Outputs
output_folder = "strasbourg_outputs"

#Map Projections
METRIC_CRS="EPSG:2154"
WORLD_CRS="EPSG:4326"

#Shortest path executable
scriptdir = r"C:\Users\theot\OneDrive\TheseMaster\Datapipeline\ShortestPath"
TCHscript = "compute_travel_times"

#set log
log = {}

#%% 0
# SCRIPT 0: Load_OSM_Data
chronos=time.time()
chronos0=time.time()
OUTDIR_0 = os.path.join(output_folder,"OSM_to_edges_nodes")

if not os.path.isdir(OUTDIR_0):
    os.makedirs(OUTDIR_0)

NODE_FILE = os.path.join(OUTDIR_0,"raw_nodes")
EDGE_FILE = os.path.join(OUTDIR_0,"raw_edges")

# File does not exists or is not in the same folder as the script.
if not os.path.exists(OSM_FILE):
    print("File not found: {}".format(OSM_FILE))
    sys.exit(0)

h = script0.NodeReader()

print("Finding nodes...")
h.apply_file(OSM_FILE, locations=True, idx="flex_mem")

g = script0.Writer(h.nodes)

print("Reading OSM data...")
g.apply_file(OSM_FILE, locations=True, idx="flex_mem")

print("Post-processing...")
g.post_process(simplify=False)

print("Found {} nodes and {} edges.".format(len(g.nodes), len(g.edges)))

print("Writing edges...")
g.write_edges(EDGE_FILE)

print("Writing nodes...")
g.write_nodes(NODE_FILE)

print("Done!")

log["script 0 nodes count"] = len(g.nodes)
log["script 0 edges count"] = len(g.edges)

chronos0=time.time()-chronos0
print(f"Le script 0 a mis {chronos0/60} minutes à tourner")

#%% 1
# SCRIPT 1: Connect_OD_to_full_network
chronos1=time.time()

OUTDIR_1 = os.path.join(output_folder,"OD_to_network")
if not os.path.isdir(OUTDIR_1):
    os.makedirs(OUTDIR_1)


TRIP_FILE=os.path.join(OUTDIR_1,"trips_OD")

CONNECTED_ROADTYPES = {
    #"motorway": 1,
    #"trunk": 2,
    "primary": 3,
    "secondary": 4,
    "tertiary": 5,
    "unclassified": 6,
    "residential": 7,
    #"motorway_link": 8,
    #"trunk_link": 9,
    "primary_link": 10,
    "secondary_link": 11,
    "tertiary_link": 12,
    "living_street": 13,
    "road": 14,
    "service": 15,
}

trips,O,D = script1.prepare_trips(TRIPS_FILE,OUTDIR_1)
edges = script1.read_edges(EDGE_FILE, OUTDIR_0)

log = {}


log["script 1 start trips count"]=len(trips)
log["script 1 finish trips count"]=len(edges)

nearO = script1.nearjoin(O, edges, OUTDIR_1+"Originset", CONNECTED_ROADTYPES, save=False )
nearD = script1.nearjoin(D, edges, OUTDIR_1+"Destinationset", CONNECTED_ROADTYPES, save=False )

trips=trips.join(nearO[["connect","connect_dist","edge_dist"]].add_prefix("O_"), how="inner", on="trip_index")
trips=trips.join(nearD[["connect","connect_dist","edge_dist"]].add_prefix("D_"), how="inner", on="trip_index")

#delete trips with same O and D
trips=trips[trips["O_connect"]!=trips["D_connect"]]

#trips.to_csv(OUTDIR_1+"trips_OD.csv")
trips.to_pickle(os.path.join(OUTDIR_1,"trips_residential_connected"))


log["script 1 finish trips count"]=len(trips)

chronos1=time.time()-chronos1
print(f"Le script 1 a mis {chronos1/60} minutes à tourner")

#%% 2
# SCRIPT 2 : Link_full_to_main

chronos2=time.time()

OUTDIR_2 = os.path.join(output_folder,"Shortest_Path")
if not os.path.isdir(OUTDIR_2):
    os.makedirs(OUTDIR_2)

trips, edges, nodes = script2.prepare_inputs(OUTDIR_2, TRIP_FILE, EDGE_FILE, NODE_FILE, save=True ,crs=METRIC_CRS)


log["script 2 start trips count"]=len(trips)
log["script 2 start edges count"]=len(edges)
log["script 2 start nodes count"]=len(nodes)



script2.prepare_shortestpath(OUTDIR_2, trips, edges)

script2.run_shortestpath(OUTDIR_2, scriptdir, TCHscript)

chronos2=time.time()-chronos2
print(f"Le script 2 a mis {chronos2/60} minutes à tourner")


#%% 3
# SCRIPT 3 : Find_main_bounday_node

chronos3=time.time()
RESULTS = os.path.join(OUTDIR_2,"output.json")


res = script3.load_results(RESULTS)

res, no_main = script3.find_connections(res, edges)

res.to_pickle(os.path.join(OUTDIR_2,"treated_results"))
no_main.to_pickle(os.path.join(OUTDIR_2,"side_results"))

chronos3=time.time()-chronos3
print(f"Le script 3 a mis {chronos3/60} minutes à tourner")
#%% 4
# Script 4: writing_metro_inputs

#res = pk.load(open(os.path.join(OUTDIR_2,"treated_results"),"rb"))
res["road_leg"]=True
#no_main=pk.load(open(os.path.join(OUTDIR_2,"side_results"),"rb"))
no_main["road_leg"]=False
#trips= pk.load(open(os.path.join(OUTDIR_1,"trips_OD"),"rb"))

trips["O_connect_time"]=trips["O_connect_dist"]/(30/3.6)
trips["D_connect_time"]=trips["D_connect_dist"]/(30/3.6)

trips = trips[['person_id', 'trip_index', 'departure_time', 'arrival_time', 'mode',
            'preceding_purpose', 'following_purpose', 'purpose', 'travel_time',
            'O_connect_time', 'D_connect_time']]

r_legs=res[['id','road_leg','travel_time','route', 
            'O_node', 'D_node', 
            'start_access_time', 'finish_access_time',  
            'start_access', 'finish_access', 
            'only_main', 'has_residential']].join(trips, lsuffix="_sp")
r_legs["O_access_time"]=r_legs["O_connect_time"]+r_legs["start_access_time"]
r_legs["D_access_time"]=r_legs["D_connect_time"]+r_legs["finish_access_time"]



v_legs = no_main[['id','road_leg','travel_time','route']].join(trips, lsuffix="_sp")

trips = pd.DataFrame(pd.concat([r_legs,v_legs], )).sort_index()

trips.to_pickle(os.path.join(output_folder,"final_trip_file"))

# from shapely.geometry import Point, LineString
# test["timediff"] = test["travel_time"]-test["travel_time_sp"]
# error = test[test.timediff > 5000 ][['departure_time', 'arrival_time',"origin","destination","travel_time","travel_time_sp"]]  
# error["geometry"]=error.apply(lambda l : LineString([l["origin"],l["destination"]]), axis=1)
# error = error.set_geometry("geometry")
# error.crs = METRIC_CRS
# import contextily as cx
# cx.add_basemap(error.plot() ,crs=METRIC_CRS, source=cx.providers.CartoDB.Voyager )
# plt.show()
# #error.drop(columns=['origin','destination']).to_file("errors.geojson",driver="GeoJSON")

#%% 
# Plot performances
print(f"Le script 0 a mis {chronos0/60} minutes à tourner")
print(f"Le script 1 a mis {chronos1/60} minutes à tourner")
print(f"Le script 2 a mis {chronos2/60} minutes à tourner")
print(f"Le script 3 a mis {chronos3/60} minutes à tourner")
chronos=time.time()-chronos
print(f"Le total des scripts ont mis {chronos/60} minutes à tourner")

