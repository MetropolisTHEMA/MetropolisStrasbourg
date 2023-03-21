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
import Write_Metro_Inputs as script4

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
# File where the Eqasim synthetic population trip geopackage is stored.
# If memory is an issue, run the following command on the geopackage:
# `ogr2ogr -f "CSV" FILENAME.csv FILENAME.gpkg -sql "SELECT person_id, trip_index, departure_time,
#  arrival_time, mode, preceding_purpose, following_purpose, ST_X(ST_StartPoint(geom)) AS 'x0'
#  ST_Y(ST_StartPoint(geom)) AS 'y0', ST_X(ST_EndPoint(geom)) AS 'x1', ST_Y(ST_EndPoint(geom))
#  AS 'y1' FROM FILENAME"`
# Then specify the path to the CSV file instead of the geopackage file.
OD_FILE= r"C:\Users\theot\OneDrive\TheseMaster\Data\Equasim\output_100%\strasbourg_trips.gpkg"

#Outputs
output_folder = "FINAL_strasbourg_outputs"

#Map Projections
METRIC_CRS="EPSG:2154"
WORLD_CRS="EPSG:4326"

# Returns only trips whose mode is within the following modes
# (available values: car, car_passenger, pt, walk, bike).
MODES = ("car", "car_passenger", "pt", "walk", "bike")
# Returns only trips whose departure time is later than this value (in seconds after midnight).
START_TIME = 3.0 * 3600.0
# Returns only trips whose arrival time is earlier than this value (in seconds after midnight).
END_TIME = 10.0 * 3600.0

#Shortest path executable
scriptdir = r"C:\Users\theot\OneDrive\TheseMaster\Datapipeline\ShortestPath"
TCHscript = "compute_travel_times"

# all dirs

LOAD_DIR = os.path.join(output_folder,"OSM_to_edges_nodes")
if not os.path.isdir(LOAD_DIR):
    os.makedirs(LOAD_DIR)
OD_NETWORK_DIR = os.path.join(output_folder,"OD_to_network")
if not os.path.isdir(OD_NETWORK_DIR):
    os.makedirs(OD_NETWORK_DIR)
SHORT_PATH_DIR = os.path.join(output_folder,"Shortest_Path")
if not os.path.isdir(SHORT_PATH_DIR):
    os.makedirs(SHORT_PATH_DIR)
METRO_INPUT_DIR = os.path.join(output_folder,"Metro_Input")
if not os.path.isdir(METRO_INPUT_DIR):
    os.makedirs(METRO_INPUT_DIR)

NODE_FILE = os.path.join(LOAD_DIR,"raw_nodes")
EDGE_FILE = os.path.join(LOAD_DIR,"raw_edges")
TRIP_FILE=os.path.join(OD_NETWORK_DIR,"trips_residential_connected")

#set log
log = {}

#%% 0
# SCRIPT 0: Load_OSM_Data
chronos=time.time()
chronos0=time.time()
# File does not exists or is not in the same folder as the script.
if not os.path.exists(OSM_FILE):
    print("File not found: {}".format(OSM_FILE))
    sys.exit(0)

try:
    edges = pk.load(open(EDGE_FILE,"rb"))
    nodes = pk.load(open(NODE_FILE,"rb"))
except:
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

trips,O,D = script1.prepare_trips(OD_FILE,OD_NETWORK_DIR)
edges = script1.read_edges(EDGE_FILE, LOAD_DIR)

log = {}


log["script 1 start trips count"]=len(trips)
log["script 1 finish trips count"]=len(edges)

nearO = script1.nearjoin(O, edges, OD_NETWORK_DIR+"Originset", CONNECTED_ROADTYPES, save=False )
nearD = script1.nearjoin(D, edges, OD_NETWORK_DIR+"Destinationset", CONNECTED_ROADTYPES, save=False )

trips=trips.join(nearO[["connect","connect_dist","edge_dist"]].add_prefix("O_"), how="inner", on="trip_index")
trips=trips.join(nearD[["connect","connect_dist","edge_dist"]].add_prefix("D_"), how="inner", on="trip_index")

#delete trips with same O and D
trips=trips[trips["O_connect"]!=trips["D_connect"]]

#trips.to_csv(OUTDIR_1+"trips_OD.csv")
trips.to_pickle(os.path.join(OD_NETWORK_DIR,"trips_residential_connected"))


log["script 1 finish trips count"]=len(trips)

chronos1=time.time()-chronos1
print(f"Le script 1 a mis {chronos1/60} minutes à tourner")

#%% 2
# SCRIPT 2 : Link_full_to_main

chronos2=time.time()

trips, edges, nodes = script2.prepare_inputs(SHORT_PATH_DIR, TRIP_FILE, EDGE_FILE, NODE_FILE, save=True ,crs=METRIC_CRS)


log["script 2 start trips count"]=len(trips)
log["script 2 start edges count"]=len(edges)
log["script 2 start nodes count"]=len(nodes)



script2.prepare_shortestpath(SHORT_PATH_DIR, trips, edges)

script2.run_shortestpath(SHORT_PATH_DIR, scriptdir, TCHscript)

chronos2=time.time()-chronos2
print(f"Le script 2 a mis {chronos2/60} minutes à tourner")


#%% 3
# SCRIPT 3 : Find_main_bounday_node

chronos3=time.time()
RESULTS = os.path.join(SHORT_PATH_DIR,"output.json")


res = script3.load_results(RESULTS)

res, no_main = script3.find_connections(res, edges)

res.to_pickle(os.path.join(SHORT_PATH_DIR,"treated_results"))
no_main.to_pickle(os.path.join(SHORT_PATH_DIR,"side_results"))

trips = script3.final_trip_file(res,no_main, trips)

trips.to_pickle(os.path.join(SHORT_PATH_DIR,"final_results"))



chronos3=time.time()-chronos3
print(f"Le script 3 a mis {chronos3/60} minutes à tourner")
#%% 4
# Script 4: writing_metro_inputs

# chronos4=time.time()
# res = pk.load(open(os.path.join(SHORT_PATH_DIR,"treated_results"),"rb"))
# no_main=pk.load(open(os.path.join(SHORT_PATH_DIR,"side_results"),"rb"))
# edges = pk.load(open(os.path.join(SHORT_PATH_DIR,"graph_edges"),"rb"))
# nodes = pk.load(open(os.path.join(SHORT_PATH_DIR,"graph_nodes"),"rb"))
# trips= pk.load(open(os.path.join(SHORT_PATH_DIR,"final_results"),"rb"))

nodes,edges = script4.pre_preocess(nodes, edges, trips, savedir = METRO_INPUT_DIR)

main_edges = edges[edges["main_network"]]

road_network = script4.generate_road_network(main_edges)
agents = script4.generate_agents(trips)

script4.write_data(METRO_INPUT_DIR,road_network, agents)

chronos4=time.time()-chronos4



#%% 
# Plot performances
print(f"Le script 0 a mis {chronos0/60} minutes à tourner")
print(f"Le script 1 a mis {chronos1/60} minutes à tourner")
print(f"Le script 2 a mis {chronos2/60} minutes à tourner")
print(f"Le script 3 a mis {chronos3/60} minutes à tourner")
print(f"Le script 4 a mis {chronos3/60} minutes à tourner")
chronos=time.time()-chronos
print(f"Le total des scripts ont mis {chronos/60} minutes à tourner")


# %%
