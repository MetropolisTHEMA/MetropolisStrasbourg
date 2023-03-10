import os, sys, time
import json
import subprocess

import numpy as np
import pandas as pd
import geopandas as gpd
import pickle as pk
from shapely.geometry import Point, LineString

#use the right projection for metric operations
METRIC_CRS = "EPSG:2154"

# Script location
scriptdir = r"C:\Users\theot\OneDrive\TheseMaster\Datapipeline\ShortestPath"
TCHscript = "compute_travel_times"


# Parametrer file: parameters.json -> write manually

parameters = {
  "algorithm": "TCH",
  "output_route": True,
}


#Dir check

#Main


# #load edges 
# print("Reading edges")
# try:
#     edges = pk.load(open(edgefile,"rb"))
#     edges.crs = METRIC_CRS
#     edges.set_geometry("geometry", inplace = True)
# except: 
#     edges = gpd.read_file(edgefile+"geojson").to_crs(METRIC_CRS)
#     edges.to_pickle(edgefile)

#load nodes
def prepare_inputs(outdir, tripfile, edgefile, nodefile, save=False, crs=METRIC_CRS):
    print("Reading edges node")
    try:
        edges = pk.load(open(os.path.join(outdir,"graph_edges"),"rb"))
        nodes = pk.load(open(os.path.join(outdir,"graph_nodes"),"rb"))
        trips = pk.load(open(os.path.join(outdir,"graph_trips"),"rb"))
    except: 
        print("Reindexing needed")

        print("Reading trips")
        try:
            trips = pk.load(open(tripfile,"rb"))
            trips = pd.DataFrame(trips)
        except: 
            trips = pd.read_csv(tripfile+".csv")
        
        print("Reading edges")
        try:
            edges = pk.load(open(edgefile,"rb"))
            edges.crs = METRIC_CRS
            edges.set_geometry("geometry", inplace = True)
        except: 
            edges = gpd.read_file(edgefile+"geojson").to_crs(METRIC_CRS)
            edges.to_pickle(edgefile)

        print("Reading nodes")
        try:
            nodes = pk.load(open(nodefile,"rb"))
            nodes.crs = METRIC_CRS
            nodes.set_geometry("geometry", inplace = True)
        except: 
            nodees = gpd.read_file(nodefile+"geojson").to_crs(METRIC_CRS)
            nodes.to_pickle(nodefile)

        # Removing duplicate edges.
        st_count = edges.groupby(['source', 'target'])['id'].count()
        to_remove = set()
        for s, t in st_count.loc[st_count > 1].index:
            dupl = edges.loc[(edges['source'] == s) & (edges['target'] == t)]
            # Keep only the edge with the smallest travel time.
            tt = dupl['length'] / (dupl['speed'] / 3.6)
            id_min = tt.index[tt.argmin()]
            for i in dupl.index:
                if i != id_min:
                    to_remove.add(i)
        if to_remove:
            print('Warning. Removing {} duplicate edges.'.format(len(to_remove)))
            edges.drop(labels=to_remove, inplace=True)
                
        # Node ande edge ids should start at 0.
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
        trips = trips.merge(node_id_map, left_on="O_connect", right_index=True).drop(columns=["O_connect"]).rename(
            columns={"index": "O_connect"}
        )
        trips = trips.merge(node_id_map, left_on="D_connect", right_index=True).drop(columns=["D_connect"]).rename(
            columns={"index": "D_connect"}
        ).sort_index()


        edges["traveltime"]=(edges["length"]/edges["speed"])*3600
        
        print("Save files")
        edges.to_crs(METRIC_CRS, inplace=True)
        if save:
            edges.to_file(os.path.join(outdir,"graph_edges.geojson"), driver="GeoJSON")
        edges.to_pickle(os.path.join(outdir,"graph_edges"))

        nodes.to_crs(METRIC_CRS, inplace=True)
        if save:
            nodes.to_file(os.path.join(outdir,"graph_nodes.geojson"), driver="GeoJSON")
        nodes.to_pickle(os.path.join(outdir,"graph_nodes"))

        trips.to_pickle(os.path.join(outdir,"graph_trips"))
    return trips, edges, nodes


def prepare_shortestpath(outdir, trips, edges, parameters=parameters):
    print("Setting query")
    trips[['departime']]=0
    query = trips[["trip_index","O_connect","D_connect","departime"]].values.tolist()
    print("Creating Graph")
    # c = time.time()
    # graph = edges.apply(lambda row :[ row["source"], row["target"], row["traveltime"]], axis=1)
    # c = time.time()-c

    # graph = edges[["source","target","traveltime"]].tolist()
    e = edges[["source","target","traveltime"]]
    graph = [*map(list, zip(*map(e.get, e)))]

    print("Writing data...")
    print("Graph")
    with open(outdir+"/graph.json", "w") as f:
        f.write(json.dumps(graph))
    print("Parameters")
    with open(outdir+"/parameters.json", "w") as f:
        f.write(json.dumps(parameters))
    print("Queries")
    with open(outdir+"/queries.json", "w") as f:
        f.write(json.dumps(query))
    
    print("Done!")


def run_shortestpath(outdir, scriptdir, TCHscript):
    
    #Inputs of the shortestpath script

            #Path to the file where the queries to compute are stored
    q=  f"--queries  {outdir}/queries.json "
            #Path to the file where the graph is stored
    g=  f"--graph {outdir}/graph.json "
            #Path to the file where the results of the queries should be stored
    o=  f"--output {outdir}/output.json "
            #Path to the file where the parameters are stored 
    p=  f"--parameters {outdir}/parameters.json "

    #Run TCH script
    print("Run TCH script")
    #add ".exe" if on windows
    syst = ""
    if sys.platform == "win32":
        syst=".exe"


    command = f"{os.path.join(scriptdir,TCHscript)}{syst} {q} {p} {o} {g} " 

    subprocess.run(command, shell=True)
    print(f"Saved in {outdir}")

if __name__=="__main__":
        
    p="paris_"

    chronos2=time.time()

    # Input files
    tripfile = p+"outputs/OD_to_network/trips_OD"
    edgefile = p+"outputs/graphbuilder/raw_edges"
    nodefile = p+"outputs/graphbuilder/raw_nodes"
    #output directory
    outdir = p+"outputs/shortestspath"
    if not os.path.isdir(outdir):
        os.makedirs(outdir)


    chronos2=time.time()

    trips, edges = prepare_inputs(outdir, tripfile, edgefile, nodefile, crs=METRIC_CRS)

    prepare_shortestpath(trips, edges, parameters=parameters)

    run_shortestpath(outdir, scriptdir, TCHscript)

    chronos2=time.time()-chronos2
    print(f"Le script a mis {chronos2/60} minutes à tourner")

