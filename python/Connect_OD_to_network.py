import os, sys, time
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import time

import pickle as pk
from tqdm import tqdm
tqdm.pandas()

chronos=time.time()
chronos1A=time.time()

# Returns only trips whose mode is within the following modes
# (available values: car, car_passenger, pt, walk, bike).
MODES = ("car", "car_passenger", "pt", "walk", "bike")
# Returns only trips whose departure time is later than this value (in seconds after midnight).
START_TIME = 3.0 * 3600.0
# Returns only trips whose arrival time is earlier than this value (in seconds after midnight).
END_TIME = 10.0 * 3600.0
#use the right projection for metric operations
METRIC_CRS = "EPSG:2154"
WORLD_CRS = "EPSG:4326"

#Selection of the desired roadtypes from this table
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


#selecting the trips interesting for us
def prepare_trips(tripspath, outputdirpath, START_TIME=START_TIME, END_TIME=END_TIME):
    print("Reading trips")
    try:
        trips = pk.load(open(os.path.join(outputdirpath, "car_time_trips"),"rb"))
        #trips = gpd.GeoDataFrame(trips)
        # trips.crs = METRIC_CRS
        # trips.set_geometry("geometry", inplace=True)
    except: 
        if tripspath.endswith('csv'):
            trips = pd.read_csv(tripspath)
            trips = trips.loc[
                trips['mode'].isin(MODES)
                & (trips['departure_time'] >= START_TIME)
                & (trips['arrival_time'] <= END_TIME)
            ]
            trips = gpd.GeoDataFrame(trips)
            print("Creating geometries")
            trips['origin'] = gpd.GeoSeries.from_xy(trips['x0'], trips['y0'], crs=METRIC_CRS)
            trips['destination'] = gpd.GeoSeries.from_xy(trips['x1'], trips['y1'], crs=METRIC_CRS)
        else:
            trips = gpd.read_file(
                tripspath,
                where=f"mode IN {MODES} AND departure_time BETWEEN {START_TIME} AND {END_TIME} "
            )
            trips.to_crs(METRIC_CRS, inplace=True)
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
        trips.to_pickle(os.path.join(outputdirpath, "car_time_trips"))
            
    #separation of Origin and Destination gdf:
    O = gpd.GeoDataFrame(trips[["trip_index"]],geometry=trips["origin"])
    D = gpd.GeoDataFrame(trips[["trip_index"]],geometry=trips["destination"])

    return trips, O, D

def read_edges(edgespath, outputdirpath):
    #load edges 
    print("Reading edges")
    try:
        edges = pk.load(open(edgespath,"rb"))
        edges.to_crs(METRIC_CRS, inplace=True)
    except: 
        edges = gpd.read_file(edgespath+".geojson").to_crs(METRIC_CRS)

    return edges




# gdf=D
def nearjoin(gdf, edges, sheetname, CONNECTED_ROADTYPES=CONNECTED_ROADTYPES ,save=False):
    edges = edges[edges["road_type"].isin(CONNECTED_ROADTYPES.values())]

    edges["source_pt"]=edges["geometry"].apply(lambda g:Point( g.coords[0]))
    edges["target_pt"]=edges["geometry"].apply(lambda g:Point( g.coords[-1]))

    print(sheetname+ ":\n   Join nearest edges...")
    nearedges=gpd.sjoin_nearest(gdf.to_crs(METRIC_CRS), 
                                edges[["id","source", "source_pt","target","target_pt","road_type", "geometry"]].to_crs(METRIC_CRS),
                                distance_col="edge_dist", how="inner")
    nearedges = nearedges.drop_duplicates(subset=["trip_index"])

    print("   Compute distance node ...")
    #set source / target point distance
    print("     sources")
    nearedges["source_dist"]=nearedges.progress_apply(lambda e :e["geometry"].distance(e["source_pt"]), axis=1)
    print("     targets")
    nearedges["target_dist"]=nearedges.progress_apply(lambda e :e["geometry"].distance(e["target_pt"]), axis =1)
    # True for source closer, False fot target
    print("    Find nearest node...")
    nearedges["nearst"] = nearedges.progress_apply(lambda e :  (e["source_dist"]) < (e["target_dist"])  , axis=1)
    #set connector value acconrdingly
    print(    "Assignating values:")
    print("-when source is near")
    nearsource = nearedges.loc[nearedges["nearst"]==True]
    nearsource["connect"] = nearsource["source"]
    nearsource["connect_dist"] = nearsource["source_dist"]
    nearsource["geometry"] = nearsource["source_pt"]
    print("-when target is near")
    neartarget = nearedges.loc[nearedges["nearst"]==False]
    neartarget["connect"] = neartarget["target"]
    neartarget["connect_dist"] = neartarget["target_dist"]
    neartarget["geometry"] = neartarget["target_pt"]
    print("Merging")
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
    if save:
        print("   Save file : "+sheetname+".geojson")
        nearedges.to_file(sheetname+".geojson", driver="GeoJSON")
        nearedges.to_pickle(sheetname)
    print("Done!")

    return nearedges


if __name__ == "__main__":
    chronos1A=time.time()

        
    #tripspath = r"D:\OneDrive\TheseMaster\Data\Equasim\output_100%\strasbourg_trips.gpkg"
    #tripspath = r"C:\Users\theot\OneDrive\TheseMaster\Data\Equasim\output_100%\strasbourg_trips.gpkg"
    tripspath = r"C:\Users\theot\OneDrive\TheseMaster\Data\Equasim\ile_de_france_trips.gpkg"

    edgespath = "paris_outputs/graphbuilder/raw_edges"

    outputdirpath = "paris_outputs/OD_to_network/"

    if not os.path.isdir(outputdirpath):
        os.makedirs(outputdirpath)

    trips, O , D = prepare_trips(tripspath)
    edges=read_edges(edgespath, outputdirpath)

    nearO = nearjoin(O, edges, outputdirpath+"Originset")
    nearD = nearjoin(D, edges, outputdirpath+"Destinationset")

    trips=trips.join(nearO[["connect","connect_dist","edge_dist"]].add_prefix("O_"), how="inner", on="trip_index")
    trips=trips.join(nearD[["connect","connect_dist","edge_dist"]].add_prefix("D_"), how="inner", on="trip_index")

    #delete trips with same O and D
    trips=trips[trips["O_connect"]!=trips["D_connect"]]

    #trips.to_csv(outputdirpath+"trips_OD.csv")
    trips.to_pickle(outputdirpath+"trips_OD")


    chronos1A=time.time()-chronos1A
    print(f"Le script a mis {chronos1A/60} minutes à tourner")
