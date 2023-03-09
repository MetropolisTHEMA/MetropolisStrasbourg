import os, sys, time
import json
from matplotlib import pyplot as plt

from itertools import compress
import numpy as np
import pandas as pd
import geopandas as gpd
import pickle as pk
#library to replace apply with progress_apply and prompt the progress of the pandas operation
from tqdm import tqdm
tqdm.pandas()


chronos3=time.time()

def load_results(result):
      with open(result, 'r') as f:
        data = json.load(f)

      res = data["results"]

      clean_res = pd.DataFrame(list(filter(None,res)), columns=["id", "travel_time", "route"]).set_index("id")
      print(f"Warning: discarded {len(res)-len(clean_res) } items ")
      return clean_res

def find_connections(clean_res, edges, penalty=5):
      print("Prepare edges data")
      valid_edges=set(edges[edges["main_network"]]["id"])
      edges=edges.set_index("id")
      edge_time=edges.traveltime.to_dict()
      edge_source=edges.source.to_dict()
      edge_target=edges.target.to_dict()

      clean_res["id"] = clean_res.index

      print("Find the valid edges on the route")
      clean_res["main_part"] = clean_res.route.progress_apply(lambda l : [x for x in l if x in valid_edges])
      print("Mask out the main / residential edges")
      clean_res["mask"] = clean_res.route.progress_apply(lambda l : [x in valid_edges for x in l ])
      print("Extract the position of each edges")
      clean_res["mask"] = clean_res["mask"].progress_apply(lambda l : list(compress(range(len(l)), l)))
      print("Keep first and last")
      clean_res["mask"] = clean_res["mask"].progress_apply(lambda l : [l[0],l[-1]] if len(l)!=0 else 0)
      print("Put trips that never use the main network on the side")
      no_main = clean_res[clean_res["mask"]==0]
      print(f"{len(no_main)} trips discarded : no travel on the main network")
      clean_res = clean_res[clean_res["mask"]!=0]

      print("extract connection edges and nodes into O_edge, D_edge, O_node and D_node columns")
      clean_res["O_edge"]=clean_res.main_part.progress_apply(lambda l : l[0] )
      clean_res["D_edge"]=clean_res.main_part.progress_apply(lambda l : l[-1])
      clean_res["O_node"]=clean_res.O_edge.progress_apply(lambda o : edge_source[o])
      clean_res["D_node"]=clean_res.D_edge.progress_apply(lambda d : edge_target[d])
      #clean_res["O_node"]=clean_res.O_edge.progress_apply(lambda o : int(edges[edges["id"]==o]["source"].values) )
      #clean_res["D_node"]=clean_res.D_edge.progress_apply(lambda d : int(edges[edges["id"]==d]["target"].values))
      
      print("Trim the acces trips to the main network, save them on the side")
      start = clean_res["mask"].apply(lambda l : l[0]).to_dict()
      finish = clean_res["mask"].apply(lambda l : l[-1]).to_dict()
      print("start_access:")
      clean_res["start_access"]=clean_res.progress_apply(lambda l: l.loc["route"][:start[l.loc["id"]]] , axis=1 )
      print("finish_access:")
      clean_res["finish_access"]=clean_res.progress_apply(lambda l: l.loc["route"][finish[l.loc["id"]]+1:] , axis=1 )
      print("main_route:")
      clean_res["only_main"] = clean_res.progress_apply(lambda l: l.loc["route"][start[l.loc["id"]]:finish[l.loc["id"]]+1] , axis=1 )
      print("Find how many residential edges are used in the middle of main trips")
      clean_res["has_residential"] = clean_res.apply( lambda l : len(l.loc["only_main"])-len(l.loc["main_part"]), axis=1)

      print("Compute access times")
      print("start_access_time:")
      clean_res["start_access_time"] = clean_res.progress_apply(lambda l : np.sum([edge_time[x]+penalty for x in l.loc["start_access"]]), axis=1)
      print("finish_access_time:")
      clean_res["finish_access_time"] = clean_res.progress_apply(lambda l : np.sum([edge_time[x]+penalty for x in l.loc["finish_access"]]), axis=1)

      # clean_res["main_connect_nodes"]=clean_res.progress_apply(lambda l: [edge_source[l["mask"][0]],edge_target[l["mask"][-1]]], axis=1)
      return clean_res, no_main

def final_trip_file(res, no_main, trips):
      res["road_leg"]=True
      no_main["road_leg"]=False
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
      return trips

if __name__=="__main__":
      
      chronos3=time.time()

      outdir = "strasbourg_outputs/Shortest_Path"

      edges = pk.load(open(os.path.join(outdir,"graph_edges"), "rb")).set_index("id")

      result=os.path.join(outdir,"output.json")  

      clean_res = load_results(result)

      clean_res, no_main = find_connections(clean_res, edges)

      count_O=clean_res[["route","O_node"]].groupby("O_node").count()
      count_D=clean_res[["route","D_node"]].groupby("D_node").count()

      connectors=set(count_O.index.append(count_D.index))

      chronos3=time.time()-chronos3
      print(f"Le script 3 a mis {chronos3/60} minutes à tourner")
      