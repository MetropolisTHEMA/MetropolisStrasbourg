import os

import pickle as pk

import libs.Network as Net
import libs.OD_Matrix as ODM
import libs.Writer as Wr
import libs.Run_Metro as Run

from importlib import reload
reload(Wr)

import yaml
from yaml.loader import Loader as Loader
config = yaml.load(open("CONFIG.yml"), Loader=Loader)
generconfig = config["Generate Inputs"]
writerconfig = config["Write Metropolis Inputs"]

if not os.path.exists("temp"):
    os.mkdir("temp")


if config["Run"]["Generate Inputs"]: 
    generconfig = config["Generate Inputs"]
    
    OUTDIR = generconfig["Generator_outdir"]
    CRS = generconfig["CRS"]

    Network_config =  generconfig["Network import"]

    if Network_config["Import Method"] == "OSM":
        edges, nodes = Net.Load_OSM(OUTDIR, CRS, Network_config)

    OD_config = generconfig["O-D Matrix"]

    TRIP_FILE = OD_config["TRIP_FILE"]
    START_TIME = OD_config["PERIOD"][0]
    END_TIME = OD_config["PERIOD"][1]
    MODES = OD_config["MODES"]
    
    trips = ODM.Load_trips(TRIP_FILE, OUTDIR, START_TIME, END_TIME, MODES, CRS)

    trips = ODM.Connect_OD(OUTDIR, edges, trips, CRS)

    SPconfig = OD_config["ShortetPath"]

    res = ODM.run_shortestpath(edges, trips, SPconfig["ScriptPath"] , SPconfig["Parameters"])

    PPconfig = OD_config["PostProcess"]

    trips = Wr.find_connections(res, edges, trips, OUTDIR, float(PPconfig["Penalty"]))

    if PPconfig["Type"] == "DEFAULT":
         nodes, edges, trips = Wr.default_post_process(nodes, edges, trips, PPconfig["Save"], OUTDIR)
    elif PPconfig["Type"] == "DISCONNECT":
        nodes, edges, trips = Wr.disconnect_post_process(nodes, edges, trips, PPconfig["Save"], OUTDIR)
    else:
        print("This PostProcess in unknown, used DEFAULT as default")
        nodes, edges, trips = Wr.default_post_process(nodes, edges, trips, PPconfig["Save"], OUTDIR)

if config["Run"]["Write Metropolis Inputs"]:
    writerconfig = config["Write Metropolis Inputs"]
    Writer_OUTDIR = writerconfig["Writer_outdir"]   

    if 'From Files' in writerconfig:
        edges, trips = Wr.writer_inputs(writerconfig)
    
    if len(edges) == 0 :
        raise Exception("Empty edges GeoDataFrame, please set the input variable or add 'From Files' in your CONGIF.yaml")
    if len(trips) == 0 :
        raise Exception("Empty trips DataFrame, please set the input variable or add 'From Files' in your CONGIF.yaml")
    
    PARAMETERS, agent_param, network_param = Wr.read_writer_parameters(writerconfig)
    road_network = Wr.generate_road_network(edges[edges.main_network], network_param)
    agents = Wr.generate_agents(trips, agent_param)
    Wr.write_data(Writer_OUTDIR,road_network,agents,PARAMETERS)

if config["Run"]["Run Metropolis"]:
    runconfig = config["Run Metro"]
    Run.run_metro(runconfig)