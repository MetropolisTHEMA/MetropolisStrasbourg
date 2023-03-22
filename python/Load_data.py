# IMPORTS
import os
import sys
import time

import numpy as np
import pandas as pd
import geopandas as gpd
import osmium
from geojson import Point, LineString, Feature, FeatureCollection
from haversine import haversine_vector, Unit
from shapely.ops import linemerge
from shapely.geometry import MultiLineString
from collections import defaultdict
import networkx as nx

# INPUTS
#Timer
chronos=time.time()
chronos0=time.time()


# PARAMETERS

#Setting which links to take into the model or not (source: https://wiki.openstreetmap.org/wiki/FR:Key:highway?uselang=fr)
CONNECTABLE = {
    "motorway" :False ,
    "trunk":False,
    "primary":True ,
    "secondary":True ,
    "motorway_link":False ,
    "trunk_link":False ,
    "primary_link":True ,
    "secondary_link":True ,
    "tertiary":True ,
    "tertiary_link":True ,
    'residential':True ,
    'living_street':True ,
    'unclassified':True ,
    'road': True ,
    'service': True ,
}

# Main network (used inside METRO) and residential network (used for network conection)
MAIN_NETWORK = {
    "motorway": True,
    "trunk": True,
    "primary": True,
    "secondary": True,
    "tertiary": True,
    "unclassified": False,
    "residential": False,
    "motorway_link": True,
    "trunk_link": True,
    "primary_link": True,
    "secondary_link": True,
    "tertiary_link": True,
    "living_street": False,
    "road": True,
    "service": False    
}

# Road type id to use for each highway tag.
ROADTYPE_TO_ID = {
    "motorway": 1,
    "trunk": 2,
    "primary": 3,
    "secondary": 4,
    "tertiary": 5,
    "unclassified": 6,
    "residential": 7,
    "motorway_link": 8,
    "trunk_link": 9,
    "primary_link": 10,
    "secondary_link": 11,
    "tertiary_link": 12,
    "living_street": 13,
    "road": 14,
    "service": 15,
}
# Default number of lanes when unspecified.
DEFAULT_LANES = {
    "motorway": 2,
    "trunk": 2,
    "primary": 1,
    "secondary": 1,
    "tertiary": 1,
    "unclassified": 1,
    "residential": 1,
    "motorway_link": 1,
    "trunk_link": 1,
    "primary_link": 1,
    "secondary_link": 1,
    "tertiary_link": 1,
    "living_street": 1,
    "road": 1,
    "service": 1,
}
# Default speed, in km/h, when unspecified.
DEFAULT_SPEED = {
    "motorway": 130,
    "trunk": 110,
    "primary": 80,
    "secondary": 80,
    "tertiary": 80,
    "unclassified": 30,
    "residential": 30,
    "motorway_link": 90,
    "trunk_link": 70,
    "primary_link": 50,
    "secondary_link": 50,
    "tertiary_link": 50,
    "living_street": 20,
    "road": 50,
    "service": 30,
}


#  NETWORK LOADING INTO METRO FORMAT
def valid_way(way):
    has_access = not "access" in way.tags or way.tags["access"] == "yes"
    return has_access and len(way.nodes) > 1 and way.tags.get("highway") in VALID_HIGHWAYS


class NodeReader(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.all_nodes = set()
        self.nodes = dict()
        self.counter = 0

    def way(self, way):
        if not valid_way(way):
            return
        self.handle_way(way)

    def handle_way(self, way):
        # Always add source and origin node.
        mainet = MAIN_NETWORK.get(way.tags.get("highway"))
        self.add_node(way.nodes[0], mainet)
        self.add_node(way.nodes[-1], mainet)
        self.all_nodes.add(way.nodes[0])
        self.all_nodes.add(way.nodes[-1])
        # Add the other nodes if they were already explored, i.e., they
        # intersect with another road.
        for i in range(1, len(way.nodes) - 1):
            node = way.nodes[i]
            if node in self.all_nodes:
                self.add_node(node, mainet)
            self.all_nodes.add(node)


    def add_node(self, node, main_network):
        if node.ref in self.nodes:
        # Node was already added.    
            if main_network==False:
                return
            else :
            # If the node is from the main network, we make sure the node already added has the right property
                self.nodes[node.ref]["properties"]["main_network"]=True
        if node.location.valid():
            self.nodes[node.ref] = Feature(
                geometry=Point((node.lon, node.lat)),
                properties={"id": self.counter, "osm_id": node.ref, "main_network": main_network},
            )
            self.counter += 1

testf= Feature(
                geometry=Point((2, 3)),
                properties={"id": 12, "osm_id": 123, "main_network": False},
            )

class Writer(osmium.SimpleHandler):
    def __init__(self, nodes):
        super().__init__()
        self.nodes = nodes
        self.edges = list()
        self.counter = 0

    def way(self, way):
        self.add_way(way)

    def add_way(self, way):

        if not valid_way(way):
            return

        road_type = way.tags.get("highway", None)
        road_type_id = ROADTYPE_TO_ID[road_type]

        name = (
            way.tags.get("name", "") or way.tags.get("addr:street", "") or way.tags.get("ref", "")
        )
        if len(name) > 50:
            name = name[:47] + "..."

        oneway = (
            way.tags.get("oneway", "no") == "yes" or way.tags.get("junction", "") == "roundabout"
        )

        # Find maximum speed if available.
        maxspeed = way.tags.get("maxspeed", "")
        speed = None
        back_speed = None
        if maxspeed == "FR:walk":
            speed = 20
        elif maxspeed == "FR:urban":
            speed = 50
        elif maxspeed == "FR:rural":
            speed = 80
        else:
            try:
                speed = float(maxspeed)
            except ValueError:
                pass
        if not oneway:
            try:
                speed = float(way.tags.get("maxspeed:forward", "0")) or speed
            except ValueError:
                pass
            try:
                back_speed = float(way.tags.get("maxspeed:backward", "0")) or speed
            except ValueError:
                pass
        if speed is None:
            speed = DEFAULT_SPEED.get(road_type, 50)
        if back_speed is None:
            back_speed = DEFAULT_SPEED.get(road_type, 50)

        main_network = MAIN_NETWORK.get(way.tags.get("highway"))

        # Find number of lanes if available.
        lanes = None
        back_lanes = None
        if oneway:
            try:
                lanes = int(way.tags.get("lanes", ""))
            except ValueError:
                pass
            else:
                lanes = max(lanes, 1)
        else:
            try:
                lanes = (
                    int(way.tags.get("lanes:forward", "0")) or int(way.tags.get("lanes", "")) // 2
                )
            except ValueError:
                pass
            else:
                lanes = max(lanes, 1)
            try:
                back_lanes = (
                    int(way.tags.get("lanes:backward", "0")) or int(way.tags.get("lanes", "")) // 2
                )
            except ValueError:
                pass
            else:
                back_lanes = max(back_lanes, 1)
        if lanes is None:
            lanes = DEFAULT_LANES.get(road_type, 1)
        if back_lanes is None:
            back_lanes = DEFAULT_LANES.get(road_type, 1)

        for i, node in enumerate(way.nodes):
            if node.ref in self.nodes:
                source = i
                break
        else:
            # No node of the way is in the nodes.
            return

        j = source + 1
        for i, node in enumerate(list(way.nodes)[j:]):
            if node.ref in self.nodes:
                target = j + i
                self.add_edge(
                    way,
                    source,
                    target,
                    oneway,
                    name,
                    road_type_id,
                    lanes,
                    back_lanes,
                    speed,
                    back_speed,
                    main_network
                )
                source = target

    def add_edge(
        self, way, source, target, oneway, name, road_type, lanes, back_lanes, speed, back_speed, main_network
    ):
        source_id = self.nodes[way.nodes[source].ref].properties["id"]
        target_id = self.nodes[way.nodes[target].ref].properties["id"]
        if source_id == target_id:
            # Self-loop.
            return

        # Create a geometry of the road.
        coords = list()
        for i in range(source, target + 1):
            if way.nodes[i].location.valid():
                coords.append((way.nodes[i].lon, way.nodes[i].lat))
        geometry = LineString(coords)
        if not oneway:
            back_geometry = LineString(coords[::-1])

        edge_id = self.counter
        self.counter += 1
        if not oneway:
            back_edge_id = self.counter
            self.counter += 1

        # Compute length in kilometers.
        length = haversine_vector(coords[:-1], coords[1:], Unit.KILOMETERS).sum()

        self.edges.append(
            Feature(
                geometry=geometry,
                properties={
                    "id": edge_id,
                    "name": name,
                    "road_type": road_type,
                    "lanes": lanes,
                    "length": length,
                    "speed": speed,
                    "source": source_id,
                    "target": target_id,
                    "osm_id": way.id,
                    "main_network": main_network
                },
            )
        )

        if not oneway:
            self.edges.append(
                Feature(
                    geometry=back_geometry,
                    properties={
                        "id": back_edge_id,
                        "name": name,
                        "road_type": road_type,
                        "lanes": back_lanes,
                        "length": length,
                        "speed": back_speed,
                        "source": target_id,
                        "target": source_id,
                        "osm_id": way.id,
                        "main_network": main_network
                    },
                )
            )


if __name__ == "__main__":

    
    #Path to the OSM network (.pbf):
    OSM_FILE= r"C:\Users\theot\OneDrive\TheseMaster\Datapipeline\ile-de-france-latest.osm.pbf"


    # OUTPUTS

    #Path to the output directory
    OUTDIR = "./paris_outputs/graphbuilder"
    #Data output prefix (leave as "" if none)
    prefix=""

    #preparing NODE and EDGE files:
    NODE_FILE = os.path.join(OUTDIR,prefix+"raw_nodes")
    EDGE_FILE = os.path.join(OUTDIR,prefix+"raw_edges")

    if not os.path.exists(OUTDIR):
        os.makedirs(OUTDIR)


    # File does not exists or is not in the same folder as the script.
    if not os.path.exists(OSM_FILE):
        print("File not found: {}".format(OSM_FILE))
        sys.exit(0)

    h = NodeReader()

    print("Finding nodes...")
    h.apply_file(OSM_FILE, locations=True, idx="flex_mem")

    g = Writer(h.nodes)

    print("Reading OSM data...")
    g.apply_file(OSM_FILE, locations=True, idx="flex_mem")

    print("Post-processing...")
    post_process(nodes, edges)

    print("Found {} nodes and {} edges.".format(len(nodes), len(edges)))

    if save:
        print("Writing edges...")
        write_edges(edges, EDGE_FILE)

        print("Writing nodes...")
        write_nodes(nodes, NODE_FILE)

    print("Done!")

        
    chronos0=time.time()-chronos0
    print(f"Le script a mis {chronos0/60} minutes à tourner")


