#%% IMPORTS
import os
import sys
import time

import numpy as np
import pandas as pd
import geopandas as gpd
import osmium
from osmium.geom import WKBFactory
from geojson import Point, LineString, Feature, FeatureCollection
from haversine import haversine_vector, Unit
from shapely.ops import linemerge
from shapely.geometry import MultiLineString
from collections import defaultdict
import networkx as nx
from shapely.ops import transform
from shapely.prepared import PreparedGeometry, prep
import pyproj


#%%
def valid_way(way, VALID_HIGHWAYS):
    has_access = not "access" in way.tags or way.tags["access"] == "yes"
    return has_access and len(way.nodes) > 1 and way.tags.get("highway") in VALID_HIGHWAYS

      
def is_urban_area(area):

    URBAN_LANDUSE = [
        # A commercial zone, predominantly offices or services.
        "commercial",
        # An area being built on.
        "construction",
        # An area predominately used for educational purposes/facilities.
        "education",
        # An area with predominantly workshops, factories or warehouses.
        "industrial",
        # An area with predominantly houses or apartment buildings.
        "residential",
        # An area that encloses predominantly shops.
        "retail",
        # A smaller area of grass, usually mown and managed.
        #  "grass",
        # A place where people, or sometimes animals are buried that isn't part of a place of worship.
        #  "cemetery",
        # An area of land artificially graded to hold water.
        #  "basin",
        # Allotment gardens with multiple land parcels assigned to individuals or families for
        # gardening.
        #  "allotments",
        # A village green is a distinctive area of grassy public land in a village centre.
        "village_green",
        # An area designated for flowers.
        #  "flowerbed",
        # An open green space for general recreation, which often includes formal or informal pitches,
        # nets and so on.
        "recreation_ground",
        # Area used for military purposes.
        "military",
        # Denotes areas occupied by multiple private garage buildings.
        "garages",
        # An area used for religious purposes.
        "religious",
    ]
   
    return area.tags.get("landuse") in URBAN_LANDUSE and (area.num_rings()[0] > 0)


class UrbanAreasReader(osmium.SimpleHandler):
    def __init__(self, CRS):
        super().__init__()
        self.wkb_factory = WKBFactory()
        self.areas_wkb = list()
        self.CRS=CRS

    def area(self, area):
        if not is_urban_area(area):
            return
        self.handle_area(area)

    def handle_area(self, area):
        self.areas_wkb.append(self.wkb_factory.create_multipolygon(area))

    def get_urban_area(self):
        polygons = gpd.GeoSeries.from_wkb(self.areas_wkb)
        polygons.crs = "epsg:4326"
        return polygons.to_crs(self.CRS)



class NodeReader(osmium.SimpleHandler):
    def __init__(self, RoadTypes, CRS):
        super().__init__()
        self.all_nodes = set()
        self.nodes = dict()
        self.counter = 0
        self.CRS=CRS
        self.VALID_HIGHWAYS = list(RoadTypes["ROADTYPE"])
        RoadTypes = RoadTypes.set_index("ROADTYPE")
        self.MAIN_NETWORK = RoadTypes["MAIN_NETWORK"]

    def way(self, way):
        if not valid_way(way, self.VALID_HIGHWAYS):
            return
        self.handle_way(way)

    def handle_way(self, way):
        # Always add source and origin node.
        mainet = self.MAIN_NETWORK.get(way.tags.get("highway"))
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

    def post_process(self):
            node_collection = FeatureCollection(
                list(self.nodes.values()),
                crs={"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
            )
            self.nodes = gpd.GeoDataFrame.from_features(node_collection, crs="epsg:4326")
            self.nodes = self.nodes.to_crs(self.CRS)


class EdgeReader(osmium.SimpleHandler):
    def __init__(self, nodes, RoadTypes, CRS):
        super().__init__()
        self.nodes = nodes
        self.edges = list()
        self.counter = 0
        self.CRS=CRS
        self.VALID_HIGHWAYS = list(RoadTypes["ROADTYPE"])
        RoadTypes = RoadTypes.set_index("ROADTYPE")
        self.ROADTYPE_TO_ID = RoadTypes["ID"]
        self.MAIN_NETWORK = RoadTypes["MAIN_NETWORK"]
        self.DEFAULT_LANES = RoadTypes["DEFAULT_LANES"]
        self.DEFAULT_SPEED_URBAN = RoadTypes["DEFAULT_URBAN_SPEED"]
        self.CAPACITY = RoadTypes["CAPACITY"]
        self.CONNECTABLE = RoadTypes["CONNECTABLE"]
        self.DEFAULT_SPEED_RURAL = RoadTypes["DEFAULT_RURAL_SPEED"]

    def way(self, way):
        self.add_way(way)

    def add_way(self, way):

        if not valid_way(way, self.VALID_HIGHWAYS):
            return

        road_type = way.tags.get("highway", None)
        road_type_id = self.ROADTYPE_TO_ID[road_type]

        name = (
            way.tags.get("name", "") or way.tags.get("addr:street", "") or way.tags.get("ref", "")
        )
        name = way.tags.get("ref", "")
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
            lanes = self.DEFAULT_LANES.get(road_type, 1)
        if back_lanes is None:
            back_lanes = self.DEFAULT_LANES.get(road_type, 1)

        capacity = self.CAPACITY.get(road_type)

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
                    capacity,
                )
                source = target

    def add_edge(
        self,
        way,
        source,
        target,
        oneway,
        name,
        road_type,
        lanes,
        back_lanes,
        speed,
        back_speed,
        capacity,
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
        back_geometry = None
        if not oneway:
            back_geometry = LineString(coords[::-1])

        edge_id = self.counter
        self.counter += 1
        back_edge_id = None
        if not oneway:
            back_edge_id = self.counter
            self.counter += 1

        # Compute length in meters.
        length = np.sum(haversine_vector(coords[:-1], coords[1:], Unit.KILOMETERS)) * 1000

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
                    "capacity": capacity,
                    "source": source_id,
                    "target": target_id,
                    "osm_id": way.id,
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
                        "capacity": capacity,
                        "source": target_id,
                        "target": source_id,
                        "osm_id": way.id,
                    },
                )
            )

    def post_process(self, urban_area: PreparedGeometry):
            edge_collection = FeatureCollection(self.edges)
            edges = gpd.GeoDataFrame.from_features(edge_collection, crs="epsg:4326")
            edges = edges.to_crs(self.CRS)
            
            # Flag the highways that we want to keep in the final graph.
            main_highways = self.ROADTYPE_TO_ID[self.MAIN_NETWORK[self.MAIN_NETWORK].index]
            edges["main_network"] = edges["road_type"].isin(main_highways)

            # Flag the highways that cannot be used as OD.
            od_forbidden = self.ROADTYPE_TO_ID[self.CONNECTABLE[ self.CONNECTABLE ].index]
            edges["allow_od"] = edges["road_type"].isin(od_forbidden)

            # Set speed of edges to default speed if NA.
            edges["urban"] = [urban_area.contains(geom) for geom in edges.geometry]
            ids = self.ROADTYPE_TO_ID.reset_index().drop(columns="ROADTYPE")
            urban_speeds = self.DEFAULT_SPEED_URBAN.reset_index().drop(columns="ROADTYPE")
            rural_speeds = self.DEFAULT_SPEED_RURAL.reset_index().drop(columns="ROADTYPE")
            default_speeds = pd.concat((ids, urban_speeds, rural_speeds), axis=1).set_index("ID")
            edges = edges.merge(default_speeds, left_on="road_type", right_index=True, how="left")
            edges.loc[edges["speed"].isna() & edges["urban"], "speed"] = edges["DEFAULT_URBAN_SPEED"]
            edges.loc[edges["speed"].isna() & ~edges["urban"], "speed"] = edges["DEFAULT_RURAL_SPEED"]

            print("Number of edges: {}".format(len(edges)))
            print("Number of edges in main graph: {}".format(edges["main_network"].sum()))

            edges = edges[
                [
                    "geometry",
                    "source",
                    "target",
                    "length",
                    "speed",
                    "lanes",
                    "main_network",
                    "allow_od",
                    "capacity",
                    "osm_id",
                    "name",
                    "road_type",
                ]
            ]

            self.edges = edges

def count_neighbors(edges):
    in_neighbors = edges.groupby(["target"])["source"].unique()
    out_neighbors = edges.groupby(["source"])["target"].unique()
    node_neighbors = pd.DataFrame({"in": in_neighbors, "out": out_neighbors})

    def merge_lists(row):
        if row["in"] is np.nan:
            in_set = set()
        else:
            in_set = set(row["in"])
        if row["out"] is np.nan:
            out_set = set()
        else:
            out_set = set(row["out"])
        return in_set.union(out_set)

    node_neighbors = node_neighbors.apply(merge_lists, axis=1)
    neighbor_counts = node_neighbors.apply(lambda x: len(x))
    neighbor_counts.name = "neighbor_count"
    edges = edges.merge(neighbor_counts, how="left", left_on="target", right_index=True)
    assert not edges["neighbor_count"].isna().any()
    return edges

def keep_strongly_connected(nodes, edges):
    G = nx.DiGraph()
    G.add_edges_from(
        map(
            lambda f: (f["properties"]["source"], f["properties"]["target"], f["properties"]),
            edges.iterfeatures(),
        )
    )
    # Find the nodes of the largest weakly connected component.
    connected_nodes = max(nx.strongly_connected_components(G), key=len)
    if len(connected_nodes) < G.number_of_nodes():
        print(
            "Warning: discarding {} nodes disconnected from the main graph".format(
                G.number_of_nodes() - len(connected_nodes)
            )
        )
        G.remove_nodes_from(set(G.nodes).difference(connected_nodes))
        edges = edges.loc[edges["source"].isin(connected_nodes)]
        edges = edges.loc[edges["target"].isin(connected_nodes)]
        nodes = nodes[nodes["id"].isin(connected_nodes)]
    return nodes, edges

def keep_weakly_connected(nodes, edges):
    G = nx.DiGraph()
    G.add_edges_from(
        map(
            lambda f: (f["properties"]["source"], f["properties"]["target"], f["properties"]),
            edges.iterfeatures(),
        )
    )
    # Find the nodes of the largest weakly connected component.
    connected_nodes = max(nx.weakly_connected_components(G), key=len)
    if len(connected_nodes) < G.number_of_nodes():
        print(
            "Warning: discarding {} nodes disconnected from the main graph".format(
                G.number_of_nodes() - len(connected_nodes)
            )
        )
        G.remove_nodes_from(set(G.nodes).difference(connected_nodes))
        edges = edges.loc[edges["source"].isin(connected_nodes)]
        edges = edges.loc[edges["target"].isin(connected_nodes)]
        nodes = nodes[nodes["id"].isin(connected_nodes)]
    return nodes, edges

def remove_duplicate(edges):
    print("Removing duplicate edges.")
    st_count = edges.groupby(['source', 'target'])['id'].count()
    to_remove = set()
    for s, t in st_count.loc[st_count > 1].index:
        dupl = edges.loc[(edges['source'] == s) & (edges['target'] == t)]
        # Keep only the edge with the smallest travel time.
        tt = dupl["traveltime"]
        id_min = tt.index[tt.argmin()]
        for i in dupl.index:
            if i != id_min:
                to_remove.add(i)
    if to_remove:
        print('Warning. Removing {} duplicate edges.'.format(len(to_remove)))
        edges.drop(labels=to_remove, inplace=True)
    
    return edges

def reindex(nodes, edges="", trips=""): 
    """
    Function to reindex nodes and/or edges and/or trips, as Metropolis needs all the indexes to be in a row.
    Both edges and trips are optional
    - will reindex the new nodes indexes into edges["source"] and edges["target"]
    - for trips, put trips[["columns refering node index"]] as input, as assign it to trips[["columns refering node index"]] accordingly
    """      
    # Node ande edge ids should start at 0.
    print("Index reset")
    nodes.reset_index(drop=True, inplace=True)
    node_id_map = nodes["id"].to_frame().reset_index().set_index("id")
    nodes["id"] = nodes.index
    output = ( nodes, )

    if len(edges) != 0:
        edges.reset_index(drop=True, inplace=True)
        edges["id"]=edges.index
        #edges.drop(columns="id", inplace=True)
        edges = edges.merge(node_id_map, left_on="source", right_index=True).drop(columns=["source"]).rename(
            columns={"index": "source"}
        )
        edges = edges.merge(node_id_map, left_on="target", right_index=True).drop(columns=["target"]).rename(
            columns={"index": "target"}
        ).sort_index()
        output = output + (edges , )

    if len(trips) != 0:
        trips = trips.merge(node_id_map, left_on="O_connect", right_index=True).drop(columns=["O_connect"]).rename(
            columns={"index": "O_connect"}
        )
        trips = trips.merge(node_id_map, left_on="D_connect", right_index=True).drop(columns=["D_connect"]).rename(
            columns={"index": "D_connect"}
        ).sort_index()
        output = output + (trips, )

    return output

def write_edges(edges, OUTDIR, filename, driver="", crs="epsg:2154"):
    edges = edges.to_crs(crs)
    edges.to_pickle("temp/"+filename)
    if driver != "": 
        edges.to_file(os.path.join(OUTDIR,filename+"."+driver), driver=driver, crs=crs, encoding='utf-8')

def write_nodes(nodes, OUTDIR, filename, driver="" , crs="epsg:2154"):
    nodes.to_crs(crs, inplace=True)
    nodes.to_pickle("temp/"+filename)
    if driver != "": 
        nodes.to_crs("epsg:2154").to_file(os.path.join(OUTDIR,filename+"."+driver), driver=driver, crs="epsg:2154", encoding='utf-8')

def post_process(nodes, edges):
    nodes["id"]=nodes.index
    edges["id"]=edges.index

    nodes, edges = keep_strongly_connected(nodes, edges)

    edges["traveltime"]= edges['length'] / (edges['speed'] / 3.6)

    edges = count_neighbors(edges)

    edges = remove_duplicate(edges)

    nodes = nodes.sort_values(by=["main_network"], ascending = False)
    edges = edges.sort_values(by=["main_network"], ascending = False)

    nodes, edges = reindex(nodes, edges)

    return nodes, edges


def Load_OSM(OUTDIR, CRS, Network_config):
    """"
    Reads OSM_FILE, RoadTypes.csv, CRS, Buffer, Save and Driver from Network Config part
    Uses OSMIUM library to read the PBF File, and save 
    """
    OSM_FILE = Network_config["OSM_FILE"]
    RoadTypes = pd.read_csv(Network_config["Road_import_info.csv"], sep=Network_config["sep"])
    UA_BUFFER=Network_config["OSM zone buffer"]
    save =  Network_config["Save"]
    driver= Network_config["Driver"]

    t0 = time.time()

    if not os.path.exists(OUTDIR):
        os.makedirs(OUTDIR)

    NODE_FILE = "raw_nodes"
    EDGE_FILE = "raw_edges"

    # File does not exists or is not in the same folder as the script.
    if not os.path.exists(OSM_FILE):
        print("File not found: {}".format(OSM_FILE))
        sys.exit(0)

    print("Finding nodes...")
    node_reader = NodeReader(RoadTypes, CRS=CRS)
    node_reader.apply_file(OSM_FILE, locations=True, idx="flex_mem")

    print("Reading edges...")
    edge_reader = EdgeReader(node_reader.nodes, RoadTypes, CRS=CRS)
    edge_reader.apply_file(OSM_FILE, locations=True, idx="flex_mem")

    print("Finding urban areas...")
    area_reader = UrbanAreasReader(CRS=CRS)
    area_reader.apply_file(OSM_FILE, locations=True, idx="flex_mem")
    urban_area = area_reader.get_urban_area()

    # Buffer the urban areas by X meters to capture all nearby roads.
    urban_area = urban_area.buffer(float(UA_BUFFER)).simplify(0, preserve_topology=False).unary_union
    urban_area = prep(urban_area)

    print("Attaching rural/urban speed limits...")
    node_reader.post_process()
    edge_reader.post_process(urban_area)

    print("Post-processing...")
    #Returns treated GeoDataFrames
    nodes, edges = post_process(nodes, edges)
    
    print("Found {} nodes and {} edges.".format(len(nodes), len(edges)))

    if save:
        print("Writing edges...")
        write_edges(edges, OUTDIR, EDGE_FILE, driver, CRS)

        print("Writing nodes...")
        write_nodes(nodes, OUTDIR, NODE_FILE, driver, CRS)

    print("Done!")

    print("Total running time: {:.2f} minutes".format((time.time() - t0)/60))

    return edges, nodes

    


        
    
    
# %%
