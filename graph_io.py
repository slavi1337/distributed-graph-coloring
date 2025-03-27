import json
from node import Node
from graph import Graph

class GraphLoader:
    
    @staticmethod
    def serialize(graph, file):
        nodes_toSer = []
        for v in graph.get_nodes():
            nodes_toSer.append(v.to_map())
            
        with open(file, "w") as serfile:
            json.dump(nodes_toSer, serfile, indent=2)

    @staticmethod
    def deserialize(file):
        with open(file, "r") as deserfile:
            deser_nodes = json.load(deserfile)
        
        reconstructed_nodes = []
        for node_info in deser_nodes:
            reconstructed_nodes.append(Node.from_map(node_info))
        
        graph_deser=Graph(reconstructed_nodes)
        
        return graph_deser

