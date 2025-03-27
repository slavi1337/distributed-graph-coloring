from node import Node

class Graph:
    def __init__(self, nodes=None):
        if nodes is None:
            self._nodes = []
        else:
            self._nodes = nodes

    def get_nodes(self):
        return self._nodes

    def set_nodes(self, new_nodes):
        if new_nodes is None:
            self._nodes = []
        else:
            self._nodes = new_nodes

    def add_node(self, node):
        self._nodes.append(node)

    def get_node(self, node_id):
        return next((nd for nd in self._nodes if nd.get_node_id() == node_id), None)

    def calculate_max_degree(self):
        if len(self._nodes) == 0:
            return 0
        
        max_deg = 0
        for nd in self._nodes:
            deg = len(nd.get_neighbors())
            if deg > max_deg:
                max_deg = deg
        return max_deg

    @staticmethod
    def calculate_average_degree(graph_rdd):
        if graph_rdd.isEmpty():
            return 0 
        
        node_degrees_rdd = graph_rdd.map(lambda x: len(x[1])) 
        total_degree = node_degrees_rdd.reduce(lambda a, b: a + b)
        node_count = graph_rdd.count() 

        return total_degree / node_count if node_count > 0 else 0
