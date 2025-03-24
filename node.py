# Node that is used for Graph
class Node:
    def __init__(self, node_id, neighbors=None, color=-1):
        self._node_id = node_id
        self._color = color
        if neighbors is None:
            self._neighbors = []
        else:
            self._neighbors = list(set(neighbors))

    def get_node_id(self):
        return self._node_id

    def set_node_id(self, node_id):
        self._node_id = node_id

    def get_neighbors(self):
        return self._neighbors

    def set_neighbors(self, neighbors):
        if neighbors is None:
            self._neighbors = []
        else:
            self._neighbors = list(set(neighbors))

    def add_neighbor(self, new_neighbor_id):
        if new_neighbor_id == self._node_id:
            return
        if new_neighbor_id not in self._neighbors:
            self._neighbors.append(new_neighbor_id)
            
    def get_color(self):
        return self._color

    def set_color(self, color):
        self._color = color

    def to_dict(self):
        node_data = {}
        node_data["node_id"] = self._node_id
        node_data["neighbors"] = self._neighbors
        node_data["color"] = self._color
        return node_data

    @classmethod
    def from_dict(cls, raw_data):
        id1 = raw_data.get("node_id")
        neigbors1 = raw_data.get("neighbors", [])
        color1 = raw_data.get("color", -1)
        
        return cls(id1, neigbors1, color1)