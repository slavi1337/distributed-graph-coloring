import random
from node import Node

class GraphGenerator:
    
    @staticmethod
    def generate_graph(num_nodes, max_degree):
        generated_nodes = []
        for i in range(num_nodes):
            node = Node(i)
            generated_nodes.append(node)

        # Set 0- numNodes-1
        all_nodes = set(range(num_nodes))
        
        for i in range(num_nodes):
            current_node = generated_nodes[i]
            
            available_neighbors = []
            for j in all_nodes - {i}: 
                if len(generated_nodes[j].get_neighbors()) < max_degree:
                    available_neighbors.append(j)

            random.shuffle(available_neighbors)

            num_to_add = min(max_degree - len(current_node.get_neighbors()), len(available_neighbors))
            for j in available_neighbors[:num_to_add]:
                if len(generated_nodes[j].get_neighbors()) < max_degree: 
                    current_node.add_neighbor(j)
                    generated_nodes[j].add_neighbor(i)
        
        for node in generated_nodes:
            if len(node.get_neighbors()) > max_degree:
                print(f"AAAAAAA NODE {node.get_node_id()} ima vise od {max_degree} nbrs")
        
        return generated_nodes