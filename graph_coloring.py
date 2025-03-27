def largest_id_first_iteration(node_rdd, max_colors):
    print("Start largest_id_first_iteration")
    
    neighbors_info = node_rdd.flatMap(lambda x: [(nbr, (x[0], x[1][0])) for nbr in x[1][1]])

    neighbors_info_grouped = neighbors_info.groupByKey()

    def update_colors(iterator):
        updated_nodes = []

        for node, ((color, neighbors), maybe_nbr_info) in iterator:
            if color != -1:
                updated_nodes.append((node, (color, neighbors)))
                continue

            nbr_list = list(maybe_nbr_info) if maybe_nbr_info else []

            if any(nID > node and nColor == -1 for nID, nColor in nbr_list):
                updated_nodes.append((node, (-1, neighbors)))
                continue

            used_colors = {nColor for _, nColor in nbr_list if nColor != -1}

            for c in range(max_colors):
                if c not in used_colors:
                    updated_nodes.append((node, (c, neighbors)))
                    break
            else:
                updated_nodes.append((node, (-2, neighbors))) 

        return iter(updated_nodes) 

    new_node_rdd = node_rdd.leftOuterJoin(neighbors_info_grouped).mapPartitions(update_colors)

    print("End largest_id_first_iteration")
    return new_node_rdd

def distributed_graph_coloring(node_id_nbrs_rdd, max_colors):
    node_rdd = node_id_nbrs_rdd.map(lambda x: (x[0], (-1, x[1])))

    total_nodes = node_rdd.count()
    print(f"\nColoring starts - number of nodes = {total_nodes}, max_colors = {max_colors}")

    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} ---")
        
        node_rdd = largest_id_first_iteration(node_rdd, max_colors)

        fail_count = node_rdd.filter(lambda x: x[1][0] == -2).count()
        print(f"Number of nodes with color -2: {fail_count}")
        
        if fail_count > 0:
            print(f"Unable to color with {max_colors} colors")
            return None
        
        uncolored_count = node_rdd.filter(lambda x: x[1][0] == -1).count()
        print(f"Uncolored number of nodes: {uncolored_count}")

        if uncolored_count == 0:
            print("Graph colored")
            return node_rdd

def validate_coloring(graph_rdd):
    print("\tPocinje validacija")
    if graph_rdd is None:
        return False
    
    color_rdd = graph_rdd.map(lambda x: (x[0], x[1][0]))
    #print("\tispod colorrdd")
    neighbor_colors = graph_rdd.flatMap(lambda x: [(n, x[1][0]) for n in x[1][1]])
    #print("\tispod nbr colors rdd")
    grouped_neighbor_colors = neighbor_colors.groupByKey().mapValues(list)
    #print("\tispod groupbykey i mapvalues")
    node_with_neighbor_colors = color_rdd.leftOuterJoin(grouped_neighbor_colors)
    #print("\tispod leftouterjoin")
    def check_conflict(node_entry):
        node_id, (color, neighbor_colors_list) = node_entry
        if color == -1:
            return True
        if neighbor_colors_list is None:
            return False
        return color in neighbor_colors_list

    conflicts = node_with_neighbor_colors.filter(check_conflict).count()
    return conflicts == 0