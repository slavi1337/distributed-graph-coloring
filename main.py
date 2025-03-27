import argparse
import json
from datetime import datetime
from pyspark import SparkContext, SparkConf
from graph_generator import GraphGenerator
from graph import Graph
from graph_coloring import distributed_graph_coloring, validate_coloring
from graph_io import GraphLoader

def main(args):
    conf = SparkConf().setAppName("Distributed Graph Coloring and Validation with CLI").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    print()
    
    if args.load_graph:
        try:
            graph = GraphLoader.deserialize(args.load_graph)
            
            if not graph or not graph.get_nodes():  
                raise ValueError("Graph is empty or not forrmated well")

            sample_node = graph.get_nodes()[0]
            if not hasattr(sample_node, "get_node_id") or not hasattr(sample_node, "get_neighbors") or not hasattr(sample_node, "get_color"):
                raise ValueError("Graph format is incorrect")
            
            print(f"Graph with {len(graph.get_nodes())} nodes and {graph.calculate_max_degree()} max degree was successfully \
                    loaded from \033[94m{args.load_graph}\033[0m")

        except Exception as e:
            print(f"Error: Unable to load graph from {args.load_graph}. Make sure file is formatted as following:")
            print("\nExpected format:\n")
            print("node_id")
            print("neighbors (list)")
            print("color\n")
            print(f"Detailed error: {e}")
            exit(1)
    else:
        if args.generate_graph[1] >= args.generate_graph[0]:
            print(f"Error: MAX_DEGREE ({args.generate_graph[1]}) cannot be greater than or equal to NUM_NODES ({args.generate_graph[0]})")
            exit(1)
            
        print(f"Generating a graph with {args.generate_graph[0]} vertecies and max degree of {args.generate_graph[1]}")
        nodes = GraphGenerator.generate_graph(args.generate_graph[0], args.generate_graph[1])
        graph = Graph(nodes)
        
        if args.save_graph:
            GraphLoader.serialize(graph, args.save_graph)
            print(f"Generated graph saved to \033[94m{args.save_graph}\033[0m")

    max_degree = graph.calculate_max_degree()
    max_colors = max_degree + 1 
    
    report = {}
    min_colors_info = {}
    total_time = 0
    
    try:
        node_id_nbrs_rdd = []
        for node in graph.get_nodes():
            node_id_nbrs_rdd.append((node.get_node_id(), node.get_neighbors()))
        node_id_nbrs_rdd = sc.parallelize(node_id_nbrs_rdd)
        
        avg_degree = Graph.calculate_average_degree(node_id_nbrs_rdd)
        
        report["Start time"] = datetime.now().isoformat()
        report["End time"] = datetime.now().isoformat()
        report["CLI Parameters"] = vars(args)
        report["Average degree of the graph"] = avg_degree
        report["Max Degree of the graph"] = max_degree
        
        while max_colors > 0:
            
            time_before = datetime.now()
            colored_graph_rdd = distributed_graph_coloring(node_id_nbrs_rdd, max_colors)
            time_after = datetime.now()
            execution_time = (time_after - time_before).total_seconds()
            total_time += execution_time
            
            if colored_graph_rdd:
                print(f"\t\033[92mGraph colored with {max_colors} colors, ex time {execution_time}s\033[0m")
            print(f"\t\033[92mVALIDATION BEGINS\033[0m")
            
            if validate_coloring(colored_graph_rdd):
                min_colors_info["nodes"] = colored_graph_rdd.collect()
                min_colors_info["colors"] = max_colors
                min_colors_info["execution_time"] = execution_time
                print(f"\t\033[92mVALIDATION Graph colored with {max_colors} colors")
                execution_time_val=(datetime.now() - time_before).total_seconds()
                print(f"\t\tExecution time: {execution_time_val:.2f}s\033[0m")
                max_colors -= 1
            else:
                print(f"\t\033[91mCouldn't color with {max_colors} colors\033[0m")
                break

        if min_colors_info:
            print()
            report["End time"] = datetime.now().isoformat()
            report["Minimum successful colors"] = min_colors_info["colors"]
            report["Min color coloring execution time (s)"] = min_colors_info["execution_time"]
            
            if args.save_colored_graph:
                for node_id, (color, _) in min_colors_info["nodes"]:
                    graph.get_node(node_id).set_color(color)
                GraphLoader.serialize(graph, args.save_colored_graph)
                print(f"Colored graph saved to \033[94m{args.save_colored_graph}\033[0m")
            
    except Exception as ex:
        print(f"\033[91mException occured: {ex}\033[0m")
        report["Exception"] = str(ex)
        
    finally:
        if args.save_results_file:
            report["Total time taken for all the colorings (s)"] = total_time
            with open(args.save_results_file, 'w') as f:
                json.dump(report, f, indent=4)
            print(f"Results saved to \033[94m{args.save_results_file}\033[0m")
        print()
        sc.stop()
        

if __name__ == "__main__":
    
    cli = argparse.ArgumentParser(description="*DISTRIBUTED GRAPH COLORING Command Line Interface (CLI)*")
    
    cli.add_argument("-lg", "--load-graph", help="Input file that containts a graph (can be a graph.json file generated using -g command)")
    cli.add_argument("-gg", "--generate-graph", nargs=2, type=int, metavar=("NUM_NODES", "MAX_DEGREE"), 
                            help="Generate a graph with given parameters NUM_NODES and MAX_DEGREE, instead of loading it from the inpit file. \
                                Average degree of the graph will be close to MAX_DEGREE")
    cli.add_argument("-srf", "--save-results-file", help="Output file that will contain graph coloring report, time taken, graph information and ran CLI commands")
    cli.add_argument("-sg", "--save-graph", help="Output file that the graph(either randomly generated or previously deserialized) will be saved/serialized to")
    cli.add_argument("-scg", "--save-colored-graph", help="Output file that the colored graph(either randomly generated or previously deserialized) will be saved/serialized to. \
                        It will only differ only in colors from -s command")
    
    args = cli.parse_args()
     
    if not (args.load_graph or args.generate_graph):
        cli.error("You must specify either -lg / --load-graph or -gg / --generate-graph")
    if args.load_graph and args.generate_graph:
        cli.error("You cannot specify both -lg / --load-graph and -gg / --generate-graph")
    
    main(args)