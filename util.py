from datetime import datetime

def measure_execution_time():
    start_time = datetime.now()
    # algo
    end_time = datetime.now()
    
    execution_time = (end_time - start_time).total_seconds()
    print(f"Ex time: {execution_time:.4f} (s)")