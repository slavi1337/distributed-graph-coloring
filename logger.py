# logger.py
# use instead of print, beacuse of messy output 
def log_message(message):
    with open("izlaz.txt", "a") as log_file:
        log_file.write(message + "\n")
