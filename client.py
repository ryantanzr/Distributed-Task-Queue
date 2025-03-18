from tasks import extract_data, chained_tasks, grouped_tasks
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    
    # # Example 1: Trigger a single task (extract_data)
    # logging.info("Triggering extract_data task...")
    # extract_data.delay()

    # Example 2: Trigger a chained task (extract_data -> standardize_data)
    logging.info("Triggering chained_tasks...")
    chained_tasks.delay()

    # # Example 3: Trigger a group of tasks (parallel execution)
    # logging.info("Triggering grouped_tasks...")
    # grouped_tasks().delay()

if __name__ == "__main__":
    main()