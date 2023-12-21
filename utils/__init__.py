import decipherAutomatic.utils.key_variables as key
import decipherAutomatic.utils.system_variables  as sys
import itertools
import random
import pandas as pd

sys_vars = [i for i in sys.variables.split('\n') if not i == '']
key_vars = [i for i in key.variables.split('\n') if not i == '']


def create_maxdiff(num_versions, num_tasks, item_list, num_attributes):
    """
    Generate unique combinations of items for each version and task.

    :param num_versions: Number of versions
    :param num_tasks: Number of tasks in each version
    :param item_list: List of items to choose from
    :param num_attributes: Number of attributes to select for each combination
    :return: DataFrame with the combinations
    """
    # Generate all possible combinations of items
    all_combinations = list(itertools.combinations(item_list, num_attributes))
    random.shuffle(all_combinations)  # Shuffle for randomness

    # Create a list to hold the data
    data = []

    # Assign combinations to each version and task
    for version in range(1, num_versions + 1):
        for task in range(1, num_tasks + 1):
            combination = all_combinations.pop()
            combination = list(combination)
            random.shuffle(combination)
            row = [version, task] + combination
            data.append(row)

    # Create a DataFrame
    columns = ['Version', 'Set'] + [f'Item{i+1}' for i in range(num_attributes)]
    df = pd.DataFrame(data, columns=columns)

    return df