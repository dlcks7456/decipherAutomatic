import decipherAutomatic.utils.key_variables as key
import decipherAutomatic.utils.system_variables  as sys
import itertools
import random
import pandas as pd
import os

sys_vars = [i for i in sys.variables.split('\n') if not i == '']
key_vars = [i for i in key.variables.split('\n') if not i == '']


def create_maxdiff(num_versions, num_tasks, item_list, num_attributes):
    """
    Generate MaxDiff survey combinations of items for each version and task.
    Ensuring balance, completeness, and efficiency, with shuffled item order in each combination.

    :param num_versions: Number of versions
    :param num_tasks: Number of tasks in each version
    :param item_list: List of items to choose from
    :param num_attributes: Number of attributes to select for each combination
    :return: DataFrame with the MaxDiff combinations
    """
    # Check if the number of combinations is feasible
    total_combinations = itertools.combinations(item_list, num_attributes)
    if sum(1 for _ in total_combinations) < num_versions * num_tasks:
        raise ValueError("Not enough unique combinations available for the given parameters.")

    # Generate all possible combinations of items
    all_combinations = list(itertools.combinations(item_list, num_attributes))
    random.shuffle(all_combinations)  # Shuffle for randomness

    # Balance: Ensure each item appears roughly equally across all tasks
    counts = {item: 0 for item in item_list}
    selected_combinations = []

    for combination in all_combinations:
        if len(selected_combinations) == num_versions * num_tasks:
            break  # Stop if we have enough combinations

        # Check balance
        if all(counts[item] < (num_versions * num_tasks / len(item_list)) for item in combination):
            shuffled_combination = list(combination)
            random.shuffle(shuffled_combination)  # Shuffle items in the combination
            selected_combinations.append(shuffled_combination)
            for item in combination:
                counts[item] += 1

    # Create a list to hold the data
    data = []

    # Assign combinations to each version and task
    for i, combination in enumerate(selected_combinations):
        version = (i // num_tasks) + 1
        task = (i % num_tasks) + 1
        row = [version, task] + combination
        data.append(row)

    # Create a DataFrame
    columns = ['Version', 'Set'] + [f'Item{i+1}' for i in range(num_attributes)]
    df = pd.DataFrame(data, columns=columns)

    return df


def get_versioned_filename(base_name):
    # 파일 이름과 확장자를 분리
    base, ext = os.path.splitext(base_name)
    
    # 현재 디렉토리의 파일 목록 가져오기
    files = os.listdir()
    
    # base_name 파일이 존재하는지 확인
    if base_name not in files:
        return base_name
    
    version = 2
    while True:
        # 새로운 버전 파일 이름 생성
        versioned_name = f"{base}_v{version}{ext}"
        if versioned_name not in files:
            return versioned_name
        version += 1