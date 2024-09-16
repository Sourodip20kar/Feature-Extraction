import re
import constants
import os
import requests
import pandas as pd
import multiprocessing
import time
from time import time as timer
from tqdm import tqdm
import numpy as np
from pathlib import Path
from functools import partial
import requests
import urllib
from PIL import Image

def common_mistake(unit):
    if unit in constants.allowed_units:
        return unit
    if unit.replace('ter', 'tre') in constants.allowed_units:
        return unit.replace('ter', 'tre')
    if unit.replace('feet', 'foot') in constants.allowed_units:
        return unit.replace('feet', 'foot')
    return unit

def parse_string(s):
    s_stripped = "" if s==None or str(s)=='nan' else s.strip()
    if s_stripped == "":
        return None, None
    pattern = re.compile(r'^-?\d+(\.\d+)?\s+[a-zA-Z\s]+$')
    if not pattern.match(s_stripped):
        raise ValueError("Invalid format in {}".format(s))
    parts = s_stripped.split(maxsplit=1)
    number = float(parts[0])
    unit = common_mistake(parts[1])
    if unit not in constants.allowed_units:
        raise ValueError("Invalid unit [{}] found in {}. Allowed units: {}".format(
            unit, s, constants.allowed_units))
    return number, unit


def create_placeholder_image(image_save_path):
    try:
        placeholder_image = Image.new('RGB', (100, 100), color='black')
        placeholder_image.save(image_save_path)
    except Exception as e:
        return

def download_image(row, base_save_folder, retries=3, delay=3):
    if isinstance(row, dict):
        index = row['index']
        image_link = row['image_link']
        group_id = row['group_id']
        entity_name = row['entity_name']
    else:  # assume it's a pandas Series
        index = row.name
        image_link = row['image_link']
        group_id = row['group_id']
        entity_name = row['entity_name']

    if not isinstance(image_link, str):
        return

    filename = f"{index}_{Path(image_link).name}"
    save_folder = os.path.join(base_save_folder, str(group_id), entity_name)
    os.makedirs(save_folder, exist_ok=True)
    image_save_path = os.path.join(save_folder, filename)

    if os.path.exists(image_save_path):
        return

    for _ in range(retries):
        try:
            response = requests.get(image_link, timeout=10)
            response.raise_for_status()
            with open(image_save_path, 'wb') as f:
                f.write(response.content)
            return
        except Exception as e:
            print(f"Error downloading {image_link}: {e}")
            time.sleep(delay)
    
    create_placeholder_image(image_save_path)

def download_images(df, download_folder, allow_multiprocessing=True, limit_per_group=5000):
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # Reset index to make sure we have an 'index' column
    df = df.reset_index(drop=True).reset_index(names='index')

    # Group the dataframe by group_id and entity_name, then sample up to the limit
    grouped = df.groupby(['group_id', 'entity_name'])
    sampled_df = grouped.apply(lambda x: x.sample(min(len(x), limit_per_group))).reset_index(drop=True)

    # Further group by image_link to avoid redundant downloads
    grouped_df = sampled_df.groupby('image_link').first().reset_index()
    
    if allow_multiprocessing:
        download_image_partial = partial(
            download_image, base_save_folder=download_folder, retries=3, delay=3)

        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            list(tqdm(pool.imap(download_image_partial, grouped_df.to_dict('records')), 
                      total=len(grouped_df), desc="Downloading images"))
    else:
        for _, row in tqdm(grouped_df.iterrows(), total=len(grouped_df), desc="Downloading images"):
            download_image(row.to_dict(), base_save_folder=download_folder, retries=3, delay=3)

    # Create hard links or copies for duplicate images within the sampled set
    #for _, row in tqdm(sampled_df.iterrows(), total=len(sampled_df), desc="Creating links/copies"):
        #source_path = os.path.join(download_folder, str(row['group_id']), row['entity_name'], 
                                #   f"{row['index']}_{Path(row['image_link']).name}")
        #if os.path.exists(source_path):
           # for entity in sampled_df[(sampled_df['image_link'] == row['image_link']) & 
                                 #    (sampled_df['index'] != row['index'])]['entity_name']:
               # target_folder = os.path.join(download_folder, str(row['group_id']), entity)
               # os.makedirs(target_folder, exist_ok=True)
               # target_path = os.path.join(target_folder, f"{row['index']}_{Path(row['image_link']).name}")
               # if not os.path.exists(target_path):
                #    try:
                        # Try creating a hard link first
                    #    os.link(source_path, target_path)
                    #except OSError:
                        # If hard link fails, create a copy
                        #shutil.copy2(source_path, target_path)

def print_directory_structure(startpath, max_level=2):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        if level > max_level:
            continue
        indent = ' ' * 4 * level
        print(f'{indent}{os.path.basename(root)}/')
        if level == max_level:
            sub_indent = ' ' * 4 * (level + 1)
            file_count = len(files)
            print(f'{sub_indent}{file_count} files')
        elif level < max_level:
            sub_indent = ' ' * 4 * (level + 1)
            for f in files[:5]:  # Print only first 5 files to keep output manageable
                print(f'{sub_indent}{f}')
            if len(files) > 5:
                print(f'{sub_indent}...')