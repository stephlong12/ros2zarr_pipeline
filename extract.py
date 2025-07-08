from pathlib import Path
from queue import Queue
import os
import sys
import yaml
import numpy as np

def validate_path(path_to_validate : Path, make:bool =True) -> bool:
    if not path_to_validate.is_dir():
        if not make:
            print(f"ERROR: the directory you have designated as the source for bag files--'{path_to_validate}'--does not exist or is not a directory")
            sys.exit(1)
        else:
            path_to_validate.mkdir(parents=True)

    return True


def find_rosbag_paths(data_root: Path, keyword: str) -> list[Path]:
    '''
    Recursively walk the data root to find all 'rosbag' folders under data_root whos
    filename contains the keyword.

    Params:
        - name of root directory, with a / after (ex. "data/")
        - string that you want file name to start with. not _ sensitive
    Returns:
        - list of paths to the correct found rosbags, in sorted time order
    '''
    validate_path(data_root, False)
        
    for root, dirs, files in os.walk(data_root):
        path_obj = Path(root)
        if path_obj.name == 'rosbag':
            #rel_parent = path_obj.parent.relative_to(data_root)
            print(f"Found 'rosbag' folder: {path_obj}")

           
            if keyword == "":
                filtered = [path_obj / f for f in files if f.endswith('.bag')]
            else:
                filtered = [path_obj / f for f in files if f.endswith('.bag') and f.startswith(keyword)]

            print(f"Keyword '{keyword}' matched {len(filtered)} files")

            if filtered:
                return sorted(filtered)
            
#print(find_rosbag_paths(Path('data/'), "sensors"))

def load_config(path : Path) -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)
    
    
def get_topics_from_config(config_path : Path) -> list[str]:
    '''returns a list of the topics specified in the yaml file'''
    if config_path:
        config = load_config(config_path)
        return list(config.get("topics", {}).values())
    return None

def extract_fields(msg):
    """
    Extracts relevant fields from a ds_hotel_msgs/A2D2 message.

    Returns a flat numpy array containing:
    - ROS header timestamp (secs + nsecs)
    - ds_header timestamp (secs + nsecs)
    - raw[0..3]
    - proc[0..3]

    Returns:
        np.ndarray of shape (10,)
    """

    try:
        ros_time = msg.header.stamp.secs + msg.header.stamp.nsecs *1e-9
        #ds_time = msg.ds_header.orig_stamp.secs + msg.ds_header.orig_stamp.nsecs * 1e-9

        #raw_vals = list(msg.raw)
        #proc_vals = list(msg.proc)

        #return np.array([ros_time, ds_time] + raw_vals + proc_vals, dtype=np.float64)
        return np.array(ros_time, dtype=np.float64)
    except Exception as e:
        print(f"Failed to extract fields from message: {e}. (method extract_fields in extract.py)")