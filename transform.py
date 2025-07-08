import yaml
import zarr
import numpy as np
from pathlib import Path
from tqdm import tqdm

from rosbags.rosbag1 import Reader
from rosbags.serde import deserialize_ros1
from message_type_creator import MsgParser
from rosbags.typesys import register_types

from extract import get_topics_from_config, extract_fields

parser = MsgParser('~/Code/ros2zarr_pipeline/config/ds_msgs/ds_hotel_msgs')
register_types(parser.get_type_definitions())

    
def init_zarr_dataset(root, topic, sample_shape=(10,), chunk_size=1000, dtype=np.float64):
    return root.create_dataset(
        topic,
        shape=(0,)+sample_shape,
        chunks=(chunk_size,) + sample_shape,
        dtype=dtype,
        compressor=zarr.Blosc()
        )

def flush_buffer(zarr_array, buffer):
    if buffer:
        stacked = np.vstack(buffer)
        zarr_array.append(stacked)
        buffer.clear()

def transform_ros2zarr(input_bag_path, zarr_path, config_path="/config/config.yaml", chunk_size=1000, append=True):
    topic_filter = get_topics_from_config(config_path)
    print(f"Converting {input_bag_path.name} with topic filter: {topic_filter}")

    mode = 'a' if append else 'w'
    root = zarr.open_group(zarr_path, mode=mode)

    buffers = {}
    counts = {}
    last_timestamps ={}

    with Reader(str(input_bag_path)) as reader:

        for connection, _, rawdata in tqdm(reader.messages(), desc=f"Reading {input_bag_path.name}"):
            topic = connection.topic
            msgtype = connection.msgtype
            #print(msgtype)

            if topic_filter and topic not in topic_filter:
                continue

            try:
                msg = deserialize_ros1(rawdata, msgtype)
            except Exception as e:
                print(f"Skipping malformed message on {topic}: {e}. msgtype is: {msgtype}")
                continue
        
            data = extract_fields(msg)
            if data is None:
                print("data is none")
                continue

            topic_key = topic.strip('/').replace('/','_')

            if topic_key not in root:
                root_array = init_zarr_dataset(root, topic_key, sample_shape=(10,), chunk_size=chunk_size)
                buffers[topic_key] = []
                counts[topic_key] = 0
                last_timestamps[topic_key] = -float("inf")
            else:
                root_array = root[topic_key]

            timestamp = data[0]
            if timestamp < last_timestamps[topic_key]:
                print(f"Non continuous timestamp: {timestamp:.6f} < {last_timestamps[topic_key]:.6f}")
            
            last_timestamps[topic_key] = timestamp
            buffers[topic_key].append(data)
            counts[topic_key] += 1

            if len(buffers[topic_key]) >= chunk_size:
                flush_buffer(root_array, buffers[topic_key])

        for topic_key in buffers:
            flush_buffer(root[topic_key], buffers[topic_key])

    print(f"✔ Done {input_bag_path.name} → {zarr_path}")
    print(f"✔ Message counts: {counts}")
    




