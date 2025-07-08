from transform import transform_ros2zarr
from pathlib import Path

bag = Path("data/raw/rosbag/sensors_2024-09-08-17-57-12_13.bag")
out = Path("output/sensors.zarr")
cfg = Path("config/config.yaml")

transform_ros2zarr(bag, out, config_path=cfg, append=False)