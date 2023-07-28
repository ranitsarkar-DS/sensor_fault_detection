from datetime import datetime
import os,sys


TRAIN_FILE_NAME = "train.csv"
TEST_FILE_NAME = "test.csv"

class TrainingPipelineConfig:
    def __init__(self):
        timestamp = datetime.now().strftime("%m_%d_%Y_%H_%S")
        self.artifact_dir=os.path.join("artifact",timestamp)


class DataIngestionConfig:
    def __init__(self,training_pipeline_config:TrainingPipelineConfig):
        data_ingestion_dir = os.path.join(training_pipeline_config.artifact_dir,"data_ingestion")
        self.dataset_dir = os.path.join(data_ingestion_dir,"dataset")
        self.train_file_path = os.path.join(self.dataset_dir,TRAIN_FILE_NAME)
        self.test_file_path = os.path.join(self.dataset_dir, TEST_FILE_NAME)
        self.database_name="sensor"
        self.collection_name="sensor_readings"
        self.test_size = 0.2


