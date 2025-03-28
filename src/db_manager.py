import logging
import os
from abc import ABC, abstractmethod

import psycopg2

from data_manager import RecordingMetaData

class DBManager(ABC):

    @abstractmethod
    def save_metadata_for_video(self, metadata: RecordingMetaData) -> bool:
        pass

    @abstractmethod
    def extract_video_locations(self, query: str) -> {str:str}:
        pass

    @abstractmethod
    def update_results_for_video(self, results_location: str, video_id: str):
        pass

class PostgresDBManager(DBManager):
    def __init__(self):
        self.__init_db()

    def __connect(self) -> psycopg2.connect:
        self.__conn = psycopg2.connect(
            dbname=os.environ.get('PG_DBNAME'),
            user=os.environ.get('PG_USER'),
            password=os.environ.get('PG_PASS'),
            host=os.environ.get('PG_HOST'),
            port=os.environ.get('PG_PORT'),  # Default PostgreSQL port
            connect_timeout=30
        )
        self.__conn.autocommit = True
        return self.__conn.cursor()

    def __disconnect(self):
        self.__conn.close()

    def __init_db(self):
        cursor = self.__connect()
        sql = '''
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY,
                session_start TIMESTAMP NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS activities (
                id SERIAL PRIMARY KEY,
                activity_name TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS participants (
                id SERIAL PRIMARY KEY,
                participant_name TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS recordings (
                id SERIAL PRIMARY KEY,
                session_id INT REFERENCES sessions(id) ON DELETE SET NULL,
                activity_id INT REFERENCES activities(id) ON DELETE SET NULL,
                participant_id INT REFERENCES participants(id) ON DELETE SET NULL,
                is_corrupted BOOLEAN NOT NULL,
                video_path TEXT NOT NULL,
                fps INT NOT NULL,
                amount_of_frames INT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                duration_in_sec INT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS processors (
                id SERIAL PRIMARY KEY,
                processor_name TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS results (
                id SERIAL PRIMARY KEY,
                recording_id INT REFERENCES recordings(id) ON DELETE CASCADE,
                processor_id INT REFERENCES processors(id) ON DELETE SET NULL,
                file_location TEXT NOT NULL
            );
            '''
        cursor.execute(sql)
        self.__disconnect()

    def save_metadata_for_video(self, metadata: RecordingMetaData) -> bool:
        pass

    def extract_video_locations(self, query: str) -> {str: str}:
        pass

    def update_results_for_video(self, results_location: str, video_id: str):
        pass

