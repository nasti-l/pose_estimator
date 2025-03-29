import logging
import os
import psycopg2
from abc import ABC, abstractmethod
from src.storage_manager import RecordingMetaData

class DBManager(ABC):

    @abstractmethod
    def save_metadata_for_video(self, metadata: RecordingMetaData):
        pass

    @abstractmethod
    def get_all_recordings_in_time_range(self, start_time: str, end_time: str) -> dict[str, str]:
        pass

    @abstractmethod
    def update_results_for_video(self, processor_name: str, results_location: str, video_id: str):
        pass

class PostgresDBManager(DBManager):
    def __init__(self):
        self.__conn = None
        self.__cursor = None
        self.__init_db()

    def __connect(self) -> psycopg2.connect:
        if self.__conn is None:
            self.__conn = psycopg2.connect(
                dbname=os.environ.get('PG_DBNAME'),
                user=os.environ.get('PG_USER'),
                password=os.environ.get('PG_PASS'),
                host=os.environ.get('PG_HOST'),
                port=os.environ.get('PG_PORT'),  # Default PostgreSQL port
                connect_timeout=30
            )
            self.__conn.autocommit = True
            self.__cursor = self.__conn.cursor()

    def __disconnect(self):
        if self.__conn:
            self.__conn.close()
            self.__conn = None

    def __init_db(self):
        self.__connect()
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
        self.__cursor.execute(sql)

    def __set_id(self, table_name: str, column_name: str, data: str) -> str:
        sql_query = f"""
                    INSERT INTO {table_name} ({column_name}) 
                    VALUES (%s) 
                    ON CONFLICT DO NOTHING 
                    RETURNING id 
                """
        self.__cursor.execute(sql_query, (data,))
        query_result = self.__cursor.fetchone()
        if not query_result:
            raise Exception(f"Failed to insert {column_name}: {data} in table {table_name}")
        return query_result[0]

    def __get_id(self, table_name: str, column_name: str, data: str) -> str:
        sql_query = f"SELECT id FROM {table_name} WHERE {column_name} = %s"
        self.__run_query(sql_query=sql_query, data=(data,))
        query_result = self.__cursor.fetchone()
        return query_result[0] if query_result else self.__set_id(data=data,
                                                                    table_name=table_name,
                                                                    column_name=column_name)

    def __run_query(self, sql_query: str, data: tuple):
        self.__cursor.execute(sql_query, data)

    def save_metadata_for_video(self, metadata: RecordingMetaData):
        try:
            session_id = self.__get_id(data=metadata.session_start,
                                       table_name="sessions",
                                       column_name="session_start")
            activity_id = self.__get_id(data=metadata.activity,
                                        table_name="activities",
                                        column_name="activity_name")
            participant_id = self.__get_id(data=metadata.participant,
                                        table_name="participants",
                                        column_name="participant_name")
            sql_query = """
                INSERT INTO recordings (
                    session_id, activity_id, participant_id, is_corrupted, video_path,
                    fps, amount_of_frames, start_time, end_time, duration_in_sec
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            data = (
                session_id, activity_id, participant_id, metadata.if_corrupted,
                str(metadata.file_location), metadata.fps, metadata.amount_of_frames,
                metadata.start_time, metadata.end_time, metadata.duration_in_sec
            )
            self.__run_query(sql_query=sql_query, data=data)
            query_result = self.__cursor.fetchone()
            if not query_result:
                raise Exception(f"Failed to insert recording's metadata for: {metadata.file_location}")
            logging.info(f"Successfully saved recording's metadata for: {metadata.file_location}")
        except Exception as e:
            raise e

    def get_all_recordings_in_time_range(self, start_time: str, end_time: str) -> dict[str, str]:
            sql_query = """
                SELECT id::text, video_path 
                FROM recordings 
                WHERE start_time >= %s AND end_time <= %s
            """
            data = (start_time, end_time)
            try:
                self.__run_query(sql_query=sql_query, data=data)
            except Exception as e:
                raise Exception(f"Query failed in attempt to get recordings in time range: {start_time}-{end_time}: {e}")
            query_results = self.__cursor.fetchall()
            if not query_results:
                raise Exception(f"No matching recordings found in time range: {start_time}-{end_time}")
            results = {row[0]: row[1] for row in query_results}
            logging.info(f"Successfully retrieved {len(results)} recordings in time range: {start_time}-{end_time}")
            return results

    def update_results_for_video(self, processor_name: str, results_location: str, video_id: str):
        processor_id = self.__get_id(data=processor_name,
                                   table_name="processors",
                                   column_name="processor_name")
        sql_query = """
            INSERT INTO results (
                recording_id, processor_id, file_location) 
            VALUES (%s, %s, %s) RETURNING id
        """
        data = (video_id, processor_id, results_location)
        try:
            self.__run_query(sql_query=sql_query, data=data)
        except Exception as e:
            raise Exception(f"Query failed in attempt to update {processor_name} results for: {results_location}: {e}")
        query_result = self.__cursor.fetchone()
        if not query_result:
            raise Exception(f"Query executed successfully, but no row was inserted in attempt to update {processor_name} results for: {results_location}")
        logging.info(f"Successfully updated {processor_name} results for: {results_location}")


    def __del__(self):
        self.__disconnect()

