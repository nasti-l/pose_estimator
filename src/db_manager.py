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
                frames_lost_on_save INT NOT NULL,
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

    def get_all_recordings(self) -> dict[str, RecordingMetaData]:
        sql_query = """
            SELECT r.id, r.duration_in_sec, a.activity_name, s.session_start, p.participant_name,
                   r.fps, r.amount_of_frames, r.frames_lost_on_save, r.start_time, r.end_time, r.is_corrupted, r.video_path
            FROM recordings r
            LEFT JOIN activities a ON r.activity_id = a.id
            LEFT JOIN sessions s ON r.session_id = s.id
            LEFT JOIN participants p ON r.participant_id = p.id
        """
        try:
            self.__run_query(sql_query=sql_query, data=())
            rows = self.__cursor.fetchall()
            if not rows:
                raise Exception("No recordings found in the database.")
            recordings = {}
            for row in rows:
                (
                    rec_id,
                    duration_in_sec,
                    activity,
                    session_start,
                    participant,
                    fps,
                    amount_of_frames,
                    frames_lost_on_save,
                    start_time,
                    end_time,
                    if_corrupted,
                    file_location,
                ) = row

                metadata = RecordingMetaData(
                    duration_in_sec=duration_in_sec,
                    activity=activity,
                    session_start=str(session_start),
                    participant=participant,
                    fps=fps,
                    amount_of_frames=amount_of_frames,
                    frames_lost_on_save=frames_lost_on_save,
                    start_time=str(start_time),
                    end_time=str(end_time),
                    if_corrupted=if_corrupted,
                    file_location=file_location,
                )
                recordings[str(rec_id)] = metadata

            logging.info(f"Successfully fetched {len(recordings)} recordings from the database.")
            return recordings

        except Exception as e:
            raise Exception(f"Failed to fetch recordings: {e}")

    def remove_recording_by_id(self, recording_id: str) -> str:
        sql_delete = "DELETE FROM recordings WHERE id = %s RETURNING video_path"
        try:
            self.__run_query(sql_query=sql_delete, data=(recording_id,))
            result = self.__cursor.fetchone()
            if not result:
                raise Exception(f"No recording deleted; id {recording_id} may not exist.")
            video_path = result[0]
            logging.info(f"Successfully removed recording with id: {recording_id}")
            return video_path
        except Exception as e:
            raise Exception(f"Failed to remove recording with id {recording_id}: {e}")

    def get_recordings_column_names(self) -> list[str]:
        try:
            columns = self.__get_column_order_from_schema()
            sql_query = f"SELECT {', '.join(columns)} FROM recordings"
            self.__run_query(sql_query=sql_query, data=())
            column_names = [desc[0].replace("_id","_name") for desc in self.__cursor.description]
            return column_names
        except Exception as e:
            raise Exception(f"Failed to fetch column names from 'recordings' table: {e}")

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
                    fps, amount_of_frames, frames_lost_on_save, start_time, end_time, duration_in_sec
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            data = (
                session_id, activity_id, participant_id, metadata.if_corrupted,
                str(metadata.file_location), metadata.fps, metadata.amount_of_frames,
                metadata.frames_lost_on_save, metadata.start_time, metadata.end_time, metadata.duration_in_sec
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

    def __get_column_order_from_schema(self, table_name="recordings"):
        try:
            query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """
            self.__run_query(query, (table_name,))
            result = self.__cursor.fetchall()
            if result:
                return [row[0] for row in result]
        except Exception as e:
            raise Exception(f"Failed to get column order from schema: {e}")

