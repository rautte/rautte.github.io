CREATE OR REPLACE TABLE rauttejas.actors_history_scd (
                actor_id VARCHAR,
                actor_name VARCHAR,
                is_active BOOLEAN,
                start_year INTEGER,
                end_year INTEGER,
                current_year INTEGER
            )
            WITH 
            (
                FORMAT = 'PARQUET',
                partitioning = ARRAY['current_year']
            )
