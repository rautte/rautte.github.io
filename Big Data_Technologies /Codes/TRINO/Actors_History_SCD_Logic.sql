INSERT INTO {production_table}
            WITH yesterday AS (
                SELECT *
                FROM {production_table}
                WHERE current_year = YEAR(DATE('{current_year}')) - 1
            ),
            today AS (
                SELECT 
                    actor_id,
                    actor,
                    year
                FROM {staging_table}
                WHERE year = YEAR(DATE('{current_year}'))
            ),
            checker AS (
                SELECT *
                FROM {production_table}
                WHERE current_year = YEAR(DATE('{current_year}'))
            ),
            changes AS (
                SELECT 
                    CASE
                        WHEN t.actor_id IS NULL THEN y.actor_id
                        WHEN y.actor_id IS NULL THEN t.actor_id
                        ELSE y.actor_id 
                    END AS actor_id,
                    CASE
                        WHEN t.actor_id IS NULL THEN y.actor_name
                        WHEN y.actor_id IS NULL THEN t.actor
                        ELSE y.actor_name
                    END AS actor_name,
                    CASE
                        WHEN t.actor_id IS NULL THEN False
                        WHEN y.actor_id IS NULL THEN True
                        ELSE True
                    END AS is_active,
                    CASE
                        WHEN y.is_active = False AND t.actor_id IS NULL 
                            THEN y.start_year
                        WHEN y.actor_id IS NULL THEN YEAR(DATE('{current_year}'))INSERT INTO {production_table}.sql
                        WHEN y.is_active = True AND t.actor_id IS NOT NULL 
                            THEN y.start_year
                        ELSE YEAR(DATE('{current_year}'))
                    END AS start_year,
                    YEAR(DATE('{current_year}')) AS end_year,
                    YEAR(DATE('{current_year}')) AS current_year
                FROM yesterday y FULL OUTER JOIN today t
                    ON y.actor_id = t.actor_id
            ),
            non_duplicates AS (
                SELECT 
                    c.actor_id,
                    c.actor_name,
                    c.is_active,
                    c.start_year,
                    c.end_year,
                    c.current_year 
                FROM changes c LEFT JOIN checker prod
                    ON c.actor_id = prod.actor_id 
                        AND c.is_active = prod.is_active
                        AND c.start_year = prod.start_year
                        AND c.end_year = prod.end_year
                WHERE
                        prod.actor_id IS NULL
            )

            SELECT *
            FROM non_duplicates