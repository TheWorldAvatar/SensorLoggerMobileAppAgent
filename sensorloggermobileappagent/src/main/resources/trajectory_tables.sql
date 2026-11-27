-- Run in the same PostGIS database used by all three agents.
-- Source seconds intentionally match time_series_data.time_as_number precision.
CREATE TABLE IF NOT EXISTS public.trajectory_points (
    device_id TEXT NOT NULL,
    source_time DOUBLE PRECISION NOT NULL,
    time BIGINT GENERATED ALWAYS AS (ROUND(source_time * 1000.0)::BIGINT) STORED,
    geom geometry(Point, 4326) NOT NULL,
    speed DOUBLE PRECISION,
    altitude DOUBLE PRECISION,
    bearing DOUBLE PRECISION,
    session_id VARCHAR,
    trip INTEGER,
    activity_type VARCHAR,
    activity_source_time DOUBLE PRECISION,
    PRIMARY KEY (device_id, source_time)
);
CREATE INDEX IF NOT EXISTS trajectory_points_device_time_idx
    ON public.trajectory_points (device_id, time);
CREATE INDEX IF NOT EXISTS trajectory_points_assigned_device_idx
    ON public.trajectory_points (device_id) WHERE trip IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.trajectory_activities (
    device_id TEXT NOT NULL,
    source_time DOUBLE PRECISION NOT NULL,
    activity_type VARCHAR,
    confidence INTEGER,
    PRIMARY KEY (device_id, source_time)
);

-- Trip presence is tested over the whole trajectory, before query time bounds.
-- Existing view definitions are changed only by explicit migrations.
DO $init$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('trajectory-view-initialization', 0));
    IF to_regclass('public.timeline_trajectory_points') IS NULL THEN
        EXECUTE $view$
CREATE VIEW public.timeline_trajectory_points AS
SELECT p.* FROM public.trajectory_points p
WHERE p.trip IS NOT NULL OR NOT EXISTS (
    SELECT 1 FROM public.trajectory_points assigned
    WHERE assigned.device_id = p.device_id AND assigned.trip IS NOT NULL
);

        $view$;
    END IF;
END;
$init$;
