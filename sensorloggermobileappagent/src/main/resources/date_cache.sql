BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('trajectory-date-cache-initialization', 0));
CREATE TABLE IF NOT EXISTS public.device_date_cache (
    device_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    PRIMARY KEY (device_id, timezone)
);
CREATE TABLE IF NOT EXISTS public.device_available_dates (
    device_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    local_date DATE NOT NULL,
    PRIMARY KEY (device_id, timezone, local_date),
    FOREIGN KEY (device_id, timezone)
        REFERENCES public.device_date_cache (device_id, timezone) ON DELETE CASCADE
);

-- Call in a READ COMMITTED transaction. Ingestion takes the identical device lock.
-- A registry row means the backfill is complete, including an empty backfill.
CREATE OR REPLACE FUNCTION public.ensure_device_date_cache(device_ids TEXT[], zone_name TEXT)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    target_device TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_timezone_names WHERE name = zone_name) THEN
        RAISE EXCEPTION 'Unknown timezone: %', zone_name;
    END IF;
    FOR target_device IN SELECT DISTINCT unnest(device_ids) ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(hashtextextended(target_device, 0));
        IF NOT EXISTS (SELECT 1 FROM public.device_date_cache
                       WHERE device_id = target_device AND timezone = zone_name) THEN
            INSERT INTO public.device_date_cache VALUES (target_device, zone_name);
            INSERT INTO public.device_available_dates(device_id, timezone, local_date)
            SELECT DISTINCT target_device, zone_name,
                (to_timestamp(p.time / 1000.0) AT TIME ZONE zone_name)::date
            FROM public.trajectory_points p WHERE p.device_id = target_device
            ON CONFLICT DO NOTHING;
        END IF;
    END LOOP;
END;
$$;
COMMIT;
