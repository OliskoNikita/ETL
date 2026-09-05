CREATE DATABASE backend;

\connect backend;

CREATE TABLE public.regions (
    place_pattern TEXT PRIMARY KEY,
    region TEXT,
    place_hash TEXT
);

INSERT INTO public.regions (place_pattern, region, place_hash)
VALUES
    ('south of the Fiji Islands', 'Fiji', MD5(LOWER(TRIM('south of the Fiji Islands')))),
    ('Fiji region', 'Fiji', MD5(LOWER(TRIM('Fiji region')))),
    ('West Chile Rise', 'Chile', MD5(LOWER(TRIM('West Chile Rise')))),
    ('South Georgia Island region', 'South Georgia Island', MD5(LOWER(TRIM('South Georgia Island region')))),
    ('Pacific-Antarctic Ridge', 'Southern Ocean', MD5(LOWER(TRIM('Pacific-Antarctic Ridge')))),
    ('Mid-Indian Ridge', 'Indian Ocean', MD5(LOWER(TRIM('Mid-Indian Ridge')))),
    ('western Indian-Antarctic Ridge', 'Southern Ocean', MD5(LOWER(TRIM('western Indian-Antarctic Ridge')))),
    ('Kermadec Islands region', 'New Zealand', MD5(LOWER(TRIM('Kermadec Islands region')))),
    ('southern East Pacific Rise', 'Pacific Ocean', MD5(LOWER(TRIM('southern East Pacific Rise')))),
    ('South Sandwich Islands region', 'South Sandwich Islands', MD5(LOWER(TRIM('South Sandwich Islands region'))));

CREATE DATABASE metadata;

\connect metadata;

CREATE TABLE public.s3_max_dates (
    table_name TEXT PRIMARY KEY,
    max_date DATE,
    updated_at TIMESTAMP
);