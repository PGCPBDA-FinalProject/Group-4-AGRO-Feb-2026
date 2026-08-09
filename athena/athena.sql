CREATE OR REPLACE VIEW vw_ps1_base_clean AS

SELECT
    ss.region_id,
    ss.state_id,
    ss.city_id,

    dc.city_name,
    ds.state_name,
    dr.region_name,

    ss.season,
    ss.crop,
    ss.year,

    ss.avg_temperature,
    ss.avg_rainfall,
    ss.avg_humidity,

    ss.official_start_date,
    ss.detected_start_date,
    ss.detected_month,

    ss.season_shift_days,
    ss.shift_category,

    ROUND(ss.crop_suitability_score, 2) AS crop_suitability_score,
    ROUND(ss.season_stability_score, 2) AS season_stability_score

FROM season_shift ss

LEFT JOIN dim_city dc
    ON ss.city_id = dc.city_id

LEFT JOIN dim_state ds
    ON ss.state_id = ds.state_id

LEFT JOIN dim_region dr
    ON ss.region_id = dr.region_id

WHERE ss.official_start_date IS NOT NULL
  AND ss.detected_start_date IS NOT NULL
  AND ss.season_shift_days IS NOT NULL;




CREATE OR REPLACE VIEW vw_ps1_arrival_summary AS

SELECT

    region_id,
    state_id,

    year,
    season,
    crop,

    state_name,
    region_name,

    CASE
        WHEN season_shift_days < 0 THEN 'Early Arrival'
        WHEN season_shift_days > 0 THEN 'Late Arrival'
        ELSE 'On Time'
    END AS arrival_type,

    COUNT(*) AS total_arrivals,

    ROUND(AVG(ABS(season_shift_days)),2) AS avg_shift_days

FROM vw_ps1_base_clean

GROUP BY

    region_id,
    state_id,

    year,
    season,
    crop,

    state_name,
    region_name,

    CASE
        WHEN season_shift_days < 0 THEN 'Early Arrival'
        WHEN season_shift_days > 0 THEN 'Late Arrival'
        ELSE 'On Time'
    END;

CREATE OR REPLACE VIEW vw_ps1_trend AS

SELECT

    region_id,
    state_id,
    city_id,

    city_name,
    state_name,
    region_name,

    season,
    crop,
    year,

    season_shift_days,

    shift_category,

    crop_suitability_score,

    season_stability_score,

    AVG(season_shift_days) OVER
    (
        PARTITION BY city_id, season
        ORDER BY year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3yr_avg_shift

FROM vw_ps1_base_clean

ORDER BY
    city_name,
    season,
    year;

CREATE OR REPLACE VIEW vw_ps1_early_arrival AS

SELECT

    region_id,
    state_id,
    city_id,

    city_name,

    state_name,

    region_name,

    season,

    crop,

    year,

    ROUND(AVG(season_shift_days),2) AS avg_advance_days,

    ROUND(AVG(crop_suitability_score),2) AS avg_crop_score,

    ROUND(AVG(season_stability_score),2) AS avg_stability_score

FROM vw_ps1_base_clean

WHERE season_shift_days < 0

GROUP BY

    region_id,
    state_id,
    city_id,

    city_name,

    state_name,

    region_name,

    season,

    crop,

    year

ORDER BY avg_advance_days ASC;

CREATE OR REPLACE VIEW vw_ps1_late_arrival AS

SELECT

    region_id,
    state_id,
    city_id,

    city_name,

    state_name,

    region_name,

    season,

    crop,

    year,

    ROUND(AVG(season_shift_days),2) AS avg_delay_days,

    ROUND(AVG(crop_suitability_score),2) AS avg_crop_score,

    ROUND(AVG(season_stability_score),2) AS avg_stability_score

FROM vw_ps1_base_clean

WHERE season_shift_days > 0

GROUP BY

    region_id,
    state_id,
    city_id,

    city_name,

    state_name,

    region_name,

    season,

    crop,

    year

ORDER BY avg_delay_days DESC;

CREATE OR REPLACE VIEW vw_ps1_shift_by_season AS

SELECT

    season,

    ROUND(AVG(season_shift_days),2) AS avg_shift,

    ROUND(AVG(crop_suitability_score),2) AS crop_score,

    ROUND(AVG(season_stability_score),2) AS stability_score,

    COUNT(*) AS observations

FROM vw_ps1_base_clean

GROUP BY season;


CREATE OR REPLACE VIEW dim_state_dashboard AS

SELECT DISTINCT

    ds.state_id,

    ds.state_name,

    ds.region_id

FROM vw_ps1_base_clean b

JOIN dim_state ds
ON b.state_id = ds.state_id

ORDER BY ds.state_name;

CREATE OR REPLACE VIEW dim_region_dashboard AS

SELECT DISTINCT

    dr.region_id,

    dr.region_name

FROM vw_ps1_base_clean b

JOIN dim_region dr
ON b.region_id = dr.region_id

ORDER BY dr.region_name;

CREATE OR REPLACE VIEW dim_city_dashboard AS

SELECT DISTINCT

    dc.city_id,

    dc.city_name,

    dc.state_id

FROM vw_ps1_base_clean b

JOIN dim_city dc
ON b.city_id = dc.city_id

ORDER BY dc.city_name;

CREATE OR REPLACE VIEW dim_year_dashboard AS

SELECT DISTINCT

    year

FROM vw_ps1_base_clean

ORDER BY year;