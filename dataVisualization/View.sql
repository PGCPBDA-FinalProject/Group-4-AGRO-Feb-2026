CREATE OR REPLACE VIEW vw_ps3_renewable_dashboard AS
SELECT
    -- Geography
    rr.region_id,
    dr.region_name,

    rr.state_id,
    ds.state_name,

    rr.city_id,
    dc.city_name,
    dc.district,
    dc.lat,
    dc.lng,

    -- Renewable Metrics
    rr.avg_wind_speed_10m,
    rr.avg_wind_speed_100m,

    rr.peak_wind_speed_10m,
    rr.peak_wind_speed_100m,

    rr.avg_cloud_cover,

    rr.avg_wind_10m_score,
    rr.avg_wind_100m_score,

    rr.avg_solar_score,
    rr.avg_solar_proxy,

    rr.renewable_index,
    rr.renewable_rank,

    rr.renewable_category,
    rr.greenhouse_wind_suitability,
    rr.recommended_energy_type

FROM renewable_ranking rr

INNER JOIN dim_city dc
    ON rr.city_id = dc.city_id

INNER JOIN dim_state ds
    ON rr.state_id = ds.state_id

INNER JOIN dim_region dr
    ON rr.region_id = dr.region_id;