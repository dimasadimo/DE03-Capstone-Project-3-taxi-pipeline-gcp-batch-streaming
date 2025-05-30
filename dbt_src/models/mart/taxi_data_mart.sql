{{ 
    config(
        materialized='incremental',
        partition_by={
        "field": "lpep_pickup_datetime",
        "data_type": "timestamp"
        },
        cluster_by=["vendor_id", "pu_location_id"]
    ) 
}}

WITH transform AS (
    SELECT
        t.VendorID AS vendor_id,
        t.PULocationID AS pu_location_id,
        t.DOLocationID AS do_location_id,
        t.RatecodeID AS rate_code_id,
        t.* EXCEPT(trip_distance, VendorID, RatecodeID, PULocationID, DOLocationID),
        TIME(TIMESTAMP_SECONDS(MOD(TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, SECOND),86400))) AS trip_duration_time,
        ROUND(t.trip_distance * 1.60934, 2) AS trip_distance
    FROM {{ source('jcdeol3_capstone3_dimasadihartomo', 'taxi_data_staging') }} t
    {% if is_incremental() %}
        WHERE lpep_pickup_datetime > (SELECT MAX(lpep_pickup_datetime) FROM {{ this }})
    {% endif %}
)

SELECT
    t.* EXCEPT(payment_type),
    pt.description AS payment_type,
    pz.zone AS pu_zone,
    dz.zone AS do_zone
FROM transform t
LEFT JOIN {{ source('jcdeol3_capstone3_dimasadihartomo', 'taxi_zone') }} AS pz
    ON t.pu_location_id = pz.LocationID
LEFT JOIN {{ source('jcdeol3_capstone3_dimasadihartomo', 'taxi_zone') }} AS dz
    ON t.do_location_id = dz.LocationID
LEFT JOIN {{ source('jcdeol3_capstone3_dimasadihartomo', 'payment_type') }} AS pt
    ON t.payment_type = pt.payment_type