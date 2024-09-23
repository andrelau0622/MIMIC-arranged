0. 创建public
-- function
SET search_path TO public;

CREATE OR REPLACE FUNCTION REGEXP_EXTRACT(str TEXT, pattern TEXT) RETURNS TEXT AS $$
BEGIN
RETURN substring(str from pattern);
END; $$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION REGEXP_CONTAINS(str TEXT, pattern TEXT) RETURNS BOOL AS $$
BEGIN
RETURN str ~ pattern;
END; $$
LANGUAGE PLPGSQL;

-- alias generate_series with generate_array
CREATE OR REPLACE FUNCTION GENERATE_ARRAY(i INTEGER, j INTEGER)
RETURNS setof INTEGER language sql as $$
    SELECT GENERATE_SERIES(i, j)
$$;

-- datetime functions
CREATE OR REPLACE FUNCTION DATETIME(dt DATE) RETURNS TIMESTAMP(3) AS $$
BEGIN
RETURN CAST(dt AS TIMESTAMP(3));
END; $$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION DATETIME(year INTEGER, month INTEGER, day INTEGER, hour INTEGER, minute INTEGER, second INTEGER) RETURNS TIMESTAMP(3) AS $$
BEGIN
RETURN TO_TIMESTAMP(
    TO_CHAR(year, '0000') || TO_CHAR(month, '00') || TO_CHAR(day, '00') || TO_CHAR(hour, '00') || TO_CHAR(minute, '00') || TO_CHAR(second, '00'),
    'yyyymmddHH24MISS'
);
END; $$
LANGUAGE PLPGSQL;

-- overload allowing string input

--  DATETIME_ADD(datetime, INTERVAL 'n' DATEPART) -> datetime + INTERVAL 'n' DATEPART
-- note: in bigquery, INTERVAL 1 YEAR is a valid interval
-- but in postgres, it must be INTERVAL '1' YEAR
CREATE OR REPLACE FUNCTION DATETIME_ADD(datetime_val TIMESTAMP(3), intvl INTERVAL) RETURNS TIMESTAMP(3) AS $$
BEGIN
RETURN datetime_val + intvl;
END; $$
LANGUAGE PLPGSQL;

--  DATETIME_SUB(datetime, INTERVAL 'n' DATEPART) -> datetime - INTERVAL 'n' DATEPART
CREATE OR REPLACE FUNCTION DATETIME_SUB(datetime_val TIMESTAMP(3), intvl INTERVAL) RETURNS TIMESTAMP(3) AS $$
BEGIN
RETURN datetime_val - intvl;
END; $$
LANGUAGE PLPGSQL;

-- TODO:
--   DATETIME_TRUNC(datetime, PART) -> DATE_TRUNC('datepart', datetime)

-- below requires a regex to convert datepart from primitive to a string
-- i.e. encapsulate it in single quotes
CREATE OR REPLACE FUNCTION DATETIME_DIFF(endtime TIMESTAMP(3), starttime TIMESTAMP(3), datepart TEXT) RETURNS NUMERIC AS $$
BEGIN
RETURN 
    EXTRACT(EPOCH FROM endtime - starttime) /
    CASE
        WHEN datepart = 'SECOND' THEN 1.0
        WHEN datepart = 'MINUTE' THEN 60.0
        WHEN datepart = 'HOUR' THEN 3600.0
        WHEN datepart = 'DAY' THEN 24*3600.0
        WHEN datepart = 'YEAR' THEN 365.242*24*3600.0
    ELSE NULL END;
END; $$
LANGUAGE PLPGSQL;

-- BigQuery has a custom data type, PART
-- It's difficult to replicate this in postgresql, which recognizes the PART as a column name,
-- unless it is within an EXTRACT() function.

CREATE OR REPLACE FUNCTION BIGQUERY_FORMAT_TO_PSQL(format_str VARCHAR(255)) RETURNS TEXT AS $$
BEGIN
RETURN 
    -- use replace to convert BigQuery string format to postgres string format
    -- only handles a few cases since we don't extensively use this function
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
        format_str
        , '%S', 'SS'
    )
        , '%M', 'MI'
    )
        , '%H', 'HH24'
    )
        , '%d', 'dd'
    )
        , '%m', 'mm'
    )
        , '%Y', 'yyyy'
    )
;
END; $$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION FORMAT_DATE(format_str VARCHAR(255), datetime_val TIMESTAMP(3)) RETURNS TEXT AS $$
BEGIN
RETURN TO_CHAR(
    datetime_val,
    -- use replace to convert BigQuery string format to postgres string format
    -- only handles a few cases since we don't extensively use this function
    BIGQUERY_FORMAT_TO_PSQL(format_str)
);
END; $$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION PARSE_DATE(format_str VARCHAR(255), string_val VARCHAR(255)) RETURNS DATE AS $$
BEGIN
RETURN TO_DATE(
    string_val,
    -- use replace to convert BigQuery string format to postgres string format
    -- only handles a few cases since we don't extensively use this function
    BIGQUERY_FORMAT_TO_PSQL(format_str)
);
END; $$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION FORMAT_DATETIME(format_str VARCHAR(255), datetime_val TIMESTAMP(3)) RETURNS TEXT AS $$
BEGIN
RETURN TO_CHAR(
    datetime_val,
    -- use replace to convert BigQuery string format to postgres string format
    -- only handles a few cases since we don't extensively use this function
    BIGQUERY_FORMAT_TO_PSQL(format_str)
);
END; $$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION PARSE_DATETIME(format_str VARCHAR(255), string_val VARCHAR(255)) RETURNS TIMESTAMP(3) AS $$
BEGIN
RETURN TO_TIMESTAMP(
    string_val,
    -- use replace to convert BigQuery string format to postgres string format
    -- only handles a few cases since we don't extensively use this function
    BIGQUERY_FORMAT_TO_PSQL(format_str)
);
END; $$
LANGUAGE PLPGSQL;

-- 年龄
-- age 
create materialized view if not exists age as 
SELECT 	
	ad.subject_id
	, ad.hadm_id
	, ad.admittime
	, pa.anchor_age
	, pa.anchor_year
	, DATETIME_DIFF(ad.admittime, DATETIME(pa.anchor_year, 1, 1, 0, 0, 0), 'YEAR') + pa.anchor_age AS age
FROM mimic_hosp.admissions ad
INNER JOIN mimic_hosp.patients pa
ON ad.subject_id = pa.subject_id;

create materialized view if not exists icustay_detail as 
SELECT ie.subject_id, ie.hadm_id, ie.stay_id

-- patient level factors
, pat.gender, pat.dod

-- 院内水平指标
-- hospital level factors
, adm.admittime, adm.dischtime
, DATETIME_DIFF(adm.dischtime, adm.admittime, 'DAY') as los_hospital
, DATETIME_DIFF(adm.admittime, DATETIME(pat.anchor_year, 1, 1, 0, 0, 0), 'YEAR') + pat.anchor_age as admission_age
, adm.race
, adm.hospital_expire_flag
, DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) AS hospstay_seq
, CASE
    WHEN DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) = 1 THEN True
    ELSE False END AS first_hosp_stay

-- ICU水平指标
-- icu level factors
, ie.intime as icu_intime, ie.outtime as icu_outtime
, ROUND(DATETIME_DIFF(ie.outtime, ie.intime, 'HOUR')/24.0, 2) as los_icu
, DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_seq

-- first ICU stay *for the current hospitalization*
, CASE
    WHEN DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1 THEN True
    ELSE False END AS first_icu_stay

FROM mimic_icu.icustays ie
INNER JOIN mimic_hosp.admissions adm
    ON ie.hadm_id = adm.hadm_id
INNER JOIN mimic_hosp.patients pat
    ON ie.subject_id = pat.subject_id;

-- ICU住院时间
-- icustay_times
create materialized view if not exists icustay_times as 
WITH t1 AS
(
    select ce.stay_id
    , min(charttime) as intime_hr
    , max(charttime) as outtime_hr
    FROM mimic_icu.chartevents ce
    -- only look at heart rate
    where ce.itemid = 220045
    group by ce.stay_id
)
-- add in subject_id/hadm_id
select
  ie.subject_id, ie.hadm_id, ie.stay_id
  , t1.intime_hr
  , t1.outtime_hr
FROM mimic_icu.icustays ie
left join t1
  on ie.stay_id = t1.stay_id;
  
-- ICU住院时间（精确到小时）
-- icustay_hourly
create materialized view if not exists icustay_hourly as 
with all_hours as
(
select
  it.stay_id

  -- ceiling the intime to the nearest hour by adding 59 minutes then truncating
  -- note thart we truncate by parsing as string, rather than using DATETIME_TRUNC
  -- this is done to enable compatibility with psql
  , PARSE_DATETIME(
      '%Y-%m-%d %H:00:00',
      FORMAT_DATETIME(
        '%Y-%m-%d %H:00:00',
          DATETIME_ADD(it.intime_hr, INTERVAL '59' MINUTE)
  )) AS endtime

  -- create integers for each charttime in hours from admission
  -- so 0 is admission time, 1 is one hour after admission, etc, up to ICU disch
  --  we allow 24 hours before ICU admission (to grab labs before admit)
  , GENERATE_ARRAY(-24, CEIL(DATETIME_DIFF(it.outtime_hr, it.intime_hr, 'HOUR'))::INTEGER) as hrs

  from icustay_times it
)
SELECT stay_id
, CAST(hr AS BIGINT) as hr
, DATETIME_ADD(endtime, interval '1 HOUR' * (CAST(hr AS bigint))) as endtime
FROM all_hours
CROSS JOIN UNNEST(array[all_hours.hrs]) AS hr;

-- 体重变化（ICU期间体重变化，主要反映患者营养状况）
-- weight_durations
create materialized view if not exists weight_durations as 
WITH wt_stg as
(
    SELECT
        c.stay_id
      , c.charttime
      , case when c.itemid = 226512 then 'admit'
          else 'daily' end as weight_type
      -- TODO: eliminate obvious outliers if there is a reasonable weight
      , c.valuenum as weight
    FROM mimic_icu.chartevents c
    WHERE c.valuenum IS NOT NULL
      AND c.itemid in
      (
          226512 -- Admit Wt
          , 224639 -- Daily Weight
      )
      AND c.valuenum > 0
)
-- assign ascending row number
, wt_stg1 as
(
  select
      stay_id
    , charttime
    , weight_type
    , weight
    , ROW_NUMBER() OVER (partition by stay_id, weight_type order by charttime) as rn
  from wt_stg
  WHERE weight IS NOT NULL
)
-- change charttime to intime for the first admission weight recorded
, wt_stg2 AS
(
  SELECT 
      wt_stg1.stay_id
    , ie.intime, ie.outtime
    , wt_stg1.weight_type
    , case when wt_stg1.weight_type = 'admit' and wt_stg1.rn = 1
        then DATETIME_SUB(ie.intime, INTERVAL '2' HOUR)
      else wt_stg1.charttime end as starttime
    , wt_stg1.weight
  from wt_stg1
  INNER JOIN mimic_icu.icustays ie
    on ie.stay_id = wt_stg1.stay_id
)
, wt_stg3 as
(
  select
    stay_id
    , intime, outtime
    , starttime
    , coalesce(
        LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime),
        DATETIME_ADD(outtime, INTERVAL '2' HOUR)
      ) as endtime
    , weight
    , weight_type
  from wt_stg2
)
-- this table is the start/stop times from admit/daily weight in charted data
, wt1 as
(
  select
      stay_id
    , starttime
    , coalesce(endtime,
      LEAD(starttime) OVER (partition by stay_id order by starttime),
      -- impute ICU discharge as the end of the final weight measurement
      -- plus a 2 hour "fuzziness" window
      DATETIME_ADD(outtime, INTERVAL '2' HOUR)
    ) as endtime
    , weight
    , weight_type
  from wt_stg3
)
-- if the intime for the patient is < the first charted daily weight
-- then we will have a "gap" at the start of their stay
-- to prevent this, we look for these gaps and backfill the first weight
-- this adds (153255-149657)=3598 rows, meaning this fix helps for up to 3598 stay_id
, wt_fix as
(
  select ie.stay_id
    -- we add a 2 hour "fuzziness" window
    , DATETIME_SUB(ie.intime, INTERVAL '2' HOUR) as starttime
    , wt.starttime as endtime
    , wt.weight
    , wt.weight_type
  from mimic_icu.icustays ie
  inner join
  -- the below subquery returns one row for each unique stay_id
  -- the row contains: the first starttime and the corresponding weight
  (
    SELECT wt1.stay_id, wt1.starttime, wt1.weight
    , weight_type
    , ROW_NUMBER() OVER (PARTITION BY wt1.stay_id ORDER BY wt1.starttime) as rn
    FROM wt1
  ) wt
    ON  ie.stay_id = wt.stay_id
    AND wt.rn = 1
    and ie.intime < wt.starttime
)
-- add the backfill rows to the main weight table
SELECT
wt1.stay_id
, wt1.starttime
, wt1.endtime
, wt1.weight
, wt1.weight_type
FROM wt1
UNION ALL
SELECT
wt_fix.stay_id
, wt_fix.starttime
, wt_fix.endtime
, wt_fix.weight
, wt_fix.weight_type
FROM wt_fix;

-- 血气
-- measurement
-- bg
-- The aim of this query is to pivot entries related to blood gases
-- which were found in LABEVENTS
create materialized view if not exists bg as 
WITH bg AS
(
select 
  -- specimen_id only ever has 1 measurement for each itemid
  -- so, we may simply collapse rows using MAX()
    MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , MAX(charttime) AS charttime
  -- specimen_id *may* have different storetimes, so this is taking the latest
  , MAX(storetime) AS storetime
  , le.specimen_id
  , MAX(CASE WHEN itemid = 52028 THEN value ELSE NULL END) AS specimen
  , MAX(CASE WHEN itemid = 50801 THEN valuenum ELSE NULL END) AS aado2
  , MAX(CASE WHEN itemid = 50802 THEN valuenum ELSE NULL END) AS baseexcess
  , MAX(CASE WHEN itemid = 50803 THEN valuenum ELSE NULL END) AS bicarbonate
  , MAX(CASE WHEN itemid = 50804 THEN valuenum ELSE NULL END) AS totalco2
  , MAX(CASE WHEN itemid = 50805 THEN valuenum ELSE NULL END) AS carboxyhemoglobin
  , MAX(CASE WHEN itemid = 50806 THEN valuenum ELSE NULL END) AS chloride
  , MAX(CASE WHEN itemid = 50808 THEN valuenum ELSE NULL END) AS calcium
  , MAX(CASE WHEN itemid = 50809 and valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose
  , MAX(CASE WHEN itemid = 50810 and valuenum <= 100 THEN valuenum ELSE NULL END) AS hematocrit
  , MAX(CASE WHEN itemid = 50811 THEN valuenum ELSE NULL END) AS hemoglobin
  , MAX(CASE WHEN itemid = 50813 and valuenum <= 10000 THEN valuenum ELSE NULL END) AS lactate
  , MAX(CASE WHEN itemid = 50814 THEN valuenum ELSE NULL END) AS methemoglobin
  , MAX(CASE WHEN itemid = 50815 THEN valuenum ELSE NULL END) AS o2flow
  -- fix a common unit conversion error for fio2
  -- atmospheric o2 is 20.89%, so any value <= 20 is unphysiologic
  -- usually this is a misplaced O2 flow measurement
  , MAX(CASE WHEN itemid = 50816 THEN
      CASE
        WHEN valuenum > 20 AND valuenum <= 100 THEN valuenum 
        WHEN valuenum > 0.2 AND valuenum <= 1.0 THEN valuenum*100.0
      ELSE NULL END
    ELSE NULL END) AS fio2
  , MAX(CASE WHEN itemid = 50817 AND valuenum <= 100 THEN valuenum ELSE NULL END) AS so2
  , MAX(CASE WHEN itemid = 50818 THEN valuenum ELSE NULL END) AS pco2
  , MAX(CASE WHEN itemid = 50819 THEN valuenum ELSE NULL END) AS peep
  , MAX(CASE WHEN itemid = 50820 THEN valuenum ELSE NULL END) AS ph
  , MAX(CASE WHEN itemid = 50821 THEN valuenum ELSE NULL END) AS po2
  , MAX(CASE WHEN itemid = 50822 THEN valuenum ELSE NULL END) AS potassium
  , MAX(CASE WHEN itemid = 50823 THEN valuenum ELSE NULL END) AS requiredo2
  , MAX(CASE WHEN itemid = 50824 THEN valuenum ELSE NULL END) AS sodium
  , MAX(CASE WHEN itemid = 50825 THEN valuenum ELSE NULL END) AS temperature
  , MAX(CASE WHEN itemid = 50807 THEN value ELSE NULL END) AS comments
FROM mimic_hosp.labevents le
where le.ITEMID in
-- blood gases
(
    52028 -- specimen
  , 50801 -- aado2
  , 50802 -- base excess
  , 50803 -- bicarb
  , 50804 -- calc tot co2
  , 50805 -- carboxyhgb
  , 50806 -- chloride
  -- , 52390 -- chloride, WB CL-
  , 50807 -- comments
  , 50808 -- free calcium
  , 50809 -- glucose
  , 50810 -- hct
  , 50811 -- hgb
  , 50813 -- lactate
  , 50814 -- methemoglobin
  , 50815 -- o2 flow
  , 50816 -- fio2
  , 50817 -- o2 sat
  , 50818 -- pco2
  , 50819 -- peep
  , 50820 -- pH
  , 50821 -- pO2
  , 50822 -- potassium
  -- , 52408 -- potassium, WB K+
  , 50823 -- required O2
  , 50824 -- sodium
  -- , 52411 -- sodium, WB NA +
  , 50825 -- temperature
)
GROUP BY le.specimen_id
)
, stg_spo2 as
(
  select subject_id, charttime
    -- avg here is just used to group SpO2 by charttime
    , AVG(valuenum) as SpO2
  FROM mimic_icu.chartevents
  where ITEMID = 220277 -- O2 saturation pulseoxymetry
  and valuenum > 0 and valuenum <= 100
  group by subject_id, charttime
)
, stg_fio2 as
(
  select subject_id, charttime
    -- pre-process the FiO2s to ensure they are between 21-100%
    , max(
        case
          when valuenum > 0.2 and valuenum <= 1
            then valuenum * 100
          -- improperly input data - looks like O2 flow in litres
          when valuenum > 1 and valuenum < 20
            then null
          when valuenum >= 20 and valuenum <= 100
            then valuenum
      else null end
    ) as fio2_chartevents
  FROM mimic_icu.chartevents
  where ITEMID = 223835 -- Inspired O2 Fraction (FiO2)
  and valuenum > 0 and valuenum <= 100
  group by subject_id, charttime
)
, stg2 as
(
select bg.*
  , ROW_NUMBER() OVER (partition by bg.subject_id, bg.charttime order by s1.charttime DESC) as lastRowSpO2
  , s1.spo2
from bg
left join stg_spo2 s1
  -- same hospitalization
  on  bg.subject_id = s1.subject_id
  -- spo2 occurred at most 2 hours before this blood gas
  and s1.charttime between DATETIME_SUB(bg.charttime, INTERVAL '2'HOUR) and bg.charttime
where bg.po2 is not null
)
, stg3 as
(
select bg.*
  , ROW_NUMBER() OVER (partition by bg.subject_id, bg.charttime order by s2.charttime DESC) as lastRowFiO2
  , s2.fio2_chartevents
  -- create our specimen prediction
  ,  1/(1+exp(-(-0.02544
  +    0.04598 * po2
  + coalesce(-0.15356 * spo2             , -0.15356 *   97.49420 +    0.13429)
  + coalesce( 0.00621 * fio2_chartevents ,  0.00621 *   51.49550 +   -0.24958)
  + coalesce( 0.10559 * hemoglobin       ,  0.10559 *   10.32307 +    0.05954)
  + coalesce( 0.13251 * so2              ,  0.13251 *   93.66539 +   -0.23172)
  + coalesce(-0.01511 * pco2             , -0.01511 *   42.08866 +   -0.01630)
  + coalesce( 0.01480 * fio2             ,  0.01480 *   63.97836 +   -0.31142)
  + coalesce(-0.00200 * aado2            , -0.00200 *  442.21186 +   -0.01328)
  + coalesce(-0.03220 * bicarbonate      , -0.03220 *   22.96894 +   -0.06535)
  + coalesce( 0.05384 * totalco2         ,  0.05384 *   24.72632 +   -0.01405)
  + coalesce( 0.08202 * lactate          ,  0.08202 *    3.06436 +    0.06038)
  + coalesce( 0.10956 * ph               ,  0.10956 *    7.36233 +   -0.00617)
  + coalesce( 0.00848 * o2flow           ,  0.00848 *    7.59362 +   -0.35803)
  ))) as specimen_prob
from stg2 bg
left join stg_fio2 s2
  -- same patient
  on  bg.subject_id = s2.subject_id
  -- fio2 occurred at most 4 hours before this blood gas
  and s2.charttime between DATETIME_SUB(bg.charttime, INTERVAL '4' HOUR) and bg.charttime
  AND s2.fio2_chartevents > 0
where bg.lastRowSpO2 = 1 -- only the row with the most recent SpO2 (if no SpO2 found lastRowSpO2 = 1)
)
select
    stg3.subject_id
  , stg3.hadm_id
  , stg3.charttime
  -- raw data indicating sample type
  , specimen 
  -- prediction of specimen for obs missing the actual specimen
  , case
        when specimen is not null then specimen
        when specimen_prob > 0.75 then 'ART.'
      else null end as specimen_pred
  , specimen_prob
  -- oxygen related parameters
  , so2
  , po2
  , pco2
  , fio2_chartevents, fio2
  , aado2
  -- also calculate AADO2
  , case
      when  po2 is null
        OR pco2 is null
      THEN NULL
      WHEN fio2 IS NOT NULL
        -- multiple by 100 because fio2 is in a % but should be a fraction
        THEN (fio2/100) * (760 - 47) - (pco2/0.8) - po2
      WHEN fio2_chartevents IS NOT NULL
        THEN (fio2_chartevents/100) * (760 - 47) - (pco2/0.8) - po2
      else null
    end as aado2_calc
  , case
      when PO2 is null
        THEN NULL
      WHEN fio2 IS NOT NULL
       -- multiply by 100 because fio2 is in a % but should be a fraction
        then 100 * PO2/fio2
      WHEN fio2_chartevents IS NOT NULL
       -- multiply by 100 because fio2 is in a % but should be a fraction
        then 100 * PO2/fio2_chartevents
      else null
    end as pao2fio2ratio
  -- acid-base parameters
  , ph, baseexcess
  , bicarbonate, totalco2

  -- blood count parameters
  , hematocrit
  , hemoglobin
  , carboxyhemoglobin
  , methemoglobin

  -- chemistry
  , chloride, calcium
  , temperature
  , potassium, sodium
  , lactate
  , glucose

  -- ventilation stuff that's sometimes input
  -- , intubated, tidalvolume, ventilationrate, ventilator
  -- , peep, o2flow
  -- , requiredo2
from stg3
where lastRowFiO2 = 1 -- only the most recent FiO2
;

-- 血细胞分型
-- blood_differential
-- For reference, some common unit conversions:
-- 10^9/L == K/uL == 10^3/uL
create materialized view if not exists blood_differential as 
WITH blood_diff AS
(
SELECT
    MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , MAX(charttime) AS charttime
  , le.specimen_id
  -- create one set of columns for percentages, and one set of columns for counts
  -- we harmonize all count units into K/uL == 10^9/L
  -- counts have an "_abs" suffix, percentages do not

  -- absolute counts
  , MAX(CASE WHEN itemid in (51300, 51301, 51755) THEN valuenum ELSE NULL END) AS wbc
  , MAX(CASE WHEN itemid = 52069 THEN valuenum ELSE NULL END) AS basophils_abs
  -- 52073 in K/uL, 51199 in #/uL
  , MAX(CASE WHEN itemid = 52073 THEN valuenum WHEN itemid = 51199 THEN valuenum / 1000.0 ELSE NULL END) AS eosinophils_abs
  -- 51133 in K/uL, 52769 in #/uL
  , MAX(CASE WHEN itemid = 51133 THEN valuenum WHEN itemid = 52769 THEN valuenum / 1000.0 ELSE NULL END) AS lymphocytes_abs
  -- 52074 in K/uL, 51253 in #/uL
  , MAX(CASE WHEN itemid = 52074 THEN valuenum WHEN itemid = 51253 THEN valuenum / 1000.0 ELSE NULL END) AS monocytes_abs
  , MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs
  -- convert from #/uL to K/uL
  , MAX(CASE WHEN itemid = 51218 THEN valuenum / 1000.0 ELSE NULL END) AS granulocytes_abs

  -- percentages, equal to cell count / white blood cell count
  , MAX(CASE WHEN itemid = 51146 THEN valuenum ELSE NULL END) AS basophils
  , MAX(CASE WHEN itemid = 51200 THEN valuenum ELSE NULL END) AS eosinophils
  , MAX(CASE WHEN itemid in (51244, 51245) THEN valuenum ELSE NULL END) AS lymphocytes
  , MAX(CASE WHEN itemid = 51254 THEN valuenum ELSE NULL END) AS monocytes
  , MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils

  -- other cell count percentages
  , MAX(CASE WHEN itemid = 51143 THEN valuenum ELSE NULL END) AS atypical_lymphocytes
  , MAX(CASE WHEN itemid = 51144 THEN valuenum ELSE NULL END) AS bands
  , MAX(CASE WHEN itemid = 52135 THEN valuenum ELSE NULL END) AS immature_granulocytes
  , MAX(CASE WHEN itemid = 51251 THEN valuenum ELSE NULL END) AS metamyelocytes
  , MAX(CASE WHEN itemid = 51257 THEN valuenum ELSE NULL END) AS nrbc

  -- utility flags which determine whether imputation is possible
  , CASE
    -- WBC is available
    WHEN MAX(CASE WHEN itemid in (51300, 51301, 51755) THEN valuenum ELSE NULL END) > 0
    -- and we have at least one percentage from the diff
    -- sometimes the entire diff is 0%, which looks like bad data
    AND SUM(CASE WHEN itemid IN (51146, 51200, 51244, 51245, 51254, 51256) THEN valuenum ELSE NULL END) > 0
    THEN 1 ELSE 0 END AS impute_abs

FROM mimic_hosp.labevents le
WHERE le.itemid IN
(
    51146, -- basophils
    52069, -- Absolute basophil count
    51199, -- Eosinophil Count
    51200, -- Eosinophils
    52073, -- Absolute Eosinophil count
    51244, -- Lymphocytes
    51245, -- Lymphocytes, Percent
    51133, -- Absolute Lymphocyte Count
    52769, -- Absolute Lymphocyte Count
    51253, -- Monocyte Count
    51254, -- Monocytes
    52074, -- Absolute Monocyte Count
    51256, -- Neutrophils
    52075, -- Absolute Neutrophil Count
    51143, -- Atypical lymphocytes
    51144, -- Bands (%)
    51218, -- Granulocyte Count
    52135, -- Immature granulocytes (%)
    51251, -- Metamyelocytes
    51257,  -- Nucleated Red Cells

    -- wbc totals measured in K/uL
    51300, 51301, 51755
    -- 52220 (wbcp) is percentage

    -- below are point of care tests which are extremely infrequent and usually low quality
    -- 51697, -- Neutrophils (mmol/L)

    -- below itemid do not have data as of MIMIC-IV v1.0
    -- 51536, -- Absolute Lymphocyte Count
    -- 51537, -- Absolute Neutrophil
    -- 51690, -- Lymphocytes
    -- 52151, -- NRBC
)
AND valuenum IS NOT NULL
-- differential values cannot be negative
AND valuenum >= 0
GROUP BY le.specimen_id
)
SELECT 
subject_id, hadm_id, charttime, specimen_id

, wbc
-- impute absolute count if percentage & WBC is available
, ROUND((CASE
    WHEN basophils_abs IS NULL AND basophils IS NOT NULL AND impute_abs = 1
        THEN basophils * wbc / 100
    ELSE basophils_abs
END)::numeric, 4) AS basophils_abs
, ROUND(CASE
    WHEN eosinophils_abs IS NULL AND eosinophils IS NOT NULL AND impute_abs = 1
        THEN eosinophils * wbc / 100
    ELSE eosinophils_abs
END::numeric, 4) AS eosinophils_abs
, ROUND(CASE
    WHEN lymphocytes_abs IS NULL AND lymphocytes IS NOT NULL AND impute_abs = 1
        THEN lymphocytes * wbc / 100
    ELSE lymphocytes_abs
END::numeric, 4) AS lymphocytes_abs
, ROUND((CASE
    WHEN monocytes_abs IS NULL AND monocytes IS NOT NULL AND impute_abs = 1
        THEN monocytes * wbc / 100
    ELSE monocytes_abs
END)::numeric, 4) AS monocytes_abs
, ROUND((CASE
    WHEN neutrophils_abs IS NULL AND neutrophils IS NOT NULL AND impute_abs = 1
        THEN neutrophils * wbc / 100
    ELSE neutrophils_abs
END)::numeric, 4) AS neutrophils_abs

, basophils
, eosinophils
, lymphocytes
, monocytes
, neutrophils

-- impute bands/blasts?
, atypical_lymphocytes
, bands
, immature_granulocytes
, metamyelocytes
, nrbc
FROM blood_diff
;

-- 心功能检查指标
-- cardiac_marker
-- begin query that extracts the data
create materialized view if not exists cardiac_marker as 
SELECT
    MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , MAX(charttime) AS charttime
  , le.specimen_id
  -- convert from itemid into a meaningful column
  , MAX(CASE WHEN itemid = 51002 THEN value ELSE NULL END) AS troponin_i
  , MAX(CASE WHEN itemid = 51003 THEN value ELSE NULL END) AS troponin_t
  , MAX(CASE WHEN itemid = 50911 THEN valuenum ELSE NULL END) AS ck_mb
FROM mimic_hosp.labevents le
WHERE le.itemid IN
(
    -- 51002, -- Troponin I (troponin-I is not measured in MIMIC-IV)
    -- 52598, -- Troponin I, point of care, rare/poor quality
    51003, -- Troponin T
    50911  -- Creatinine Kinase, MB isoenzyme
)
GROUP BY le.specimen_id
;

-- 实验室生化检查
-- chemistry
-- extract chemistry labs
-- excludes point of care tests (very rare)
-- blood gas measurements are *not* included in this query
-- instead they are in bg.sql
create materialized view if not exists chemistry as 
SELECT 
    MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , MAX(charttime) AS charttime
  , le.specimen_id
  -- convert from itemid into a meaningful column
  , MAX(CASE WHEN itemid = 50862 AND valuenum <=    10 THEN valuenum ELSE NULL END) AS albumin
  , MAX(CASE WHEN itemid = 50930 AND valuenum <=    10 THEN valuenum ELSE NULL END) AS globulin
  , MAX(CASE WHEN itemid = 50976 AND valuenum <=    20 THEN valuenum ELSE NULL END) AS total_protein
  , MAX(CASE WHEN itemid = 50868 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS aniongap
  , MAX(CASE WHEN itemid = 50882 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS bicarbonate
  , MAX(CASE WHEN itemid = 51006 AND valuenum <=   300 THEN valuenum ELSE NULL END) AS bun
  , MAX(CASE WHEN itemid = 50893 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS calcium
  , MAX(CASE WHEN itemid = 50902 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS chloride
  , MAX(CASE WHEN itemid = 50912 AND valuenum <=   150 THEN valuenum ELSE NULL END) AS creatinine
  , MAX(CASE WHEN itemid = 50931 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose
  , MAX(CASE WHEN itemid = 50983 AND valuenum <=   200 THEN valuenum ELSE NULL END) AS sodium
  , MAX(CASE WHEN itemid = 50971 AND valuenum <=    30 THEN valuenum ELSE NULL END) AS potassium
FROM mimic_hosp.labevents le
WHERE le.itemid IN
(
  -- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
  50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
  50930, -- Globulin
  50976, -- Total protein
  50868, -- ANION GAP | CHEMISTRY | BLOOD | 769895
  -- 52456, -- Anion gap, point of care test
  50882, -- BICARBONATE | CHEMISTRY | BLOOD | 780733
  50893, -- Calcium
  50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
  -- 52502, Creatinine, point of care
  50902, -- CHLORIDE | CHEMISTRY | BLOOD | 795568
  50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
  -- 52525, Glucose, point of care
  50971, -- POTASSIUM | CHEMISTRY | BLOOD | 845825
  -- 52566, -- Potassium, point of care
  50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
  -- 52579, -- Sodium, point of care
  51006  -- UREA NITROGEN | CHEMISTRY | BLOOD | 791925
  -- 52603, Urea, point of care
)
AND valuenum IS NOT NULL
-- lab values cannot be 0 and cannot be negative
-- .. except anion gap.
AND (valuenum > 0 OR itemid = 50868)
GROUP BY le.specimen_id
;

-- 凝血功能
-- coagulation
create materialized view if not exists coagulation as 
SELECT
    MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , MAX(charttime) AS charttime
  , le.specimen_id
  -- convert from itemid into a meaningful column
  , MAX(CASE WHEN itemid = 51196 THEN valuenum ELSE NULL END) AS d_dimer
  , MAX(CASE WHEN itemid = 51214 THEN valuenum ELSE NULL END) AS fibrinogen
  , MAX(CASE WHEN itemid = 51297 THEN valuenum ELSE NULL END) AS thrombin
  , MAX(CASE WHEN itemid = 51237 THEN valuenum ELSE NULL END) AS inr
  , MAX(CASE WHEN itemid = 51274 THEN valuenum ELSE NULL END) AS pt
  , MAX(CASE WHEN itemid = 51275 THEN valuenum ELSE NULL END) AS ptt
FROM mimic_hosp.labevents le
WHERE le.itemid IN
(
    -- 51149, 52750, 52072, 52073 -- Bleeding Time, no data as of MIMIC-IV v0.4
    51196, -- D-Dimer
    51214, -- Fibrinogen
    -- 51280, 52893, -- Reptilase Time, no data as of MIMIC-IV v0.4
    -- 51281, 52161, -- Reptilase Time Control, no data as of MIMIC-IV v0.4
    51297, -- thrombin
    51237, -- INR
    51274, -- PT
    51275 -- PTT
)
AND valuenum IS NOT NULL
GROUP BY le.specimen_id
;

-- 全血细胞计数
-- complete_blood_count
-- begin query that extracts the data
create materialized view if not exists complete_blood_count as 
SELECT
    MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , MAX(charttime) AS charttime
  , le.specimen_id
  -- convert from itemid into a meaningful column
  , MAX(CASE WHEN itemid = 51221 THEN valuenum ELSE NULL END) AS hematocrit
  , MAX(CASE WHEN itemid = 51222 THEN valuenum ELSE NULL END) AS hemoglobin
  , MAX(CASE WHEN itemid = 51248 THEN valuenum ELSE NULL END) AS mch
  , MAX(CASE WHEN itemid = 51249 THEN valuenum ELSE NULL END) AS mchc
  , MAX(CASE WHEN itemid = 51250 THEN valuenum ELSE NULL END) AS mcv
  , MAX(CASE WHEN itemid = 51265 THEN valuenum ELSE NULL END) AS platelet
  , MAX(CASE WHEN itemid = 51279 THEN valuenum ELSE NULL END) AS rbc
  , MAX(CASE WHEN itemid = 51277 THEN valuenum ELSE NULL END) AS rdw
  , MAX(CASE WHEN itemid = 52159 THEN valuenum ELSE NULL END) AS rdwsd
  , MAX(CASE WHEN itemid = 51301 THEN valuenum ELSE NULL END) AS wbc
FROM mimic_hosp.labevents le
WHERE le.itemid IN
(
    51221, -- hematocrit
    51222, -- hemoglobin
    51248, -- MCH
    51249, -- MCHC
    51250, -- MCV
    51265, -- platelets
    51279, -- RBC
    51277, -- RDW
    52159, -- RDW SD
    51301  -- WBC

)
AND valuenum IS NOT NULL
-- lab values cannot be 0 and cannot be negative
AND valuenum > 0
GROUP BY le.specimen_id
;

-- 肌酐基线
-- creatinine_baseline
-- This query extracts the serum creatinine baselines of adult patients on each hospital admission.
-- The baseline is determined by the following rules:
--     i. if the lowest creatinine value during this admission is normal (<1.1), then use the value
--     ii. if the patient is diagnosed with chronic kidney disease (CKD), then use the lowest creatinine value during the admission, although it may be rather large.
--     iii. Otherwise, we estimate the baseline using the Simplified MDRD Formula:
--          eGFR = 186 × Scr^(-1.154) × Age^(-0.203) × 0.742Female
--     Let eGFR = 75. Scr = [ 75 / 186 / Age^(-0.203) / (0.742Female) ] ^ (1/-1.154)
create materialized view if not exists creatinine_baseline as 
WITH p as
(
    SELECT 
        ag.subject_id
        , ag.hadm_id
        , ag.age
        , p.gender
        , CASE WHEN p.gender='F' THEN 
            POWER(75.0 / 186.0 / POWER(ag.age, -0.203) / 0.742, -1/1.154)
            ELSE 
            POWER(75.0 / 186.0 / POWER(ag.age, -0.203), -1/1.154)
            END 
            AS MDRD_est
    FROM age ag
    LEFT JOIN mimic_hosp.patients p
    ON ag.subject_id = p.subject_id
    WHERE ag.age >= 18
)
, lab as
(
    SELECT 
        hadm_id
        , MIN(creatinine) AS scr_min
    FROM chemistry
    GROUP BY hadm_id
)
, ckd as 
(
    SELECT hadm_id, MAX(1) AS CKD_flag
    FROM mimic_hosp.diagnoses_icd
    WHERE 
        (
            SUBSTR(icd_code, 1, 3) = '585'
            AND 
            icd_version = 9
        )
    OR 
        (
            SUBSTR(icd_code, 1, 3) = 'N18'
            AND 
            icd_version = 10
        )
    GROUP BY 1
)
SELECT 
    p.hadm_id
    , p.gender
    , p.age
    , lab.scr_min
    , COALESCE(ckd.ckd_flag, 0) AS ckd
    , p.MDRD_est
    , CASE 
    WHEN lab.scr_min<=1.1 THEN scr_min
    WHEN ckd.ckd_flag=1 THEN scr_min
    ELSE MDRD_est END AS scr_baseline
FROM p
LEFT JOIN lab
ON p.hadm_id = lab.hadm_id
LEFT JOIN ckd
ON p.hadm_id = ckd.hadm_id
;

-- 酶
-- enzyme
-- begin query that extracts the data
create materialized view if not exists enzyme as 
SELECT
    MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , MAX(charttime) AS charttime
  , le.specimen_id
  -- convert from itemid into a meaningful column
  , MAX(CASE WHEN itemid = 50861 THEN valuenum ELSE NULL END) AS alt
  , MAX(CASE WHEN itemid = 50863 THEN valuenum ELSE NULL END) AS alp
  , MAX(CASE WHEN itemid = 50878 THEN valuenum ELSE NULL END) AS ast
  , MAX(CASE WHEN itemid = 50867 THEN valuenum ELSE NULL END) AS amylase
  , MAX(CASE WHEN itemid = 50885 THEN valuenum ELSE NULL END) AS bilirubin_total
  , MAX(CASE WHEN itemid = 50883 THEN valuenum ELSE NULL END) AS bilirubin_direct
  , MAX(CASE WHEN itemid = 50884 THEN valuenum ELSE NULL END) AS bilirubin_indirect
  , MAX(CASE WHEN itemid = 50910 THEN valuenum ELSE NULL END) AS ck_cpk
  , MAX(CASE WHEN itemid = 50911 THEN valuenum ELSE NULL END) AS ck_mb
  , MAX(CASE WHEN itemid = 50927 THEN valuenum ELSE NULL END) AS ggt
  , MAX(CASE WHEN itemid = 50954 THEN valuenum ELSE NULL END) AS ld_ldh
FROM mimic_hosp.labevents le
WHERE le.itemid IN
(
    50861, -- Alanine transaminase (ALT)
    50863, -- Alkaline phosphatase (ALP)
    50878, -- Aspartate transaminase (AST)
    50867, -- Amylase
    50885, -- total bili
    50884, -- indirect bili
    50883, -- direct bili
    50910, -- ck_cpk
    50911, -- CK-MB
    50927, -- Gamma Glutamyltransferase (GGT)
    50954 -- ld_ldh
)
AND valuenum IS NOT NULL
-- lab values cannot be 0 and cannot be negative
AND valuenum > 0
GROUP BY le.specimen_id
;

-- 格拉斯哥评分（神经系统功能评分）
-- gcs
-- This query extracts the Glasgow Coma Scale, a measure of neurological function.
-- The query has a few special rules:
--    (1) The verbal component can be set to 0 if the patient is ventilated.
--    This is corrected to 5 - the overall GCS is set to 15 in these cases.
--    (2) Often only one of three components is documented. The other components
--    are carried forward.

-- ITEMIDs used:

-- METAVISION
--    223900 GCS - Verbal Response
--    223901 GCS - Motor Response
--    220739 GCS - Eye Opening

-- Note:
--  The GCS for sedated patients is defaulted to 15 in this code.
--  This is in line with how the data is meant to be collected.
--  e.g., from the SAPS II publication:
--    For sedated patients, the Glasgow Coma Score before sedation was used.
--    This was ascertained either from interviewing the physician who ordered the sedation,
--    or by reviewing the patient's medical record.
create materialized view if not exists gcs as 
with base as
(
  select 
    subject_id
  , ce.stay_id, ce.charttime
  -- pivot each value into its own column
  , max(case when ce.ITEMID = 223901 then ce.valuenum else null end) as GCSMotor
  , max(case
      when ce.ITEMID = 223900 and ce.VALUE = 'No Response-ETT' then 0
      when ce.ITEMID = 223900 then ce.valuenum
      else null 
    end) as GCSVerbal
  , max(case when ce.ITEMID = 220739 then ce.valuenum else null end) as GCSEyes
  -- convert the data into a number, reserving a value of 0 for ET/Trach
  , max(case
      -- endotrach/vent is assigned a value of 0
      -- flag it here to later parse specially
      when ce.ITEMID = 223900 and ce.VALUE = 'No Response-ETT' then 1 -- metavision
    else 0 end)
    as endotrachflag
  , ROW_NUMBER ()
          OVER (PARTITION BY ce.stay_id ORDER BY ce.charttime ASC) as rn
  from mimic_icu.chartevents ce
  -- Isolate the desired GCS variables
  where ce.ITEMID in
  (
    -- GCS components, Metavision
    223900, 223901, 220739
  )
  group by ce.subject_id, ce.stay_id, ce.charttime
)
, gcs as (
  select b.*
  , b2.GCSVerbal as GCSVerbalPrev
  , b2.GCSMotor as GCSMotorPrev
  , b2.GCSEyes as GCSEyesPrev
  -- Calculate GCS, factoring in special case when they are intubated and prev vals
  -- note that the coalesce are used to implement the following if:
  --  if current value exists, use it
  --  if previous value exists, use it
  --  otherwise, default to normal
  , case
      -- replace GCS during sedation with 15
      when b.GCSVerbal = 0
        then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0
        then 15
      -- if previously they were intub, but they aren't now, do not use previous GCS values
      when b2.GCSVerbal = 0
        then
            coalesce(b.GCSMotor,6)
          + coalesce(b.GCSVerbal,5)
          + coalesce(b.GCSEyes,4)
      -- otherwise, add up score normally, imputing previous value if none available at current time
      else
            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end as GCS

  from base b
  -- join to itself within 6 hours to get previous value
  left join base b2
    on b.stay_id = b2.stay_id
    and b.rn = b2.rn+1
    and b2.charttime > DATETIME_ADD(b.charttime, INTERVAL '6' HOUR)
)
-- combine components with previous within 6 hours
-- filter down to cohort which is not excluded
-- truncate charttime to the hour
, gcs_stg as
(
  select
    subject_id
  , gs.stay_id, gs.charttime
  , GCS
  , coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
  , coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
  , coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
  , case when coalesce(GCSMotor,GCSMotorPrev) is null then 0 else 1 end
  + case when coalesce(GCSVerbal,GCSVerbalPrev) is null then 0 else 1 end
  + case when coalesce(GCSEyes,GCSEyesPrev) is null then 0 else 1 end
    as components_measured
  , EndoTrachFlag
  from gcs gs
)
-- priority is:
--  (i) complete data, (ii) non-sedated GCS, (iii) lowest GCS, (iv) charttime
, gcs_priority as
(
  select 
      subject_id
    , stay_id
    , charttime
    , gcs
    , gcsmotor
    , gcsverbal
    , gcseyes
    , EndoTrachFlag
    , ROW_NUMBER() over
      (
        PARTITION BY stay_id, charttime
        ORDER BY components_measured DESC, endotrachflag, gcs, charttime DESC
      ) as rn
  from gcs_stg
)
select
  gs.subject_id
  , gs.stay_id
  , gs.charttime
  , GCS AS gcs
  , GCSMotor AS gcs_motor
  , GCSVerbal AS gcs_verbal
  , GCSEyes AS gcs_eyes
  , EndoTrachFlag AS gcs_unable
from gcs_priority gs
where rn = 1
;

-- 身高
-- height
-- prep height
create materialized view if not exists height as 
WITH ht_in AS
(
  SELECT 
    c.subject_id, c.stay_id, c.charttime
    -- Ensure that all heights are in centimeters
    , ROUND((c.valuenum * 2.54 ):: numeric, 2) AS height
    , c.valuenum as height_orig
  FROM mimic_icu.chartevents c
  WHERE c.valuenum IS NOT NULL
  -- Height (measured in inches)
  AND c.itemid = 226707
)
, ht_cm AS
(
  SELECT 
    c.subject_id, c.stay_id, c.charttime
    -- Ensure that all heights are in centimeters
    , ROUND(c.valuenum :: numeric, 2) AS height
  FROM mimic_icu.chartevents c
  WHERE c.valuenum IS NOT NULL
  -- Height cm
  AND c.itemid = 226730
)
-- merge cm/height, only take 1 value per charted row
, ht_stg0 AS
(
  SELECT
  COALESCE(h1.subject_id, h1.subject_id) as subject_id
  , COALESCE(h1.stay_id, h1.stay_id) AS stay_id
  , COALESCE(h1.charttime, h1.charttime) AS charttime
  , COALESCE(h1.height, h2.height) as height
  FROM ht_cm h1
  FULL OUTER JOIN ht_in h2
    ON h1.subject_id = h2.subject_id
    AND h1.charttime = h2.charttime
)
SELECT subject_id, stay_id, charttime, height
FROM ht_stg0
WHERE height IS NOT NULL
-- filter out bad heights
AND height > 120 AND height < 230;

-- 颅内压
-- icp
create materialized view if not exists icp as 
with ce as
(
  select
  ce.subject_id
  , ce.stay_id
  , ce.charttime
  -- TODO: handle high ICPs when monitoring two ICPs
  , case when valuenum > 0 and valuenum < 100 then valuenum else null end as icp
  FROM mimic_icu.chartevents ce
  -- exclude rows marked as error
  where ce.itemid in
  (
    220765 -- Intra Cranial Pressure -- 92306
  , 227989 -- Intra Cranial Pressure #2 -- 1052
  )
)
select
  ce.subject_id
  , ce.stay_id
  , ce.charttime
  , MAX(icp) as icp
from ce
group by ce.subject_id, ce.stay_id, ce.charttime
;

-- 炎症
-- inflammation
create materialized view if not exists inflammation as
SELECT
    MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , MAX(charttime) AS charttime
  , le.specimen_id
  -- convert from itemid into a meaningful column
  , MAX(CASE WHEN itemid = 50889 THEN valuenum ELSE NULL END) AS crp
  -- , CAST(NULL AS NUMERIC) AS il6
  -- , CAST(NULL AS NUMERIC) AS procalcitonin
FROM mimic_hosp.labevents le
WHERE le.itemid IN
(
    50889 -- crp
    -- 51652 -- high sensitivity CRP
)
AND valuenum IS NOT NULL
-- lab values cannot be 0 and cannot be negative
AND valuenum > 0
GROUP BY le.specimen_id
;

-- 氧气输送
-- oxygen_delivery
create materialized view if not exists oxygen_delivery as 
with ce_stg1 as
(
  SELECT
      ce.subject_id
    , ce.stay_id
    , ce.charttime
    , CASE
        -- merge o2 flows into a single row
        WHEN itemid IN (223834, 227582, 224691) THEN 223834
      ELSE itemid END AS itemid
    , value
    , valuenum
    , valueuom
    , storetime
  FROM mimic_icu.chartevents ce
  WHERE ce.value IS NOT NULL
  AND ce.itemid IN
  (
      223834 -- o2 flow
    , 227582 -- bipap o2 flow
    , 224691 -- Flow Rate (L)
    -- additional o2 flow is its own column
    , 227287 -- additional o2 flow
  )
)
, ce_stg2 AS
(
  select
    ce.subject_id
    , ce.stay_id
    , ce.charttime
    , itemid
    , value
    , valuenum
    , valueuom
    -- retain only 1 row per charttime
    -- prioritizing the last documented value
    -- primarily used to subselect o2 flows
    , ROW_NUMBER() OVER (PARTITION BY subject_id, charttime, itemid ORDER BY storetime DESC) as rn
  FROM ce_stg1 ce
)
, o2 AS
(
    -- The below ITEMID can have multiple entires for charttime/storetime
    -- These are totally valid entries, and should be retained in derived tables.
    --   224181 -- Small Volume Neb Drug #1              | Respiratory             | Text       | chartevents
    -- , 227570 -- Small Volume Neb Drug/Dose #1         | Respiratory             | Text       | chartevents
    -- , 224833 -- SBT Deferred                          | Respiratory             | Text       | chartevents
    -- , 224716 -- SBT Stopped                           | Respiratory             | Text       | chartevents
    -- , 224740 -- RSBI Deferred                         | Respiratory             | Text       | chartevents
    -- , 224829 -- Trach Tube Type                       | Respiratory             | Text       | chartevents
    -- , 226732 -- O2 Delivery Device(s)                 | Respiratory             | Text       | chartevents
    -- , 226873 -- Inspiratory Ratio                     | Respiratory             | Numeric    | chartevents
    -- , 226871 -- Expiratory Ratio                      | Respiratory             | Numeric    | chartevents
    -- maximum of 4 o2 devices on at once
    SELECT
        subject_id
        , stay_id
        , charttime
        , itemid
        , value AS o2_device
    , ROW_NUMBER() OVER (PARTITION BY subject_id, charttime, itemid ORDER BY value) as rn
    FROM mimic_icu.chartevents
    WHERE itemid = 226732 -- oxygen delivery device(s)
)
, stg AS
(
    select
      COALESCE(ce.subject_id, o2.subject_id) AS subject_id
    , COALESCE(ce.stay_id, o2.stay_id) AS stay_id
    , COALESCE(ce.charttime, o2.charttime) AS charttime
    , COALESCE(ce.itemid, o2.itemid) AS itemid
    , ce.value
    , ce.valuenum
    , o2.o2_device
    , o2.rn
    from ce_stg2 ce
    FULL OUTER JOIN o2
      ON ce.subject_id = o2.subject_id
      AND ce.charttime = o2.charttime
    -- limit to 1 row per subject_id/charttime/itemid from ce_stg2
    WHERE ce.rn = 1
)
SELECT
    subject_id
    , MAX(stay_id) AS stay_id
    , charttime
    , MAX(CASE WHEN itemid = 223834 THEN valuenum ELSE NULL END) AS o2_flow
    , MAX(CASE WHEN itemid = 227287 THEN valuenum ELSE NULL END) AS o2_flow_additional
    -- ensure we retain all o2 devices for the patient
    , MAX(CASE WHEN rn = 1 THEN o2_device ELSE NULL END) AS o2_delivery_device_1
    , MAX(CASE WHEN rn = 2 THEN o2_device ELSE NULL END) AS o2_delivery_device_2
    , MAX(CASE WHEN rn = 3 THEN o2_device ELSE NULL END) AS o2_delivery_device_3
    , MAX(CASE WHEN rn = 4 THEN o2_device ELSE NULL END) AS o2_delivery_device_4
FROM stg
GROUP BY subject_id, charttime
;

-- 心率
-- rhythm
-- Heart rhythm related documentation
create materialized view if not exists rhythm as 
select 
    ce.subject_id
  , ce.charttime
  , MAX(case when itemid = 220048 THEN value ELSE NULL END) AS heart_rhythm
  , MAX(case when itemid = 224650 THEN value ELSE NULL END) AS ectopy_type
  , MAX(case when itemid = 224651 THEN value ELSE NULL END) AS ectopy_frequency
  , MAX(case when itemid = 226479 THEN value ELSE NULL END) AS ectopy_type_secondary
  , MAX(case when itemid = 226480 THEN value ELSE NULL END) AS ectopy_frequency_secondary
FROM mimic_icu.chartevents ce
where ce.stay_id IS NOT NULL
and ce.itemid in
(
220048, -- Heart Rhythm
224650, -- Ectopy Type 1
224651, -- Ectopy Frequency 1
226479, -- Ectopy Type 2
226480  -- Ectopy Frequency 2
)
GROUP BY ce.subject_id, ce.charttime
;

-- 尿量
-- urine_output 
create materialized view if not exists urine_output as 
select
  stay_id
  , charttime
  , sum(urineoutput) as urineoutput
from
(
    select
    -- patient identifiers
    oe.stay_id
    , oe.charttime
    -- volumes associated with urine output ITEMIDs
    -- note we consider input of GU irrigant as a negative volume
    -- GU irrigant volume in usually has a corresponding volume out
    -- so the net is often 0, despite large irrigant volumes
    , case
        when oe.itemid = 227488 and oe.value > 0 then -1*oe.value
        else oe.value
    end as urineoutput
    from mimic_icu.outputevents oe
    where itemid in
    (
    226559, -- Foley
    226560, -- Void
    226561, -- Condom Cath
    226584, -- Ileoconduit
    226563, -- Suprapubic
    226564, -- R Nephrostomy
    226565, -- L Nephrostomy
    226567, -- Straight Cath
    226557, -- R Ureteral Stent
    226558, -- L Ureteral Stent
    227488, -- GU Irrigant Volume In
    227489  -- GU Irrigant/Urine Volume Out
    )
) uo
group by stay_id, charttime
;

-- 排尿频率
-- urine_output_rate
-- attempt to calculate urine output per hour
-- rate/hour is the interpretable measure of kidney function
-- though it is difficult to estimate from aperiodic point measures
-- first we get the earliest heart rate documented for the stay
create materialized view if not exists urine_output_rate as 
WITH tm AS
(
    SELECT ie.stay_id
      , min(charttime) AS intime_hr
      , max(charttime) AS outtime_hr
    FROM mimic_icu.icustays ie
    INNER JOIN mimic_icu.chartevents ce
      ON ie.stay_id = ce.stay_id
      AND ce.itemid = 220045
      AND ce.charttime > DATETIME_SUB(ie.intime, interval '1' MONTH)
      AND ce.charttime < DATETIME_ADD(ie.outtime, interval '1' MONTH)
    GROUP BY ie.stay_id
)
-- now calculate time since last UO measurement
, uo_tm AS
(
    SELECT tm.stay_id
    , CASE
        WHEN LAG(charttime) OVER W IS NULL
        THEN DATETIME_DIFF(charttime, intime_hr, 'MINUTE')
    ELSE DATETIME_DIFF(charttime, LAG(charttime) OVER W, 'MINUTE')
    END AS tm_since_last_uo
    , uo.charttime
    , uo.urineoutput
    FROM tm
    INNER JOIN urine_output uo
        ON tm.stay_id = uo.stay_id
    WINDOW W AS (PARTITION BY tm.stay_id ORDER BY charttime)
)
, ur_stg as
(
  select io.stay_id, io.charttime
  -- we have joined each row to all rows preceding within 24 hours
  -- we can now sum these rows to get total UO over the last 24 hours
  -- we can use case statements to restrict it to only the last 6/12 hours
  -- therefore we have three sums:
  -- 1) over a 6 hour period
  -- 2) over a 12 hour period
  -- 3) over a 24 hour period
  , SUM(DISTINCT io.urineoutput) AS uo
  -- note that we assume data charted at charttime corresponds to 1 hour of UO
  -- therefore we use '5' and '11' to restrict the period, rather than 6/12
  -- this assumption may overestimate UO rate when documentation is done less than hourly
  , sum(case when DATETIME_DIFF(io.charttime, iosum.charttime, 'HOUR') <= 5
      then iosum.urineoutput
    else null end) as urineoutput_6hr
  , SUM(CASE WHEN DATETIME_DIFF(io.charttime, iosum.charttime, 'HOUR') <= 5
        THEN iosum.tm_since_last_uo
    ELSE NULL END)/60.0 AS uo_tm_6hr
  , sum(case when DATETIME_DIFF(io.charttime, iosum.charttime, 'HOUR') <= 11
      then iosum.urineoutput
    else null end) as urineoutput_12hr
  , SUM(CASE WHEN DATETIME_DIFF(io.charttime, iosum.charttime, 'HOUR') <= 11
        THEN iosum.tm_since_last_uo
    ELSE NULL END)/60.0 AS uo_tm_12hr
  -- 24 hours
  , sum(iosum.urineoutput) as urineoutput_24hr
  , SUM(iosum.tm_since_last_uo)/60.0 AS uo_tm_24hr

  from uo_tm io
  -- this join gives you all UO measurements over a 24 hour period
  left join uo_tm iosum
    on  io.stay_id = iosum.stay_id
    and io.charttime >= iosum.charttime
    and io.charttime <= (DATETIME_ADD(iosum.charttime, INTERVAL '23' HOUR))
  group by io.stay_id, io.charttime
)
select
  ur.stay_id
, ur.charttime
, wd.weight
, ur.uo
, ur.urineoutput_6hr
, ur.urineoutput_12hr
, ur.urineoutput_24hr
, CASE WHEN uo_tm_6hr >= 6 THEN ROUND(CAST((ur.urineoutput_6hr/wd.weight/uo_tm_6hr) AS NUMERIC), 4) END AS uo_mlkghr_6hr
, CASE WHEN uo_tm_12hr >= 12 THEN ROUND(CAST((ur.urineoutput_12hr/wd.weight/uo_tm_12hr) AS NUMERIC), 4) END AS uo_mlkghr_12hr
, CASE WHEN uo_tm_24hr >= 24 THEN ROUND(CAST((ur.urineoutput_24hr/wd.weight/uo_tm_24hr) AS NUMERIC), 4) END AS uo_mlkghr_24hr
-- time of earliest UO measurement that was used to calculate the rate
, ROUND(uo_tm_6hr, 2) AS uo_tm_6hr
, ROUND(uo_tm_12hr, 2) AS uo_tm_12hr
, ROUND(uo_tm_24hr, 2) AS uo_tm_24hr
from ur_stg ur
LEFT JOIN weight_durations wd
    ON ur.stay_id = wd.stay_id
    AND ur.charttime > wd.starttime
    AND ur.charttime <= wd.endtime
    AND wd.weight > 0
;

-- 通气 
-- ventilator_setting
create materialized view if not exists ventilator_setting as 
with ce as
(
  SELECT
      ce.subject_id
    , ce.stay_id
    , ce.charttime
    , itemid
    -- TODO: clean
    , value
    , case
        -- begin fio2 cleaning
        when itemid = 223835
        then
            case
                when valuenum >= 0.20 and valuenum <= 1
                    then valuenum * 100
                -- improperly input data - looks like O2 flow in litres
                when valuenum > 1 and valuenum < 20
                    then null
                when valuenum >= 20 and valuenum <= 100
                    then valuenum
            ELSE NULL END
        -- end of fio2 cleaning
        -- begin peep cleaning
        WHEN itemid in (220339, 224700)
        THEN
          CASE
            WHEN valuenum > 100 THEN NULL
            WHEN valuenum < 0 THEN NULL
          ELSE valuenum END
        -- end peep cleaning
    ELSE valuenum END AS valuenum
    , valueuom
    , storetime
  FROM mimic_icu.chartevents ce
  where ce.value IS NOT NULL
  AND ce.stay_id IS NOT NULL
  AND ce.itemid IN
  (
      224688 -- Respiratory Rate (Set)
    , 224689 -- Respiratory Rate (spontaneous)
    , 224690 -- Respiratory Rate (Total)
    , 224687 -- minute volume
    , 224685, 224684, 224686 -- tidal volume
    , 224696 -- PlateauPressure
    , 220339, 224700 -- PEEP
    , 223835 -- fio2
    , 223849 -- vent mode
    , 229314 -- vent mode (Hamilton)
    , 223848 -- vent type
  )
)
SELECT
      subject_id
    , MAX(stay_id) AS stay_id
    , charttime
    , MAX(CASE WHEN itemid = 224688 THEN valuenum ELSE NULL END) AS respiratory_rate_set
    , MAX(CASE WHEN itemid = 224690 THEN valuenum ELSE NULL END) AS respiratory_rate_total
    , MAX(CASE WHEN itemid = 224689 THEN valuenum ELSE NULL END) AS respiratory_rate_spontaneous
    , MAX(CASE WHEN itemid = 224687 THEN valuenum ELSE NULL END) AS minute_volume
    , MAX(CASE WHEN itemid = 224684 THEN valuenum ELSE NULL END) AS tidal_volume_set
    , MAX(CASE WHEN itemid = 224685 THEN valuenum ELSE NULL END) AS tidal_volume_observed
    , MAX(CASE WHEN itemid = 224686 THEN valuenum ELSE NULL END) AS tidal_volume_spontaneous
    , MAX(CASE WHEN itemid = 224696 THEN valuenum ELSE NULL END) AS plateau_pressure
    , MAX(CASE WHEN itemid in (220339, 224700) THEN valuenum ELSE NULL END) AS peep
    , MAX(CASE WHEN itemid = 223835 THEN valuenum ELSE NULL END) AS fio2
    , MAX(CASE WHEN itemid = 223849 THEN value ELSE NULL END) AS ventilator_mode
    , MAX(CASE WHEN itemid = 229314 THEN value ELSE NULL END) AS ventilator_mode_hamilton
    , MAX(CASE WHEN itemid = 223848 THEN value ELSE NULL END) AS ventilator_type
FROM ce
GROUP BY subject_id, charttime
;

-- 生命体征
-- vitalsign
-- This query pivots the vital signs for the entire patient stay.
-- Vital signs include heart rate, blood pressure, respiration rate, and temperature
create materialized view if not exists vitalsign as 
select
    ce.subject_id
  , ce.stay_id
  , ce.charttime
  , AVG(case when itemid in (220045) and valuenum > 0 and valuenum < 300 then valuenum else null end) as heart_rate
  , AVG(case when itemid in (220179,220050) and valuenum > 0 and valuenum < 400 then valuenum else null end) as sbp
  , AVG(case when itemid in (220180,220051) and valuenum > 0 and valuenum < 300 then valuenum else null end) as dbp
  , AVG(case when itemid in (220052,220181,225312) and valuenum > 0 and valuenum < 300 then valuenum else null end) as mbp
  , AVG(case when itemid = 220179 and valuenum > 0 and valuenum < 400 then valuenum else null end) as sbp_ni
  , AVG(case when itemid = 220180 and valuenum > 0 and valuenum < 300 then valuenum else null end) as dbp_ni
  , AVG(case when itemid = 220181 and valuenum > 0 and valuenum < 300 then valuenum else null end) as mbp_ni
  , AVG(case when itemid in (220210,224690) and valuenum > 0 and valuenum < 70 then valuenum else null end) as resp_rate
  , ROUND(
      AVG(case when itemid in (223761) and valuenum > 70 and valuenum < 120 then (valuenum-32)/1.8 -- converted to degC in valuenum call
              when itemid in (223762) and valuenum > 10 and valuenum < 50  then valuenum else null end)::numeric
    , 2) as temperature
  , MAX(CASE WHEN itemid = 224642 THEN value ELSE NULL END) AS temperature_site
  , AVG(case when itemid in (220277) and valuenum > 0 and valuenum <= 100 then valuenum else null end) as spo2
  , AVG(case when itemid in (225664,220621,226537) and valuenum > 0 then valuenum else null end) as glucose
  FROM mimic_icu.chartevents ce
  where ce.stay_id IS NOT NULL
  and ce.itemid in
  (
    220045, -- Heart Rate
    225309, -- ART BP Systolic
    225310, -- ART BP Diastolic
    225312, -- ART BP Mean
    220050, -- Arterial Blood Pressure systolic
    220051, -- Arterial Blood Pressure diastolic
    220052, -- Arterial Blood Pressure mean
    220179, -- Non Invasive Blood Pressure systolic
    220180, -- Non Invasive Blood Pressure diastolic
    220181, -- Non Invasive Blood Pressure mean
    220210, -- Respiratory Rate
    224690, -- Respiratory Rate (Total)
    220277, -- SPO2, peripheral
    -- GLUCOSE, both lab and fingerstick
    225664, -- Glucose finger stick
    220621, -- Glucose (serum)
    226537, -- Glucose (whole blood)
    -- TEMPERATURE
    223762, -- "Temperature Celsius"
    223761,  -- "Temperature Fahrenheit"
    224642 -- Temperature Site
    -- 226329 -- Blood Temperature CCO (C)
)
group by ce.subject_id, ce.stay_id, ce.charttime
;


-- 抗生素
-- antibiotic
create materialized view if not exists antibiotic as 
with abx as
(
  SELECT DISTINCT
    drug
    , route
    , case
      when lower(drug) like '%adoxa%' then 1
      when lower(drug) like '%ala-tet%' then 1
      when lower(drug) like '%alodox%' then 1
      when lower(drug) like '%amikacin%' then 1
      when lower(drug) like '%amikin%' then 1
      when lower(drug) like '%amoxicill%' then 1
      when lower(drug) like '%amphotericin%' then 1
      when lower(drug) like '%anidulafungin%' then 1
      when lower(drug) like '%ancef%' then 1
      when lower(drug) like '%clavulanate%' then 1
      when lower(drug) like '%ampicillin%' then 1
      when lower(drug) like '%augmentin%' then 1
      when lower(drug) like '%avelox%' then 1
      when lower(drug) like '%avidoxy%' then 1
      when lower(drug) like '%azactam%' then 1
      when lower(drug) like '%azithromycin%' then 1
      when lower(drug) like '%aztreonam%' then 1
      when lower(drug) like '%axetil%' then 1
      when lower(drug) like '%bactocill%' then 1
      when lower(drug) like '%bactrim%' then 1
      when lower(drug) like '%bactroban%' then 1
      when lower(drug) like '%bethkis%' then 1
      when lower(drug) like '%biaxin%' then 1
      when lower(drug) like '%bicillin l-a%' then 1
      when lower(drug) like '%cayston%' then 1
      when lower(drug) like '%cefazolin%' then 1
      when lower(drug) like '%cedax%' then 1
      when lower(drug) like '%cefoxitin%' then 1
      when lower(drug) like '%ceftazidime%' then 1
      when lower(drug) like '%cefaclor%' then 1
      when lower(drug) like '%cefadroxil%' then 1
      when lower(drug) like '%cefdinir%' then 1
      when lower(drug) like '%cefditoren%' then 1
      when lower(drug) like '%cefepime%' then 1
      when lower(drug) like '%cefotan%' then 1
      when lower(drug) like '%cefotetan%' then 1
      when lower(drug) like '%cefotaxime%' then 1
      when lower(drug) like '%ceftaroline%' then 1
      when lower(drug) like '%cefpodoxime%' then 1
      when lower(drug) like '%cefpirome%' then 1
      when lower(drug) like '%cefprozil%' then 1
      when lower(drug) like '%ceftibuten%' then 1
      when lower(drug) like '%ceftin%' then 1
      when lower(drug) like '%ceftriaxone%' then 1
      when lower(drug) like '%cefuroxime%' then 1
      when lower(drug) like '%cephalexin%' then 1
      when lower(drug) like '%cephalothin%' then 1
      when lower(drug) like '%cephapririn%' then 1
      when lower(drug) like '%chloramphenicol%' then 1
      when lower(drug) like '%cipro%' then 1
      when lower(drug) like '%ciprofloxacin%' then 1
      when lower(drug) like '%claforan%' then 1
      when lower(drug) like '%clarithromycin%' then 1
      when lower(drug) like '%cleocin%' then 1
      when lower(drug) like '%clindamycin%' then 1
      when lower(drug) like '%cubicin%' then 1
      when lower(drug) like '%dicloxacillin%' then 1
      when lower(drug) like '%dirithromycin%' then 1
      when lower(drug) like '%doryx%' then 1
      when lower(drug) like '%doxycy%' then 1
      when lower(drug) like '%duricef%' then 1
      when lower(drug) like '%dynacin%' then 1
      when lower(drug) like '%ery-tab%' then 1
      when lower(drug) like '%eryped%' then 1
      when lower(drug) like '%eryc%' then 1
      when lower(drug) like '%erythrocin%' then 1
      when lower(drug) like '%erythromycin%' then 1
      when lower(drug) like '%factive%' then 1
      when lower(drug) like '%flagyl%' then 1
      when lower(drug) like '%fortaz%' then 1
      when lower(drug) like '%furadantin%' then 1
      when lower(drug) like '%garamycin%' then 1
      when lower(drug) like '%gentamicin%' then 1
      when lower(drug) like '%kanamycin%' then 1
      when lower(drug) like '%keflex%' then 1
      when lower(drug) like '%kefzol%' then 1
      when lower(drug) like '%ketek%' then 1
      when lower(drug) like '%levaquin%' then 1
      when lower(drug) like '%levofloxacin%' then 1
      when lower(drug) like '%lincocin%' then 1
      when lower(drug) like '%linezolid%' then 1
      when lower(drug) like '%macrobid%' then 1
      when lower(drug) like '%macrodantin%' then 1
      when lower(drug) like '%maxipime%' then 1
      when lower(drug) like '%mefoxin%' then 1
      when lower(drug) like '%metronidazole%' then 1
      when lower(drug) like '%meropenem%' then 1
      when lower(drug) like '%methicillin%' then 1
      when lower(drug) like '%minocin%' then 1
      when lower(drug) like '%minocycline%' then 1
      when lower(drug) like '%monodox%' then 1
      when lower(drug) like '%monurol%' then 1
      when lower(drug) like '%morgidox%' then 1
      when lower(drug) like '%moxatag%' then 1
      when lower(drug) like '%moxifloxacin%' then 1
      when lower(drug) like '%mupirocin%' then 1
      when lower(drug) like '%myrac%' then 1
      when lower(drug) like '%nafcillin%' then 1
      when lower(drug) like '%neomycin%' then 1
      when lower(drug) like '%nicazel doxy 30%' then 1
      when lower(drug) like '%nitrofurantoin%' then 1
      when lower(drug) like '%norfloxacin%' then 1
      when lower(drug) like '%noroxin%' then 1
      when lower(drug) like '%ocudox%' then 1
      when lower(drug) like '%ofloxacin%' then 1
      when lower(drug) like '%omnicef%' then 1
      when lower(drug) like '%oracea%' then 1
      when lower(drug) like '%oraxyl%' then 1
      when lower(drug) like '%oxacillin%' then 1
      when lower(drug) like '%pc pen vk%' then 1
      when lower(drug) like '%pce dispertab%' then 1
      when lower(drug) like '%panixine%' then 1
      when lower(drug) like '%pediazole%' then 1
      when lower(drug) like '%penicillin%' then 1
      when lower(drug) like '%periostat%' then 1
      when lower(drug) like '%pfizerpen%' then 1
      when lower(drug) like '%piperacillin%' then 1
      when lower(drug) like '%tazobactam%' then 1
      when lower(drug) like '%primsol%' then 1
      when lower(drug) like '%proquin%' then 1
      when lower(drug) like '%raniclor%' then 1
      when lower(drug) like '%rifadin%' then 1
      when lower(drug) like '%rifampin%' then 1
      when lower(drug) like '%rocephin%' then 1
      when lower(drug) like '%smz-tmp%' then 1
      when lower(drug) like '%septra%' then 1
      when lower(drug) like '%septra ds%' then 1
      when lower(drug) like '%septra%' then 1
      when lower(drug) like '%solodyn%' then 1
      when lower(drug) like '%spectracef%' then 1
      when lower(drug) like '%streptomycin%' then 1
      when lower(drug) like '%sulfadiazine%' then 1
      when lower(drug) like '%sulfamethoxazole%' then 1
      when lower(drug) like '%trimethoprim%' then 1
      when lower(drug) like '%sulfatrim%' then 1
      when lower(drug) like '%sulfisoxazole%' then 1
      when lower(drug) like '%suprax%' then 1
      when lower(drug) like '%synercid%' then 1
      when lower(drug) like '%tazicef%' then 1
      when lower(drug) like '%tetracycline%' then 1
      when lower(drug) like '%timentin%' then 1
      when lower(drug) like '%tobramycin%' then 1
      when lower(drug) like '%trimethoprim%' then 1
      when lower(drug) like '%unasyn%' then 1
      when lower(drug) like '%vancocin%' then 1
      when lower(drug) like '%vancomycin%' then 1
      when lower(drug) like '%vantin%' then 1
      when lower(drug) like '%vibativ%' then 1
      when lower(drug) like '%vibra-tabs%' then 1
      when lower(drug) like '%vibramycin%' then 1
      when lower(drug) like '%zinacef%' then 1
      when lower(drug) like '%zithromax%' then 1
      when lower(drug) like '%zosyn%' then 1
      when lower(drug) like '%zyvox%' then 1
    else 0
    end as antibiotic
  from mimic_hosp.prescriptions
  -- excludes vials/syringe/normal saline, etc
  where drug_type not in ('BASE')
  -- we exclude routes via the eye, ears, or topically
  and route not in ('OU','OS','OD','AU','AS','AD', 'TP')
  and lower(route) not like '%ear%'
  and lower(route) not like '%eye%'
  -- we exclude certain types of antibiotics: topical creams, gels, desens, etc
  and lower(drug) not like '%cream%'
  and lower(drug) not like '%desensitization%'
  and lower(drug) not like '%ophth oint%'
  and lower(drug) not like '%gel%'
  -- other routes not sure about...
  -- for sure keep: ('IV','PO','PO/NG','ORAL', 'IV DRIP', 'IV BOLUS')
  -- ? VT, PB, PR, PL, NS, NG, NEB, NAS, LOCK, J TUBE, IVT
  -- ? IT, IRR, IP, IO, INHALATION, IN, IM
  -- ? IJ, IH, G TUBE, DIALYS
  -- ?? enemas??
)
select 
pr.subject_id, pr.hadm_id
, ie.stay_id
, pr.drug as antibiotic
, pr.route
, pr.starttime
, pr.stoptime
from mimic_hosp.prescriptions pr
-- inner join to subselect to only antibiotic prescriptions
inner join abx
    on pr.drug = abx.drug
    -- route is never NULL for antibiotics
    -- only ~4000 null rows in prescriptions total.
    AND pr.route = abx.route
-- add in stay_id as we use this table for sepsis-3
LEFT JOIN mimic_icu.icustays ie
    ON pr.hadm_id = ie.hadm_id
    AND pr.starttime >= ie.intime
    AND pr.starttime < ie.outtime
WHERE abx.antibiotic = 1
;

-- 多巴酚丁胺
-- dobutamine
-- This query extracts dose+durations of dopamine administration
create materialized view if not exists dobutamine as 
select
stay_id, linkorderid
, rate as vaso_rate
, amount as vaso_amount
, starttime
, endtime
from mimic_icu.inputevents
where itemid = 221653; -- dobutamine

-- 多巴胺
-- dopamine
-- This query extracts dose+durations of dopamine administration
create materialized view if not exists dopamine as 
select
stay_id, linkorderid
, rate as vaso_rate
, amount as vaso_amount
, starttime
, endtime
from mimic_icu.inputevents
where itemid = 221662; -- dopamine

-- 肾上腺素
-- epinephrine
-- This query extracts dose+durations of epinephrine administration
create materialized view if not exists epinephrine as 
select
stay_id, linkorderid
, rate as vaso_rate
, amount as vaso_amount
, starttime
, endtime
from mimic_icu.inputevents
where itemid = 221289; -- epinephrine

-- 神经-肌肉阻滞剂
-- neuroblock
-- This query extracts dose+durations of neuromuscular blocking agents
create materialized view if not exists neuroblock as 
select
    stay_id, orderid
  , rate as drug_rate
  , amount as drug_amount
  , starttime
  , endtime
from mimic_icu.inputevents
where itemid in
(
    222062 -- Vecuronium (664 rows, 154 infusion rows)
  , 221555 -- Cisatracurium (9334 rows, 8970 infusion rows)
)
and rate is not null; -- only continuous infusions

-- 去甲肾上腺素
-- norepinephrine
-- This query extracts dose+durations of norepinephrine administration
create materialized view if not exists norepinephrine as 
select
  stay_id, linkorderid
  , rate as vaso_rate
  , amount as vaso_amount
  , starttime
  , endtime
from mimic_icu.inputevents
where itemid = 221906; -- norepinephrine

-- 苯肾上腺素
--phenylephrine
-- This query extracts dose+durations of phenylephrine administration
create materialized view if not exists phenylephrine as 
select
  stay_id, linkorderid
  , rate as vaso_rate
  , amount as vaso_amount
  , starttime
  , endtime
from mimic_icu.inputevents
where itemid = 221749; -- phenylephrine

-- 血管升压素
-- vasopressin
-- This query extracts dose+durations of vasopressin administration
create materialized view if not exists vasopressin as 
select
  stay_id, linkorderid
  , rate as vaso_rate
  , amount as vaso_amount
  , starttime
  , endtime
from mimic_icu.inputevents
where itemid = 222315; -- vasopressin


-- 肾脏代替治疗（RRT）
-- rrt
-- Creates a table with stay_id / time / dialysis type (if present)
create materialized view if not exists rrt as 
with ce as
(
  select ce.stay_id
    , ce.charttime
          -- when ce.itemid in (152,148,149,146,147,151,150) and value is not null then 1
          -- when ce.itemid in (229,235,241,247,253,259,265,271) and value = 'Dialysis Line' then 1
          -- when ce.itemid = 466 and value = 'Dialysis RN' then 1
          -- when ce.itemid = 927 and value = 'Dialysis Solutions' then 1
          -- when ce.itemid = 6250 and value = 'dialys' then 1
          -- when ce.
          -- when ce.itemid = 582 and value in ('CAVH Start','CAVH D/C','CVVHD Start','CVVHD D/C','Hemodialysis st','Hemodialysis end') then 1
    , CASE
        -- metavision itemids

        -- checkboxes
        WHEN ce.itemid IN
        (
            226118 -- | Dialysis Catheter placed in outside facility      | Access Lines - Invasive | chartevents        | Checkbox
          , 227357 -- | Dialysis Catheter Dressing Occlusive              | Access Lines - Invasive | chartevents        | Checkbox
          , 225725 -- | Dialysis Catheter Tip Cultured                    | Access Lines - Invasive | chartevents        | Checkbox
        ) THEN 1
        -- numeric data
        WHEN ce.itemid IN
        (
            226499 -- | Hemodialysis Output                               | Dialysis
          , 224154 -- | Dialysate Rate                                    | Dialysis
          , 225810 -- | Dwell Time (Peritoneal Dialysis)                  | Dialysis
          , 225959 -- | Medication Added Amount  #1 (Peritoneal Dialysis) | Dialysis
          , 227639 -- | Medication Added Amount  #2 (Peritoneal Dialysis) | Dialysis
          , 225183 -- | Current Goal                     | Dialysis
          , 227438 -- | Volume not removed               | Dialysis
          , 224191 -- | Hourly Patient Fluid Removal     | Dialysis
          , 225806 -- | Volume In (PD)                   | Dialysis
          , 225807 -- | Volume Out (PD)                  | Dialysis
          , 228004 -- | Citrate (ACD-A)                  | Dialysis
          , 228005 -- | PBP (Prefilter) Replacement Rate | Dialysis
          , 228006 -- | Post Filter Replacement Rate     | Dialysis
          , 224144 -- | Blood Flow (ml/min)              | Dialysis
          , 224145 -- | Heparin Dose (per hour)          | Dialysis
          , 224149 -- | Access Pressure                  | Dialysis
          , 224150 -- | Filter Pressure                  | Dialysis
          , 224151 -- | Effluent Pressure                | Dialysis
          , 224152 -- | Return Pressure                  | Dialysis
          , 224153 -- | Replacement Rate                 | Dialysis
          , 224404 -- | ART Lumen Volume                 | Dialysis
          , 224406 -- | VEN Lumen Volume                 | Dialysis
          , 226457 -- | Ultrafiltrate Output             | Dialysis
        ) THEN 1

        -- text fields
        WHEN ce.itemid IN
        (
            224135 -- | Dialysis Access Site | Dialysis
          , 224139 -- | Dialysis Site Appearance | Dialysis
          , 224146 -- | System Integrity | Dialysis
          , 225323 -- | Dialysis Catheter Site Appear | Access Lines - Invasive
          , 225740 -- | Dialysis Catheter Discontinued | Access Lines - Invasive
          , 225776 -- | Dialysis Catheter Dressing Type | Access Lines - Invasive
          , 225951 -- | Peritoneal Dialysis Fluid Appearance | Dialysis
          , 225952 -- | Medication Added #1 (Peritoneal Dialysis) | Dialysis
          , 225953 -- | Solution (Peritoneal Dialysis) | Dialysis
          , 225954 -- | Dialysis Access Type | Dialysis
          , 225956 -- | Reason for CRRT Filter Change | Dialysis
          , 225958 -- | Heparin Concentration (units/mL) | Dialysis
          , 225961 -- | Medication Added Units #1 (Peritoneal Dialysis) | Dialysis
          , 225963 -- | Peritoneal Dialysis Catheter Type | Dialysis
          , 225965 -- | Peritoneal Dialysis Catheter Status | Dialysis
          , 225976 -- | Replacement Fluid | Dialysis
          , 225977 -- | Dialysate Fluid | Dialysis
          , 227124 -- | Dialysis Catheter Type | Access Lines - Invasive
          , 227290 -- | CRRT mode | Dialysis
          , 227638 -- | Medication Added #2 (Peritoneal Dialysis) | Dialysis
          , 227640 -- | Medication Added Units #2 (Peritoneal Dialysis) | Dialysis
          , 227753 -- | Dialysis Catheter Placement Confirmed by X-ray | Access Lines - Invasive
        ) THEN 1
      ELSE 0 END
      AS dialysis_present
    , CASE
        WHEN ce.itemid = 225965 -- Peritoneal Dialysis Catheter Status
          AND value = 'In use' THEN 1
        WHEN ce.itemid IN
        (
            226499 -- | Hemodialysis Output              | Dialysis
          , 224154 -- | Dialysate Rate                   | Dialysis
          , 225183 -- | Current Goal                     | Dialysis
          , 227438 -- | Volume not removed               | Dialysis
          , 224191 -- | Hourly Patient Fluid Removal     | Dialysis
          , 225806 -- | Volume In (PD)                   | Dialysis
          , 225807 -- | Volume Out (PD)                  | Dialysis
          , 228004 -- | Citrate (ACD-A)                  | Dialysis
          , 228005 -- | PBP (Prefilter) Replacement Rate | Dialysis
          , 228006 -- | Post Filter Replacement Rate     | Dialysis
          , 224144 -- | Blood Flow (ml/min)              | Dialysis
          , 224145 -- | Heparin Dose (per hour)          | Dialysis
          , 224153 -- | Replacement Rate                 | Dialysis
          , 226457 -- | Ultrafiltrate Output             | Dialysis
        ) THEN 1
      ELSE 0 END
      AS dialysis_active
    , CASE
        -- dialysis mode
        -- we try to set dialysis mode to one of:
        --   CVVH
        --   CVVHD
        --   CVVHDF
        --   SCUF
        --   Peritoneal
        --   IHD
        -- these are the modes in itemid 227290
        WHEN ce.itemid = 227290 THEN value
        -- itemids which imply a certain dialysis mode
        -- peritoneal dialysis
        WHEN ce.itemid IN 
        (
            225810 -- | Dwell Time (Peritoneal Dialysis) | Dialysis
          , 225806 -- | Volume In (PD)                   | Dialysis
          , 225807 -- | Volume Out (PD)                  | Dialysis
          , 225810 -- | Dwell Time (Peritoneal Dialysis)                  | Dialysis
          , 227639 -- | Medication Added Amount  #2 (Peritoneal Dialysis) | Dialysis
          , 225959 -- | Medication Added Amount  #1 (Peritoneal Dialysis) | Dialysis
          , 225951 -- | Peritoneal Dialysis Fluid Appearance | Dialysis
          , 225952 -- | Medication Added #1 (Peritoneal Dialysis) | Dialysis
          , 225961 -- | Medication Added Units #1 (Peritoneal Dialysis) | Dialysis
          , 225953 -- | Solution (Peritoneal Dialysis) | Dialysis
          , 225963 -- | Peritoneal Dialysis Catheter Type | Dialysis
          , 225965 -- | Peritoneal Dialysis Catheter Status | Dialysis
          , 227638 -- | Medication Added #2 (Peritoneal Dialysis) | Dialysis
          , 227640 -- | Medication Added Units #2 (Peritoneal Dialysis) | Dialysis
        )
          THEN 'Peritoneal'
        WHEN ce.itemid = 226499
          THEN 'IHD'
      ELSE NULL END as dialysis_type
  from mimic_icu.chartevents ce
  WHERE ce.itemid in
  (
    -- === MetaVision itemids === --
  
    -- Checkboxes
      226118 -- | Dialysis Catheter placed in outside facility      | Access Lines - Invasive | chartevents        | Checkbox
    , 227357 -- | Dialysis Catheter Dressing Occlusive              | Access Lines - Invasive | chartevents        | Checkbox
    , 225725 -- | Dialysis Catheter Tip Cultured                    | Access Lines - Invasive | chartevents        | Checkbox

    -- Numeric values
    , 226499 -- | Hemodialysis Output                               | Dialysis                | chartevents        | Numeric
    , 224154 -- | Dialysate Rate                                    | Dialysis                | chartevents        | Numeric
    , 225810 -- | Dwell Time (Peritoneal Dialysis)                  | Dialysis                | chartevents        | Numeric
    , 227639 -- | Medication Added Amount  #2 (Peritoneal Dialysis) | Dialysis                | chartevents        | Numeric
    , 225183 -- | Current Goal                     | Dialysis | chartevents        | Numeric
    , 227438 -- | Volume not removed               | Dialysis | chartevents        | Numeric
    , 224191 -- | Hourly Patient Fluid Removal     | Dialysis | chartevents        | Numeric
    , 225806 -- | Volume In (PD)                   | Dialysis | chartevents        | Numeric
    , 225807 -- | Volume Out (PD)                  | Dialysis | chartevents        | Numeric
    , 228004 -- | Citrate (ACD-A)                  | Dialysis | chartevents        | Numeric
    , 228005 -- | PBP (Prefilter) Replacement Rate | Dialysis | chartevents        | Numeric
    , 228006 -- | Post Filter Replacement Rate     | Dialysis | chartevents        | Numeric
    , 224144 -- | Blood Flow (ml/min)              | Dialysis | chartevents        | Numeric
    , 224145 -- | Heparin Dose (per hour)          | Dialysis | chartevents        | Numeric
    , 224149 -- | Access Pressure                  | Dialysis | chartevents        | Numeric
    , 224150 -- | Filter Pressure                  | Dialysis | chartevents        | Numeric
    , 224151 -- | Effluent Pressure                | Dialysis | chartevents        | Numeric
    , 224152 -- | Return Pressure                  | Dialysis | chartevents        | Numeric
    , 224153 -- | Replacement Rate                 | Dialysis | chartevents        | Numeric
    , 224404 -- | ART Lumen Volume                 | Dialysis | chartevents        | Numeric
    , 224406 -- | VEN Lumen Volume                 | Dialysis | chartevents        | Numeric
    , 226457 -- | Ultrafiltrate Output             | Dialysis | chartevents        | Numeric
    , 225959 -- | Medication Added Amount  #1 (Peritoneal Dialysis) | Dialysis | chartevents | Numeric
    -- Text values
    , 224135 -- | Dialysis Access Site | Dialysis | chartevents | Text
    , 224139 -- | Dialysis Site Appearance | Dialysis | chartevents | Text
    , 224146 -- | System Integrity | Dialysis | chartevents | Text
    , 225323 -- | Dialysis Catheter Site Appear | Access Lines - Invasive | chartevents | Text
    , 225740 -- | Dialysis Catheter Discontinued | Access Lines - Invasive | chartevents | Text
    , 225776 -- | Dialysis Catheter Dressing Type | Access Lines - Invasive | chartevents | Text
    , 225951 -- | Peritoneal Dialysis Fluid Appearance | Dialysis | chartevents | Text
    , 225952 -- | Medication Added #1 (Peritoneal Dialysis) | Dialysis | chartevents | Text
    , 225953 -- | Solution (Peritoneal Dialysis) | Dialysis | chartevents | Text
    , 225954 -- | Dialysis Access Type | Dialysis | chartevents | Text
    , 225956 -- | Reason for CRRT Filter Change | Dialysis | chartevents | Text
    , 225958 -- | Heparin Concentration (units/mL) | Dialysis | chartevents | Text
    , 225961 -- | Medication Added Units #1 (Peritoneal Dialysis) | Dialysis | chartevents | Text
    , 225963 -- | Peritoneal Dialysis Catheter Type | Dialysis | chartevents | Text
    , 225965 -- | Peritoneal Dialysis Catheter Status | Dialysis | chartevents | Text
    , 225976 -- | Replacement Fluid | Dialysis | chartevents | Text
    , 225977 -- | Dialysate Fluid | Dialysis | chartevents | Text
    , 227124 -- | Dialysis Catheter Type | Access Lines - Invasive | chartevents | Text
    , 227290 -- | CRRT mode | Dialysis | chartevents | Text
    , 227638 -- | Medication Added #2 (Peritoneal Dialysis) | Dialysis | chartevents | Text
    , 227640 -- | Medication Added Units #2 (Peritoneal Dialysis) | Dialysis | chartevents | Text
    , 227753 -- | Dialysis Catheter Placement Confirmed by X-ray | Access Lines - Invasive | chartevents | Text
  )
  AND ce.value IS NOT NULL
)
-- TODO:
--   charttime + dialysis_present + dialysis_active
--  for inputevents_cv, outputevents
--  for procedures_mv, left join and set the dialysis_type
, oe as
(
 select stay_id
    , charttime
    , 1 AS dialysis_present
    , 0 AS dialysis_active
    , NULL AS dialysis_type
 from mimic_icu.outputevents
 where itemid in
 (
       40386 -- hemodialysis
 )
 and value > 0 -- also ensures it's not null
)
, mv_ranges as
(
  select stay_id
    , starttime, endtime
    , 1 AS dialysis_present
    , 1 AS dialysis_active
    , 'CRRT' as dialysis_type
  from mimic_icu.inputevents
  where itemid in
  (
      227536 --	KCl (CRRT)	Medications	inputevents_mv	Solution
    , 227525 --	Calcium Gluconate (CRRT)	Medications	inputevents_mv	Solution
  )
  and amount > 0 -- also ensures it's not null
  UNION DISTINCT
  select stay_id
    , starttime, endtime
    , 1 AS dialysis_present
    , CASE WHEN itemid NOT IN (224270, 225436) THEN 1 ELSE 0 END AS dialysis_active
    , CASE
        WHEN itemid = 225441 THEN 'IHD'
        WHEN itemid = 225802 THEN 'CRRT'  -- CVVH (Continuous venovenous hemofiltration)
        WHEN itemid = 225803 THEN 'CVVHD' -- CVVHD (Continuous venovenous hemodialysis)
        WHEN itemid = 225805 THEN 'Peritoneal'
        WHEN itemid = 225809 THEN 'CVVHDF' -- CVVHDF (Continuous venovenous hemodiafiltration)
        WHEN itemid = 225955 THEN 'SCUF' -- SCUF (Slow continuous ultra filtration)
      ELSE NULL END as dialysis_type
  from mimic_icu.procedureevents
  where itemid in
  (
      225441 -- | Hemodialysis          | 4-Procedures              | procedureevents_mv | Process
    , 225802 -- | Dialysis - CRRT       | Dialysis                  | procedureevents_mv | Process
    , 225803 -- | Dialysis - CVVHD      | Dialysis                  | procedureevents_mv | Process
    , 225805 -- | Peritoneal Dialysis   | Dialysis                  | procedureevents_mv | Process
    , 224270 -- | Dialysis Catheter     | Access Lines - Invasive   | procedureevents_mv | Process
    , 225809 -- | Dialysis - CVVHDF     | Dialysis                  | procedureevents_mv | Process
    , 225955 -- | Dialysis - SCUF       | Dialysis                  | procedureevents_mv | Process
    , 225436 -- | CRRT Filter Change    | Dialysis                  | procedureevents_mv | Process
  )
  AND value IS NOT NULL
)
-- union together the charttime tables; append times from mv_ranges to guarantee they exist
, stg0 AS
(
  SELECT
    stay_id, charttime, dialysis_present, dialysis_active, dialysis_type
  FROM ce
  WHERE dialysis_present = 1
  UNION DISTINCT
--   SELECT
--     stay_id, charttime, dialysis_present, dialysis_active, dialysis_type
--   FROM oe
--   WHERE dialysis_present = 1
--   UNION DISTINCT
  SELECT
    stay_id, starttime AS charttime, dialysis_present, dialysis_active, dialysis_type
  FROM mv_ranges
)
SELECT
    stg0.stay_id
    , charttime
    , COALESCE(mv.dialysis_present, stg0.dialysis_present) AS dialysis_present
    , COALESCE(mv.dialysis_active, stg0.dialysis_active) AS dialysis_active
    , COALESCE(mv.dialysis_type, stg0.dialysis_type) AS dialysis_type
FROM stg0
LEFT JOIN mv_ranges mv
  ON stg0.stay_id = mv.stay_id
  AND stg0.charttime >= mv.starttime
  AND stg0.charttime <= mv.endtime;
-- 33.crrt
create materialized view if not exists crrt as 
with crrt_settings as
(
  select ce.stay_id, ce.charttime
  , CASE WHEN ce.itemid = 227290 THEN ce.value END AS CRRT_mode
  , CASE WHEN ce.itemid = 224149 THEN ce.valuenum ELSE NULL END AS AccessPressure
  , CASE WHEN ce.itemid = 224144 THEN ce.valuenum ELSE NULL END AS BloodFlow -- (ml/min)
  , CASE WHEN ce.itemid = 228004 THEN ce.valuenum ELSE NULL END AS Citrate -- (ACD-A)
  , CASE WHEN ce.itemid = 225183 THEN ce.valuenum ELSE NULL END AS CurrentGoal
  , CASE WHEN ce.itemid = 225977 THEN ce.value ELSE NULL END AS DialysateFluid
  , CASE WHEN ce.itemid = 224154 THEN ce.valuenum ELSE NULL END AS DialysateRate
  , CASE WHEN ce.itemid = 224151 THEN ce.valuenum ELSE NULL END AS EffluentPressure
  , CASE WHEN ce.itemid = 224150 THEN ce.valuenum ELSE NULL END AS FilterPressure
  , CASE WHEN ce.itemid = 225958 THEN ce.value ELSE NULL END AS HeparinConcentration -- (units/mL)
  , CASE WHEN ce.itemid = 224145 THEN ce.valuenum ELSE NULL END AS HeparinDose -- (per hour)
  -- below may not account for drug infusion/hyperalimentation/anticoagulants infused
  , CASE WHEN ce.itemid = 224191 THEN ce.valuenum ELSE NULL END AS HourlyPatientFluidRemoval
  , CASE WHEN ce.itemid = 228005 THEN ce.valuenum ELSE NULL END AS PrefilterReplacementRate
  , CASE WHEN ce.itemid = 228006 THEN ce.valuenum ELSE NULL END AS PostFilterReplacementRate
  , CASE WHEN ce.itemid = 225976 THEN ce.value ELSE NULL END AS ReplacementFluid
  , CASE WHEN ce.itemid = 224153 THEN ce.valuenum ELSE NULL END AS ReplacementRate
  , CASE WHEN ce.itemid = 224152 THEN ce.valuenum ELSE NULL END AS ReturnPressure
  , CASE WHEN ce.itemid = 226457 THEN ce.valuenum END AS UltrafiltrateOutput
  -- separate system integrity into sub components
  -- need to do this as 224146 has multiple unique values for a single charttime
  -- e.g. "Clots Present" and "Active" at same time
  , CASE
        WHEN ce.itemid = 224146
        AND ce.value IN ('Active', 'Initiated', 'Reinitiated', 'New Filter')
            THEN 1
        WHEN ce.itemid = 224146
        AND ce.value IN ('Recirculating', 'Discontinued')
            THEN 0
    ELSE NULL END as system_active
  , CASE
        WHEN ce.itemid = 224146
        AND ce.value IN ('Clots Present', 'Clots Present')
            THEN 1
        WHEN ce.itemid = 224146
        AND ce.value IN ('No Clot Present', 'No Clot Present')
            THEN 0
    ELSE NULL END as clots
  , CASE 
        WHEN ce.itemid = 224146
        AND ce.value IN ('Clots Increasing', 'Clot Increasing')
            THEN 1
    ELSE NULL END as clots_increasing
  , CASE
        WHEN ce.itemid = 224146
        AND ce.value IN ('Clotted')
            THEN 1
    ELSE NULL END as clotted
  from mimic_icu.chartevents ce
  where ce.itemid in
  (
    -- MetaVision ITEMIDs
    227290, -- CRRT Mode
    224146, -- System Integrity
    -- 225956,  -- Reason for CRRT Filter Change
    -- above itemid is one of: Clotted, Line Changed, Procedure
    -- only ~200 rows, not super useful
    224149, -- Access Pressure
    224144, -- Blood Flow (ml/min)
    228004, -- Citrate (ACD-A)
    225183, -- Current Goal
    225977, -- Dialysate Fluid
    224154, -- Dialysate Rate
    224151, -- Effluent Pressure
    224150, -- Filter Pressure
    225958, -- Heparin Concentration (units/mL)
    224145, -- Heparin Dose (per hour)
    224191, -- Hourly Patient Fluid Removal
    228005, -- PBP (Prefilter) Replacement Rate
    228006, -- Post Filter Replacement Rate
    225976, -- Replacement Fluid
    224153, -- Replacement Rate
    224152, -- Return Pressure
    226457  -- Ultrafiltrate Output
  )
  and ce.value is not null
)
-- use MAX() to collapse to a single row
-- there is only ever 1 row for unique combinations of stay_id/charttime/itemid
select stay_id
, charttime
, MAX(crrt_mode) AS crrt_mode
, MAX(AccessPressure) AS access_pressure
, MAX(BloodFlow) AS blood_flow
, MAX(Citrate) AS citrate
, MAX(CurrentGoal) AS current_goal
, MAX(DialysateFluid) AS dialysate_fluid
, MAX(DialysateRate) AS dialysate_rate
, MAX(EffluentPressure) AS effluent_pressure
, MAX(FilterPressure) AS filter_pressure
, MAX(HeparinConcentration) AS heparin_concentration
, MAX(HeparinDose) AS heparin_dose
, MAX(HourlyPatientFluidRemoval) AS hourly_patient_fluid_removal
, MAX(PrefilterReplacementRate) AS prefilter_replacement_rate
, MAX(PostFilterReplacementRate) AS postfilter_replacement_rate
, MAX(ReplacementFluid) AS replacement_fluid
, MAX(ReplacementRate) AS replacement_rate
, MAX(ReturnPressure) AS return_pressure
, MAX(UltrafiltrateOutput) AS ultrafiltrate_output
, MAX(system_active) AS system_active
, MAX(clots) AS clots
, MAX(clots_increasing) AS clots_increasing
, MAX(clotted) AS clotted
from crrt_settings
group by stay_id, charttime;

-- 侵入式监测
-- invasive_line
create materialized view if not exists invasive_line as 
-- metavision
WITH mv AS
(
    SELECT 
        stay_id
        -- since metavision separates lines using itemid, we can use it as the line number
        , mv.itemid AS line_number
        , di.label AS line_type
        , mv.location AS line_site
        , starttime, endtime
    FROM mimic_icu.procedureevents mv
    INNER JOIN mimic_icu.d_items di
      ON mv.itemid = di.itemid
    WHERE mv.itemid IN
    (
      227719 -- AVA Line
    , 225752 -- Arterial Line
    , 224269 -- CCO PAC
    , 224267 -- Cordis/Introducer
    , 224270 -- Dialysis Catheter
    , 224272 -- IABP line
    , 226124 -- ICP Catheter
    , 228169 -- Impella Line
    , 225202 -- Indwelling Port (PortaCath)
    , 228286 -- Intraosseous Device
    , 225204 -- Midline
    , 224263 -- Multi Lumen
    , 224560 -- PA Catheter
    , 224264 -- PICC Line
    , 225203 -- Pheresis Catheter
    , 224273 -- Presep Catheter
    , 225789 -- Sheath
    , 225761 -- Sheath Insertion
    , 228201 -- Tandem Heart Access Line
    , 228202 -- Tandem Heart Return Line
    , 224268 -- Trauma line
    , 225199 -- Triple Introducer
    , 225315 -- Tunneled (Hickman) Line
    , 225205 -- RIC
    )
)
-- as a final step, combine any similar terms together
select
    stay_id
    , CASE
        WHEN line_type IN ('Arterial Line', 'A-Line') THEN 'Arterial'
        WHEN line_type IN ('CCO PA Line', 'CCO PAC') THEN 'Continuous Cardiac Output PA'
        WHEN line_type IN ('Dialysis Catheter', 'Dialysis Line') THEN 'Dialysis'
        WHEN line_type IN ('Hickman', 'Tunneled (Hickman) Line') THEN 'Hickman'
        WHEN line_type IN ('IABP', 'IABP line') THEN 'IABP'
        WHEN line_type IN ('Multi Lumen', 'Multi-lumen') THEN 'Multi Lumen'
        WHEN line_type IN ('PA Catheter', 'PA line') THEN 'PA'
        WHEN line_type IN ('PICC Line', 'PICC line') THEN 'PICC'
        WHEN line_type IN ('Pre-Sep Catheter', 'Presep Catheter') THEN 'Pre-Sep'
        WHEN line_type IN ('Trauma Line', 'Trauma line') THEN 'Trauma'
        WHEN line_type IN ('Triple Introducer', 'TripleIntroducer') THEN 'Triple Introducer'
        WHEN line_type IN ('Portacath', 'Indwelling Port (PortaCath)') THEN 'Portacath'
        -- the following lines were not merged with another line:
        -- AVA Line
        -- Camino Bolt
        -- Cordis/Introducer
        -- ICP Catheter
        -- Impella Line
        -- Intraosseous Device
        -- Introducer
        -- Lumbar Drain
        -- Midline
        -- Other/Remarks
        -- PacerIntroducer
        -- PermaCath
        -- Pheresis Catheter
        -- RIC
        -- Sheath
        -- Tandem Heart Access Line
        -- Tandem Heart Return Line
        -- Venous Access
        -- Ventriculostomy
    ELSE line_type END AS line_type
    , CASE
        WHEN line_site IN ('Left Antecub', 'Left Antecube') THEN 'Left Antecube'
        WHEN line_site IN ('Left Axilla', 'Left Axilla.') THEN 'Left Axilla'
        WHEN line_site IN ('Left Brachial', 'Left Brachial.') THEN 'Left Brachial'
        WHEN line_site IN ('Left Femoral', 'Left Femoral.') THEN 'Left Femoral'
        WHEN line_site IN ('Right Antecub', 'Right Antecube') THEN 'Right Antecube' 
        WHEN line_site IN ('Right Axilla', 'Right Axilla.') THEN 'Right Axilla' 
        WHEN line_site IN ('Right Brachial', 'Right Brachial.') THEN 'Right Brachial' 
        WHEN line_site IN ('Right Femoral', 'Right Femoral.') THEN 'Right Femoral'
        -- the following sites were not merged with other sites:
        -- 'Left Foot'
        -- 'Left IJ'
        -- 'Left Radial'
        -- 'Left Subclavian'
        -- 'Left Ulnar'
        -- 'Left Upper Arm'
        -- 'Right Foot'
        -- 'Right IJ'
        -- 'Right Radial'
        -- 'Right Side Head'
        -- 'Right Subclavian'
        -- 'Right Ulnar'
        -- 'Right Upper Arm'
        -- 'Transthoracic'
        -- 'Other/Remarks'
    ELSE line_site END AS line_site
    , starttime
    , endtime
FROM mv
ORDER BY stay_id, starttime, line_type, line_site;

-- 机械通气
-- ventilation
-- Calculate duration of mechanical ventilation.
-- Some useful cases for debugging:
--  stay_id = 30019660 has a tracheostomy placed in the ICU
--  stay_id = 30000117 has explicit documentation of extubation
-- classify vent settings into modes
create materialized view if not exists ventilation as 
WITH tm AS
(
  SELECT stay_id, charttime
  FROM ventilator_setting
  UNION DISTINCT
  SELECT stay_id, charttime
  FROM oxygen_delivery
)
, vs AS
(
    SELECT tm.stay_id, tm.charttime
    -- source data columns, here for debug
    , o2_delivery_device_1
    , COALESCE(ventilator_mode, ventilator_mode_hamilton) AS vent_mode
    -- case statement determining the type of intervention
    -- done in order of priority: trach > mech vent > NIV > high flow > o2
    , CASE
    -- tracheostomy
    WHEN o2_delivery_device_1 IN
    (
        'Tracheostomy tube'
    -- 'Trach mask ' -- 16435 observations
    )
        THEN 'Trach'
    -- mechanical ventilation
    WHEN o2_delivery_device_1 IN
    (
        'Endotracheal tube'
    )
    OR ventilator_mode IN
    (
        '(S) CMV',
        'APRV',
        'APRV/Biphasic+ApnPress',
        'APRV/Biphasic+ApnVol',
        'APV (cmv)',
        'Ambient',
        'Apnea Ventilation',
        'CMV',
        'CMV/ASSIST',
        'CMV/ASSIST/AutoFlow',
        'CMV/AutoFlow',
        'CPAP/PPS',
        'CPAP/PSV+Apn TCPL',
        'CPAP/PSV+ApnPres',
        'CPAP/PSV+ApnVol',
        'MMV',
        'MMV/AutoFlow',
        'MMV/PSV',
        'MMV/PSV/AutoFlow',
        'P-CMV',
        'PCV+',
        'PCV+/PSV',
        'PCV+Assist',
        'PRES/AC',
        'PRVC/AC',
        'PRVC/SIMV',
        'PSV/SBT',
        'SIMV',
        'SIMV/AutoFlow',
        'SIMV/PRES',
        'SIMV/PSV',
        'SIMV/PSV/AutoFlow',
        'SIMV/VOL',
        'SYNCHRON MASTER',
        'SYNCHRON SLAVE',
        'VOL/AC'
    )
    OR ventilator_mode_hamilton IN
    (
        'APRV',
        'APV (cmv)',
        'Ambient',
        '(S) CMV',
        'P-CMV',
        'SIMV',
        'APV (simv)',
        'P-SIMV',
        'VS',
        'ASV'
    )
        THEN 'InvasiveVent'
    -- NIV
    WHEN o2_delivery_device_1 IN
    (
        'Bipap mask ', -- 8997 observations
        'CPAP mask ' -- 5568 observations
    )
    OR ventilator_mode_hamilton IN
    (
        'DuoPaP',
        'NIV',
        'NIV-ST'
    )
        THEN 'NonInvasiveVent'
    -- high flow
    when o2_delivery_device_1 IN
    (
        'High flow neb', -- 10785 observations
        'High flow nasal cannula' -- 925 observations
    )
        THEN 'HighFlow'
    -- normal oxygen delivery
    WHEN o2_delivery_device_1 in
    (
        'Nasal cannula', -- 153714 observations
        'Face tent', -- 24601 observations
        'Aerosol-cool', -- 24560 observations
        'Non-rebreather', -- 5182 observations
        'Venti mask ', -- 1947 observations
        'Medium conc mask ', -- 1888 observations
        'T-piece', -- 1135 observations
        'Ultrasonic neb', -- 9 observations
        'Vapomist', -- 3 observations
        'Oxymizer' -- 1301 observations
    )
        THEN 'Oxygen'
    -- Not categorized:
    -- 'Other', 'None'
    ELSE NULL END AS ventilation_status
  FROM tm
  LEFT JOIN ventilator_setting vs
      ON tm.stay_id = vs.stay_id
      AND tm.charttime = vs.charttime
  LEFT JOIN oxygen_delivery od
      ON tm.stay_id = od.stay_id
      AND tm.charttime = od.charttime
)
, vd0 AS
(
    SELECT
      stay_id, charttime
      -- source data columns, here for debug
      , o2_delivery_device_1
      , vent_mode
      -- carry over the previous charttime which had the same state
      , LAG(charttime, 1) OVER (PARTITION BY stay_id, ventilation_status ORDER BY charttime) AS charttime_lag
      -- bring back the next charttime, regardless of the state
      -- this will be used as the end time for state transitions
      , LEAD(charttime, 1) OVER w AS charttime_lead
      , ventilation_status
      , LAG(ventilation_status, 1) OVER w AS ventilation_status_lag
    FROM vs
    WHERE ventilation_status IS NOT NULL
    WINDOW w AS (PARTITION BY stay_id ORDER BY charttime)
)
, vd1 as
(
    SELECT
        stay_id
        -- source data columns, here for debug
        , o2_delivery_device_1
        , vent_mode
        , charttime_lag
        , charttime
        , charttime_lead
        , ventilation_status

        -- calculate the time since the last event
        , DATETIME_DIFF(charttime, charttime_lag, 'MINUTE')/60 as ventduration

        -- now we determine if the current ventilation status is "new", or continuing the previous
        , CASE
            -- a 14 hour gap always initiates a new event
            WHEN DATETIME_DIFF(charttime, charttime_lag, 'HOUR') >= 14 THEN 1
            WHEN ventilation_status_lag IS NULL THEN 1
            -- not a new event if identical to the last row
            WHEN ventilation_status_lag != ventilation_status THEN 1
          ELSE 0
          END AS new_status
    FROM vd0
)
, vd2 as
(
    SELECT vd1.*
    -- create a cumulative sum of the instances of new ventilation
    -- this results in a monotonic integer assigned to each instance of ventilation
    , SUM(new_status) OVER (PARTITION BY stay_id ORDER BY charttime) AS vent_num
    FROM vd1
)
-- create the durations for each ventilation instance
SELECT stay_id
  , MIN(charttime) AS starttime
  -- for the end time of the ventilation event, the time of the *next* setting
  -- i.e. if we go NIV -> O2, the end time of NIV is the first row with a documented O2 device
  -- ... unless it's been over 14 hours, in which case it's the last row with a documented NIV.
  , MAX(
        CASE
            WHEN charttime_lead IS NULL
            OR DATETIME_DIFF(charttime_lead, charttime, 'HOUR') >= 14
                THEN charttime
        ELSE charttime_lead
        END
   ) AS endtime
   -- all rows with the same vent_num will have the same ventilation_status
   -- for efficiency, we use an aggregate here, but we could equally well group by this column
  , MAX(ventilation_status) AS ventilation_status
FROM vd2
GROUP BY stay_id, vent_num
HAVING min(charttime) != max(charttime)
;

-- 入院首日血气（动脉）
-- first_day_bg_art
-- Highest/lowest blood gas values for arterial blood specimens
create materialized view if not exists first_day_bg_art as 
select
    ie.subject_id
    , ie.stay_id
    , MIN(lactate) AS lactate_min, MAX(lactate) AS lactate_max
    , MIN(ph) AS ph_min, MAX(ph) AS ph_max
    , MIN(so2) AS so2_min, MAX(so2) AS so2_max
    , MIN(po2) AS po2_min, MAX(po2) AS po2_max
    , MIN(pco2) AS pco2_min, MAX(pco2) AS pco2_max
    , MIN(aado2) AS aado2_min, MAX(aado2) AS aado2_max
    , MIN(aado2_calc) AS aado2_calc_min, MAX(aado2_calc) AS aado2_calc_max
    , MIN(pao2fio2ratio) AS pao2fio2ratio_min, MAX(pao2fio2ratio) AS pao2fio2ratio_max
    , MIN(baseexcess) AS baseexcess_min, MAX(baseexcess) AS baseexcess_max
    , MIN(bicarbonate) AS bicarbonate_min, MAX(bicarbonate) AS bicarbonate_max
    , MIN(totalco2) AS totalco2_min, MAX(totalco2) AS totalco2_max
    , MIN(hematocrit) AS hematocrit_min, MAX(hematocrit) AS hematocrit_max
    , MIN(hemoglobin) AS hemoglobin_min, MAX(hemoglobin) AS hemoglobin_max
    , MIN(carboxyhemoglobin) AS carboxyhemoglobin_min, MAX(carboxyhemoglobin) AS carboxyhemoglobin_max
    , MIN(methemoglobin) AS methemoglobin_min, MAX(methemoglobin) AS methemoglobin_max
    , MIN(temperature) AS temperature_min, MAX(temperature) AS temperature_max
    , MIN(chloride) AS chloride_min, MAX(chloride) AS chloride_max
    , MIN(calcium) AS calcium_min, MAX(calcium) AS calcium_max
    , MIN(glucose) AS glucose_min, MAX(glucose) AS glucose_max
    , MIN(potassium) AS potassium_min, MAX(potassium) AS potassium_max
    , MIN(sodium) AS sodium_min, MAX(sodium) AS sodium_max
FROM mimic_icu.icustays ie
LEFT JOIN bg bg
    ON ie.subject_id = bg.subject_id
    AND bg.specimen_pred = 'ART.'
    AND bg.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND bg.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id
;

-- 入院首日血气
-- first_day_bg
-- Highest/lowest blood gas values for all blood specimens (venous/arterial/mixed)
create materialized view if not exists first_day_bg as 
select
    ie.subject_id
    , ie.stay_id
    , MIN(lactate) AS lactate_min, MAX(lactate) AS lactate_max
    , MIN(ph) AS ph_min, MAX(ph) AS ph_max
    , MIN(so2) AS so2_min, MAX(so2) AS so2_max
    , MIN(po2) AS po2_min, MAX(po2) AS po2_max
    , MIN(pco2) AS pco2_min, MAX(pco2) AS pco2_max
    , MIN(aado2) AS aado2_min, MAX(aado2) AS aado2_max
    , MIN(aado2_calc) AS aado2_calc_min, MAX(aado2_calc) AS aado2_calc_max
    , MIN(pao2fio2ratio) AS pao2fio2ratio_min, MAX(pao2fio2ratio) AS pao2fio2ratio_max
    , MIN(baseexcess) AS baseexcess_min, MAX(baseexcess) AS baseexcess_max
    , MIN(bicarbonate) AS bicarbonate_min, MAX(bicarbonate) AS bicarbonate_max
    , MIN(totalco2) AS totalco2_min, MAX(totalco2) AS totalco2_max
    , MIN(hematocrit) AS hematocrit_min, MAX(hematocrit) AS hematocrit_max
    , MIN(hemoglobin) AS hemoglobin_min, MAX(hemoglobin) AS hemoglobin_max
    , MIN(carboxyhemoglobin) AS carboxyhemoglobin_min, MAX(carboxyhemoglobin) AS carboxyhemoglobin_max
    , MIN(methemoglobin) AS methemoglobin_min, MAX(methemoglobin) AS methemoglobin_max
    , MIN(temperature) AS temperature_min, MAX(temperature) AS temperature_max
    , MIN(chloride) AS chloride_min, MAX(chloride) AS chloride_max
    , MIN(calcium) AS calcium_min, MAX(calcium) AS calcium_max
    , MIN(glucose) AS glucose_min, MAX(glucose) AS glucose_max
    , MIN(potassium) AS potassium_min, MAX(potassium) AS potassium_max
    , MIN(sodium) AS sodium_min, MAX(sodium) AS sodium_max
FROM mimic_icu.icustays ie
LEFT JOIN bg bg
    ON ie.subject_id = bg.subject_id
    AND bg.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND bg.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id
;

-- 入院首日格拉斯哥评分（神经系统评分）
-- first_day_gcs
-- Glasgow Coma Scale, a measure of neurological function.
-- Ranges from 3 (worst, comatose) to 15 (best, normal function).

-- Note:
-- The GCS for sedated patients is defaulted to 15 in this code.
-- This follows common practice for scoring patients with severity of illness scores.
--
--  e.g., from the SAPS II publication:
--    For sedated patients, the Glasgow Coma Score before sedation was used.
--    This was ascertained either from interviewing the physician who ordered the sedation,
--    or by reviewing the patient's medical record.
create materialized view if not exists first_day_gcs as 
WITH gcs_final AS
(
    SELECT
        gcs.*
        -- This sorts the data by GCS
        -- rn = 1 is the the lowest total GCS value
        , ROW_NUMBER () OVER
        (
            PARTITION BY gcs.stay_id
            ORDER BY gcs.GCS
        ) as gcs_seq
    FROM gcs gcs
)
SELECT
    ie.subject_id
    , ie.stay_id
    -- The minimum GCS is determined by the above row partition
    -- we only join if gcs_seq = 1
    , gcs AS gcs_min
    , gcs_motor
    , gcs_verbal
    , gcs_eyes
    , gcs_unable
FROM mimic_icu.icustays ie
LEFT JOIN gcs_final gs
    ON ie.stay_id = gs.stay_id
    AND gs.gcs_seq = 1
;


-- 入院首日身高
-- first_day_height
-- This query extracts heights for adult ICU patients.
-- It uses all information from the patient's first ICU day.
-- This is done for consistency with other queries - it's not necessarily needed.
-- Height is unlikely to change throughout a patient's stay.

-- The MIMIC-III version used echo data, this is not available in MIMIC-IV v0.4
create materialized view if not exists first_day_height as 
WITH ce AS
(
    SELECT
      c.stay_id
      , AVG(valuenum) as Height_chart
    FROM mimic_icu.chartevents c
    INNER JOIN mimic_icu.icustays ie ON
        c.stay_id = ie.stay_id
        AND c.charttime BETWEEN DATETIME_SUB(ie.intime, INTERVAL '1' DAY) AND DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    WHERE c.valuenum IS NOT NULL
    AND c.itemid in (226730) -- height
    AND c.valuenum != 0
    GROUP BY c.stay_id
)
SELECT
    ie.subject_id
    , ie.stay_id
    , ROUND(AVG(height), 2) AS height
FROM mimic_icu.icustays ie
LEFT JOIN height ht
    ON ie.stay_id = ht.stay_id
    AND ht.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND ht.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id;

-- 入院首日实验室检查
-- first_day_lab
create materialized view if not exists first_day_lab as 
WITH cbc AS
(
    SELECT
    ie.stay_id
    , MIN(hematocrit) as hematocrit_min
    , MAX(hematocrit) as hematocrit_max
    , MIN(hemoglobin) as hemoglobin_min
    , MAX(hemoglobin) as hemoglobin_max
    , MIN(platelet) as platelets_min
    , MAX(platelet) as platelets_max
    , MIN(wbc) as wbc_min
    , MAX(wbc) as wbc_max
    FROM mimic_icu.icustays ie
    LEFT JOIN complete_blood_count le
        ON le.subject_id = ie.subject_id
        AND le.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
        AND le.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    GROUP BY ie.stay_id
)
, chem AS
(
    SELECT
    ie.stay_id
    , MIN(albumin) AS albumin_min, MAX(albumin) AS albumin_max
    , MIN(globulin) AS globulin_min, MAX(globulin) AS globulin_max
    , MIN(total_protein) AS total_protein_min, MAX(total_protein) AS total_protein_max
    , MIN(aniongap) AS aniongap_min, MAX(aniongap) AS aniongap_max
    , MIN(bicarbonate) AS bicarbonate_min, MAX(bicarbonate) AS bicarbonate_max
    , MIN(bun) AS bun_min, MAX(bun) AS bun_max
    , MIN(calcium) AS calcium_min, MAX(calcium) AS calcium_max
    , MIN(chloride) AS chloride_min, MAX(chloride) AS chloride_max
    , MIN(creatinine) AS creatinine_min, MAX(creatinine) AS creatinine_max
    , MIN(glucose) AS glucose_min, MAX(glucose) AS glucose_max
    , MIN(sodium) AS sodium_min, MAX(sodium) AS sodium_max
    , MIN(potassium) AS potassium_min, MAX(potassium) AS potassium_max
    FROM mimic_icu.icustays ie
    LEFT JOIN chemistry le
        ON le.subject_id = ie.subject_id
        AND le.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
        AND le.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    GROUP BY ie.stay_id
)
, diff AS
(
    SELECT
    ie.stay_id
    , MIN(basophils_abs) AS abs_basophils_min, MAX(basophils_abs) AS abs_basophils_max
    , MIN(eosinophils_abs) AS abs_eosinophils_min, MAX(eosinophils_abs) AS abs_eosinophils_max
    , MIN(lymphocytes_abs) AS abs_lymphocytes_min, MAX(lymphocytes_abs) AS abs_lymphocytes_max
    , MIN(monocytes_abs) AS abs_monocytes_min, MAX(monocytes_abs) AS abs_monocytes_max
    , MIN(neutrophils_abs) AS abs_neutrophils_min, MAX(neutrophils_abs) AS abs_neutrophils_max
    , MIN(atypical_lymphocytes) AS atyps_min, MAX(atypical_lymphocytes) AS atyps_max
    , MIN(bands) AS bands_min, MAX(bands) AS bands_max
    , MIN(immature_granulocytes) AS imm_granulocytes_min, MAX(immature_granulocytes) AS imm_granulocytes_max
    , MIN(metamyelocytes) AS metas_min, MAX(metamyelocytes) AS metas_max
    , MIN(nrbc) AS nrbc_min, MAX(nrbc) AS nrbc_max
    FROM mimic_icu.icustays ie
    LEFT JOIN blood_differential le
        ON le.subject_id = ie.subject_id
        AND le.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
        AND le.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    GROUP BY ie.stay_id
)
, coag AS
(
    SELECT
    ie.stay_id
    , MIN(d_dimer) AS d_dimer_min, MAX(d_dimer) AS d_dimer_max
    , MIN(fibrinogen) AS fibrinogen_min, MAX(fibrinogen) AS fibrinogen_max
    , MIN(thrombin) AS thrombin_min, MAX(thrombin) AS thrombin_max
    , MIN(inr) AS inr_min, MAX(inr) AS inr_max
    , MIN(pt) AS pt_min, MAX(pt) AS pt_max
    , MIN(ptt) AS ptt_min, MAX(ptt) AS ptt_max
    FROM mimic_icu.icustays ie
    LEFT JOIN coagulation le
        ON le.subject_id = ie.subject_id
        AND le.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
        AND le.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    GROUP BY ie.stay_id
)
, enz AS
(
    SELECT
    ie.stay_id
    
    , MIN(alt) AS alt_min, MAX(alt) AS alt_max
    , MIN(alp) AS alp_min, MAX(alp) AS alp_max
    , MIN(ast) AS ast_min, MAX(ast) AS ast_max
    , MIN(amylase) AS amylase_min, MAX(amylase) AS amylase_max
    , MIN(bilirubin_total) AS bilirubin_total_min, MAX(bilirubin_total) AS bilirubin_total_max
    , MIN(bilirubin_direct) AS bilirubin_direct_min, MAX(bilirubin_direct) AS bilirubin_direct_max
    , MIN(bilirubin_indirect) AS bilirubin_indirect_min, MAX(bilirubin_indirect) AS bilirubin_indirect_max
    , MIN(ck_cpk) AS ck_cpk_min, MAX(ck_cpk) AS ck_cpk_max
    , MIN(ck_mb) AS ck_mb_min, MAX(ck_mb) AS ck_mb_max
    , MIN(ggt) AS ggt_min, MAX(ggt) AS ggt_max
    , MIN(ld_ldh) AS ld_ldh_min, MAX(ld_ldh) AS ld_ldh_max
    FROM mimic_icu.icustays ie
    LEFT JOIN enzyme le
        ON le.subject_id = ie.subject_id
        AND le.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
        AND le.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    GROUP BY ie.stay_id
)
SELECT
ie.subject_id
, ie.stay_id
-- complete blood count
, hematocrit_min, hematocrit_max
, hemoglobin_min, hemoglobin_max
, platelets_min, platelets_max
, wbc_min, wbc_max
-- chemistry
, albumin_min, albumin_max
, globulin_min, globulin_max
, total_protein_min, total_protein_max
, aniongap_min, aniongap_max
, bicarbonate_min, bicarbonate_max
, bun_min, bun_max
, calcium_min, calcium_max
, chloride_min, chloride_max
, creatinine_min, creatinine_max
, glucose_min, glucose_max
, sodium_min, sodium_max
, potassium_min, potassium_max
-- blood differential
, abs_basophils_min, abs_basophils_max
, abs_eosinophils_min, abs_eosinophils_max
, abs_lymphocytes_min, abs_lymphocytes_max
, abs_monocytes_min, abs_monocytes_max
, abs_neutrophils_min, abs_neutrophils_max
, atyps_min, atyps_max
, bands_min, bands_max
, imm_granulocytes_min, imm_granulocytes_max
, metas_min, metas_max
, nrbc_min, nrbc_max
-- coagulation
, d_dimer_min, d_dimer_max
, fibrinogen_min, fibrinogen_max
, thrombin_min, thrombin_max
, inr_min, inr_max
, pt_min, pt_max
, ptt_min, ptt_max
-- enzymes and bilirubin
, alt_min, alt_max
, alp_min, alp_max
, ast_min, ast_max
, amylase_min, amylase_max
, bilirubin_total_min, bilirubin_total_max
, bilirubin_direct_min, bilirubin_direct_max
, bilirubin_indirect_min, bilirubin_indirect_max
, ck_cpk_min, ck_cpk_max
, ck_mb_min, ck_mb_max
, ggt_min, ggt_max
, ld_ldh_min, ld_ldh_max
FROM mimic_icu.icustays ie
LEFT JOIN cbc
    ON ie.stay_id = cbc.stay_id
LEFT JOIN chem
    ON ie.stay_id = chem.stay_id
LEFT JOIN diff
    ON ie.stay_id = diff.stay_id
LEFT JOIN coag
    ON ie.stay_id = coag.stay_id
LEFT JOIN enz
    ON ie.stay_id = enz.stay_id
;

-- 入院首日肾脏代替治疗（RRT）
-- first_day_rrt
-- flag indicating if patients received dialysis during 
-- the first day of their ICU stay
create materialized view if not exists first_day_rrt as 
select
    ie.subject_id
    , ie.stay_id
    , MAX(dialysis_present) AS dialysis_present
    , MAX(dialysis_active) AS dialysis_active
    , STRING_AGG(DISTINCT dialysis_type, ', ') AS dialysis_type
FROM mimic_icu.icustays ie
LEFT JOIN rrt rrt
	ON ie.stay_id = rrt.stay_id
	AND rrt.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
	AND rrt.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id;

-- 入院首日生命体征
-- first_day_vitalsign
-- This query pivots vital signs and aggregates them
-- for the first 24 hours of a patient's stay.
create materialized view if not exists first_day_vitalsign as 
SELECT
ie.subject_id
, ie.stay_id
, MIN(heart_rate) AS heart_rate_min
, MAX(heart_rate) AS heart_rate_max
, AVG(heart_rate) AS heart_rate_mean
, MIN(sbp) AS sbp_min
, MAX(sbp) AS sbp_max
, AVG(sbp) AS sbp_mean
, MIN(dbp) AS dbp_min
, MAX(dbp) AS dbp_max
, AVG(dbp) AS dbp_mean
, MIN(mbp) AS mbp_min
, MAX(mbp) AS mbp_max
, AVG(mbp) AS mbp_mean
, MIN(resp_rate) AS resp_rate_min
, MAX(resp_rate) AS resp_rate_max
, AVG(resp_rate) AS resp_rate_mean
, MIN(temperature) AS temperature_min
, MAX(temperature) AS temperature_max
, AVG(temperature) AS temperature_mean
, MIN(spo2) AS spo2_min
, MAX(spo2) AS spo2_max
, AVG(spo2) AS spo2_mean
, MIN(glucose) AS glucose_min
, MAX(glucose) AS glucose_max
, AVG(glucose) AS glucose_mean
FROM mimic_icu.icustays ie
LEFT JOIN vitalsign ce
    ON ie.stay_id = ce.stay_id
    AND ce.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND ce.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id;

-- 入院首日尿量
-- first_day_urine_output
-- Total urine output over the first 24 hours in the ICU
create materialized view if not exists first_day_urine_output as 
SELECT
  -- patient identifiers
  ie.subject_id
  , ie.stay_id
  , SUM(urineoutput) AS urineoutput
FROM mimic_icu.icustays ie
-- Join to the outputevents table to get urine output
LEFT JOIN urine_output uo
    ON ie.stay_id = uo.stay_id
    -- ensure the data occurs during the first day
    AND uo.charttime >= ie.intime
    AND uo.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id;

-- 入院首日体重
-- first_day_weight
-- This query extracts weights for adult ICU patients on their first ICU day.
-- It does *not* use any information after the first ICU day, as weight is
-- sometimes used to monitor fluid balance.
-- The MIMIC-III version used echodata but this isn't available in MIMIC-IV.
create materialized view if not exists first_day_weight as 
SELECT
  ie.subject_id
  , ie.stay_id
  , AVG(CASE WHEN weight_type = 'admit' THEN ce.weight ELSE NULL END) AS weight_admit
  , AVG(ce.weight) AS weight
  , MIN(ce.weight) AS weight_min
  , MAX(ce.weight) AS weight_max
FROM mimic_icu.icustays ie
  -- admission weight
LEFT JOIN weight_durations ce
    ON ie.stay_id = ce.stay_id
    -- we filter to weights documented during or before the 1st day
    AND ce.starttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id
;

-- 入院24hrs的SOFA评分
-- first_day_sofa 
-- ------------------------------------------------------------------
-- Title: Sequential Organ Failure Assessment (SOFA)
-- This query extracts the sequential organ failure assessment (formally: sepsis-related organ failure assessment).
-- This score is a measure of organ failure for patients in the ICU.
-- The score is calculated on the first day of each ICU patients' stay.
-- ------------------------------------------------------------------

-- Reference for SOFA:（参考文献）
--    Jean-Louis Vincent, Rui Moreno, Jukka Takala, Sheila Willatts, Arnaldo De Mendonça,
--    Hajo Bruining, C. K. Reinhart, Peter M Suter, and L. G. Thijs.
--    "The SOFA (Sepsis-related Organ Failure Assessment) score to describe organ dysfunction/failure."
--    Intensive care medicine 22, no. 7 (1996): 707-710.

-- Variables used in SOFA:（需要用到的变量）
--  GCS, MAP, FiO2, Ventilation status (sourced from CHARTEVENTS)
--  Creatinine, Bilirubin, FiO2, PaO2, Platelets (sourced from LABEVENTS)
--  Dopamine, Dobutamine, Epinephrine, Norepinephrine (sourced from INPUTEVENTS)
--  Urine output (sourced from OUTPUTEVENTS)

-- The following views required to run this query:（需要先提取的指标）
--  1) first_day_urine_output
--  2) first_day_vitalsign
--  3) first_day_gcs
--  4) first_day_lab
--  5) first_day_bg_art
--  6) ventdurations

-- extract drug rates from derived vasopressor tables
create materialized view if not exists first_day_sofa as 
with vaso_stg as
(
  select ie.stay_id, 'norepinephrine' AS treatment, vaso_rate as rate
  FROM mimic_icu.icustays ie
  INNER JOIN norepinephrine mv
    ON ie.stay_id = mv.stay_id
    AND mv.starttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND mv.starttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
  UNION ALL
  select ie.stay_id, 'epinephrine' AS treatment, vaso_rate as rate
  FROM mimic_icu.icustays ie
  INNER JOIN epinephrine mv
    ON ie.stay_id = mv.stay_id
    AND mv.starttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND mv.starttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
  UNION ALL
  select ie.stay_id, 'dobutamine' AS treatment, vaso_rate as rate
  FROM mimic_icu.icustays ie
  INNER JOIN dobutamine mv
    ON ie.stay_id = mv.stay_id
    AND mv.starttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND mv.starttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
  UNION ALL
  select ie.stay_id, 'dopamine' AS treatment, vaso_rate as rate
  FROM mimic_icu.icustays ie
  INNER JOIN dopamine mv
    ON ie.stay_id = mv.stay_id
    AND mv.starttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND mv.starttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
)
, vaso_mv AS
(
    SELECT
    ie.stay_id
    , max(CASE WHEN treatment = 'norepinephrine' THEN rate ELSE NULL END) as rate_norepinephrine
    , max(CASE WHEN treatment = 'epinephrine' THEN rate ELSE NULL END) as rate_epinephrine
    , max(CASE WHEN treatment = 'dopamine' THEN rate ELSE NULL END) as rate_dopamine
    , max(CASE WHEN treatment = 'dobutamine' THEN rate ELSE NULL END) as rate_dobutamine
  from mimic_icu.icustays ie
  LEFT JOIN vaso_stg v
      ON ie.stay_id = v.stay_id
  GROUP BY ie.stay_id
)
, pafi1 as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  select ie.stay_id, bg.charttime
  , bg.pao2fio2ratio
  , case when vd.stay_id is not null then 1 else 0 end as IsVent
  from mimic_icu.icustays ie
  LEFT JOIN bg bg
      ON ie.subject_id = bg.subject_id
      AND bg.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
      AND bg.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
  LEFT JOIN ventilation vd
    ON ie.stay_id = vd.stay_id
    AND bg.charttime >= vd.starttime
    AND bg.charttime <= vd.endtime
    AND vd.ventilation_status = 'InvasiveVent'
)
, pafi2 as
(
  -- because pafi has an interaction between vent/PaO2:FiO2, we need two columns for the score
  -- it can happen that the lowest unventilated PaO2/FiO2 is 68, but the lowest ventilated PaO2/FiO2 is 120
  -- in this case, the SOFA score is 3, *not* 4.
  select stay_id
  , min(case when IsVent = 0 then pao2fio2ratio else null end) as PaO2FiO2_novent_min
  , min(case when IsVent = 1 then pao2fio2ratio else null end) as PaO2FiO2_vent_min
  from pafi1
  group by stay_id
)
-- Aggregate the components for the score
, scorecomp as
(
select ie.stay_id
  , v.mbp_min
  , mv.rate_norepinephrine
  , mv.rate_epinephrine
  , mv.rate_dopamine
  , mv.rate_dobutamine

  , l.creatinine_max
  , l.bilirubin_total_max as bilirubin_max
  , l.platelets_min as platelet_min

  , pf.PaO2FiO2_novent_min
  , pf.PaO2FiO2_vent_min

  , uo.UrineOutput

  , gcs.gcs_min
from mimic_icu.icustays ie
left join vaso_mv mv
  on ie.stay_id = mv.stay_id
left join pafi2 pf
 on ie.stay_id = pf.stay_id
left join first_day_vitalsign v
  on ie.stay_id = v.stay_id
left join first_day_lab l
  on ie.stay_id = l.stay_id
left join first_day_urine_output uo
  on ie.stay_id = uo.stay_id
left join first_day_gcs gcs
  on ie.stay_id = gcs.stay_id
)
, scorecalc as
(
  -- Calculate the final score
  -- note that if the underlying data is missing, the component is null
  -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
  select stay_id
  -- Respiration
  , case
      when PaO2FiO2_vent_min   < 100 then 4
      when PaO2FiO2_vent_min   < 200 then 3
      when PaO2FiO2_novent_min < 300 then 2
      when PaO2FiO2_novent_min < 400 then 1
      when coalesce(PaO2FiO2_vent_min, PaO2FiO2_novent_min) is null then null
      else 0
    end as respiration

  -- Coagulation
  , case
      when platelet_min < 20  then 4
      when platelet_min < 50  then 3
      when platelet_min < 100 then 2
      when platelet_min < 150 then 1
      when platelet_min is null then null
      else 0
    end as coagulation

  -- Liver
  , case
      -- Bilirubin checks in mg/dL
        when bilirubin_max >= 12.0 then 4
        when bilirubin_max >= 6.0  then 3
        when bilirubin_max >= 2.0  then 2
        when bilirubin_max >= 1.2  then 1
        when bilirubin_max is null then null
        else 0
      end as liver

  -- Cardiovascular
  , case
      when rate_dopamine > 15 or rate_epinephrine >  0.1 or rate_norepinephrine >  0.1 then 4
      when rate_dopamine >  5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
      when rate_dopamine >  0 or rate_dobutamine > 0 then 2
      when mbp_min < 70 then 1
      when coalesce(mbp_min, rate_dopamine, rate_dobutamine, rate_epinephrine, rate_norepinephrine) is null then null
      else 0
    end as cardiovascular

  -- Neurological failure (GCS)
  , case
      when (gcs_min >= 13 and gcs_min <= 14) then 1
      when (gcs_min >= 10 and gcs_min <= 12) then 2
      when (gcs_min >=  6 and gcs_min <=  9) then 3
      when  gcs_min <   6 then 4
      when  gcs_min is null then null
  else 0 end
    as cns

  -- Renal failure - high creatinine or low urine output
  , case
    when (creatinine_max >= 5.0) then 4
    when  UrineOutput < 200 then 4
    when (creatinine_max >= 3.5 and creatinine_max < 5.0) then 3
    when  UrineOutput < 500 then 3
    when (creatinine_max >= 2.0 and creatinine_max < 3.5) then 2
    when (creatinine_max >= 1.2 and creatinine_max < 2.0) then 1
    when coalesce(UrineOutput, creatinine_max) is null then null
  else 0 end
    as renal
  from scorecomp
)
select ie.subject_id, ie.hadm_id, ie.stay_id
  -- Combine all the scores to get SOFA
  -- Impute 0 if the score is missing
  , coalesce(respiration,0)
  + coalesce(coagulation,0)
  + coalesce(liver,0)
  + coalesce(cardiovascular,0)
  + coalesce(cns,0)
  + coalesce(renal,0)
  as SOFA
, respiration
, coagulation
, liver
, cardiovascular
, cns
, renal
from mimic_icu.icustays ie
left join scorecalc s
  on ie.stay_id = s.stay_id
;
-- 
-- organfailure

-- KDIGO-肌酐
-- kdigo_creatinine
-- Extract all creatinine values from labevents around patient's ICU stay
create materialized view if not exists kdigo_creatinine as 
WITH cr AS
(
    SELECT
        ie.hadm_id
        , ie.stay_id
        , le.charttime
        , AVG(le.valuenum) AS creat
    FROM mimic_icu.icustays ie
    LEFT JOIN mimic_hosp.labevents le
    ON ie.subject_id = le.subject_id
    AND le.ITEMID = 50912
    AND le.VALUENUM IS NOT NULL
    AND le.VALUENUM <= 150
    AND le.CHARTTIME BETWEEN DATETIME_SUB(ie.intime, INTERVAL '7' DAY) AND ie.outtime
    GROUP BY ie.hadm_id, ie.stay_id, le.charttime
)
, cr48 AS
(
    -- add in the lowest value in the previous 48 hours
    SELECT 
        cr.stay_id
        , cr.charttime
        , MIN(cr48.creat) AS creat_low_past_48hr
    FROM cr
    -- add in all creatinine values in the last 48 hours
    LEFT JOIN cr cr48
        ON cr.stay_id = cr48.stay_id
        AND cr48.charttime <  cr.charttime
        AND cr48.charttime >= DATETIME_SUB(cr.charttime, INTERVAL '48' HOUR)
    GROUP BY cr.stay_id, cr.charttime
)
, cr7 AS
(
    -- add in the lowest value in the previous 7 days
    SELECT
        cr.stay_id
        , cr.charttime
        , MIN(cr7.creat) AS creat_low_past_7day
    FROM cr
    -- add in all creatinine values in the last 7 days
    LEFT JOIN cr cr7
      ON cr.stay_id = cr7.stay_id
      AND cr7.charttime <  cr.charttime
      AND cr7.charttime >= DATETIME_SUB(cr.charttime, INTERVAL '7' DAY)
    GROUP BY cr.stay_id, cr.charttime
)
SELECT 
    cr.hadm_id
    , cr.stay_id
    , cr.charttime
    , cr.creat
    , cr48.creat_low_past_48hr
    , cr7.creat_low_past_7day
FROM cr
LEFT JOIN cr48
    ON cr.stay_id = cr48.stay_id
    AND cr.charttime = cr48.charttime
LEFT JOIN cr7
    ON cr.stay_id = cr7.stay_id
    AND cr.charttime = cr7.charttime
;

-- KDIGO-尿量
-- kdigo_uo
create materialized view if not exists kdigo_uo as 
with ur_stg as
(
  select io.stay_id, io.charttime
  -- we have joined each row to all rows preceding within 24 hours
  -- we can now sum these rows to get total UO over the last 24 hours
  -- we can use case statements to restrict it to only the last 6/12 hours
  -- therefore we have three sums:
  -- 1) over a 6 hour period
  -- 2) over a 12 hour period
  -- 3) over a 24 hour period
  -- note that we assume data charted at charttime corresponds to 1 hour of UO
  -- therefore we use '5' and '11' to restrict the period, rather than 6/12
  -- this assumption may overestimate UO rate when documentation is done less than hourly

  -- 6 hours
  , sum(case when iosum.charttime >= DATETIME_SUB(io.charttime, interval '5' hour)
      then iosum.urineoutput
    else null end) as UrineOutput_6hr
  -- 12 hours
  , sum(case when iosum.charttime >= DATETIME_SUB(io.charttime, interval '11' hour)
      then iosum.urineoutput
    else null end) as UrineOutput_12hr
  -- 24 hours
  , sum(iosum.urineoutput) as UrineOutput_24hr
    
  -- calculate the number of hours over which we've tabulated UO
  , ROUND(CAST(
      DATETIME_DIFF(io.charttime, 
        -- below MIN() gets the earliest time that was used in the summation 
        MIN(case when iosum.charttime >= DATETIME_SUB(io.charttime, interval '5' hour)
          then iosum.charttime
        else null end),
        'SECOND') AS NUMERIC)/3600.0, 4)
     AS uo_tm_6hr
  -- repeat extraction for 12 hours and 24 hours
  , ROUND(CAST(
      DATETIME_DIFF(io.charttime,
        MIN(case when iosum.charttime >= DATETIME_SUB(io.charttime, interval '11' hour)
          then iosum.charttime
        else null end),
        'SECOND') AS NUMERIC)/3600.0, 4)
   AS uo_tm_12hr
  , ROUND(CAST(
      DATETIME_DIFF(io.charttime, MIN(iosum.charttime), 'SECOND')
   AS NUMERIC)/3600.0, 4) AS uo_tm_24hr
  from urine_output io
  -- this join gives all UO measurements over the 24 hours preceding this row
  left join urine_output iosum
    on  io.stay_id = iosum.stay_id
    and iosum.charttime <= io.charttime
    and iosum.charttime >= DATETIME_SUB(io.charttime, interval '23' hour)
  group by io.stay_id, io.charttime
)
select
  ur.stay_id
, ur.charttime
, wd.weight
, ur.urineoutput_6hr
, ur.urineoutput_12hr
, ur.urineoutput_24hr
-- calculate rates - adding 1 hour as we assume data charted at 10:00 corresponds to previous hour
, ROUND(CAST((ur.UrineOutput_6hr/wd.weight/(uo_tm_6hr+1))   AS NUMERIC), 4) AS uo_rt_6hr
, ROUND(CAST((ur.UrineOutput_12hr/wd.weight/(uo_tm_12hr+1)) AS NUMERIC), 4) AS uo_rt_12hr
, ROUND(CAST((ur.UrineOutput_24hr/wd.weight/(uo_tm_24hr+1)) AS NUMERIC), 4) AS uo_rt_24hr
-- number of hours between current UO time and earliest charted UO within the X hour window
, uo_tm_6hr
, uo_tm_12hr
, uo_tm_24hr
from ur_stg ur
left join weight_durations wd
  on  ur.stay_id = wd.stay_id
  and ur.charttime >= wd.starttime
  and ur.charttime <  wd.endtime
;

-- KDIGO-急性肾衰竭
-- kdigo_stages
-- This query checks if the patient had AKI according to KDIGO.
-- AKI is calculated every time a creatinine or urine output measurement occurs.
-- Baseline creatinine is defined as the lowest creatinine in the past 7 days.

-- get creatinine stages
create materialized view if not exists kdigo_stages as 
with cr_stg AS
(
  SELECT
    cr.stay_id
    , cr.charttime
    , cr.creat_low_past_7day 
    , cr.creat_low_past_48hr
    , cr.creat
    , case
        -- 3x baseline
        when cr.creat >= (cr.creat_low_past_7day*3.0) then 3
        -- *OR* cr >= 4.0 with associated increase
        when cr.creat >= 4
        -- For patients reaching Stage 3 by SCr >4.0 mg/dl
        -- require that the patient first achieve ... acute increase >= 0.3 within 48 hr
        -- *or* an increase of >= 1.5 times baseline
        and (cr.creat_low_past_48hr <= 3.7 OR cr.creat >= (1.5*cr.creat_low_past_7day))
            then 3 
        -- TODO: initiation of RRT
        when cr.creat >= (cr.creat_low_past_7day*2.0) then 2
        when cr.creat >= (cr.creat_low_past_48hr+0.3) then 1
        when cr.creat >= (cr.creat_low_past_7day*1.5) then 1
    else 0 end as aki_stage_creat
  FROM kdigo_creatinine cr
)
-- stages for UO / creat
, uo_stg as
(
  select
      uo.stay_id
    , uo.charttime
    , uo.weight
    , uo.uo_rt_6hr
    , uo.uo_rt_12hr
    , uo.uo_rt_24hr
    -- AKI stages according to urine output
    , CASE
        WHEN uo.uo_rt_6hr IS NULL THEN NULL
        -- require patient to be in ICU for at least 6 hours to stage UO
        WHEN uo.charttime <= DATETIME_ADD(ie.intime, INTERVAL '6' HOUR) THEN 0
        -- require the UO rate to be calculated over half the period
        -- i.e. for uo rate over 24 hours, require documentation at least 12 hr apart
        WHEN uo.uo_tm_24hr >= 11 AND uo.uo_rt_24hr < 0.3 THEN 3
        WHEN uo.uo_tm_12hr >= 5 AND uo.uo_rt_12hr = 0 THEN 3
        WHEN uo.uo_tm_12hr >= 5 AND uo.uo_rt_12hr < 0.5 THEN 2
        WHEN uo.uo_tm_6hr >= 2 AND uo.uo_rt_6hr  < 0.5 THEN 1
    ELSE 0 END AS aki_stage_uo
  from kdigo_uo uo
  INNER JOIN mimic_icu.icustays ie
    ON uo.stay_id = ie.stay_id
)
-- get all charttimes documented
, tm_stg AS
(
    SELECT
      stay_id, charttime
    FROM cr_stg
    UNION DISTINCT
    SELECT
      stay_id, charttime
    FROM uo_stg
)
select
    ie.subject_id
  , ie.hadm_id
  , ie.stay_id
  , tm.charttime
  , cr.creat_low_past_7day 
  , cr.creat_low_past_48hr
  , cr.creat
  , cr.aki_stage_creat
  , uo.uo_rt_6hr
  , uo.uo_rt_12hr
  , uo.uo_rt_24hr
  , uo.aki_stage_uo
  -- Classify AKI using both creatinine/urine output criteria
  , GREATEST(cr.aki_stage_creat, uo.aki_stage_uo) AS aki_stage
FROM mimic_icu.icustays ie
-- get all possible charttimes as listed in tm_stg
LEFT JOIN tm_stg tm
  ON ie.stay_id = tm.stay_id
LEFT JOIN cr_stg cr
  ON ie.stay_id = cr.stay_id
  AND tm.charttime = cr.charttime
LEFT JOIN uo_stg uo
  ON ie.stay_id = uo.stay_id
  AND tm.charttime = uo.charttime
;

-- 终末期肝病模型（MELD）
-- meld
-- Model for end-stage liver disease (MELD)
-- This model is used to determine prognosis and receipt of liver transplantation.

-- Reference:（参考文献）
--  Kamath PS, Wiesner RH, Malinchoc M, Kremers W, Therneau TM,
--  Kosberg CL, D'Amico G, Dickson ER, Kim WR.
--  A model to predict survival in patients with end-stage liver disease.
--  Hepatology. 2001 Feb;33(2):464-70.


-- Updated January 2016 to include serum sodium, see:
--  https://optn.transplant.hrsa.gov/news/meld-serum-sodium-policy-changes/

-- Here is the relevant portion of the policy note:
--    9.1.D MELD Score
--    Candidates who are at least 12 years old receive an initial MELD(i) score equal to:
--    0.957 x ln(creatinine mg/dL) + 0.378 x ln(bilirubin mg/dL) + 1.120 x ln(INR) + 0.643

--    Laboratory values less than 1.0 will be set to 1.0 when calculating a candidate’s MELD
--    score.

--    The following candidates will receive a creatinine value of 4.0 mg/dL:
--    - Candidates with a creatinine value greater than 4.0 mg/dL
--    - Candidates who received two or more dialysis treatments within the prior week
--    - Candidates who received 24 hours of continuous veno-venous hemodialysis (CVVHD) within the prior week

--    The maximum MELD score is 40. The MELD score derived from this calculation will be rounded to the tenth decimal place and then multiplied by 10.

--    For candidates with an initial MELD score greater than 11, The MELD score is then recalculated as follows:
--    MELD = MELD(i) + 1.32*(137-Na) – [0.033*MELD(i)*(137-Na)]
--    Sodium values less than 125 mmol/L will be set to 125, and values greater than 137 mmol/L will be set to 137.



-- TODO needed in this code:
--  1. identify 2x dialysis in the past week, or 24 hours of CVVH
--      at the moment it just checks for any dialysis on the first day
--  2. identify cholestatic or alcoholic liver disease
--      0.957 x ln(creatinine mg/dL) + 0.378 x ln(bilirubin mg/dL) + 1.120 x ln(INR) + 0.643 x etiology
--      (0 if cholestatic or alcoholic, 1 otherwise)
--  3. adjust the serum sodium using the corresponding glucose measurement
--      Measured sodium + 0.024 * (Serum glucose - 100)   (Hiller, 1999)
create materialized view if not exists meld as 
WITH cohort AS
(
SELECT 
    ie.subject_id
    , ie.hadm_id
    , ie.stay_id
    , ie.intime
    , ie.outtime

    , labs.creatinine_max
    , labs.bilirubin_total_max
    , labs.inr_max
    , labs.sodium_min

    , r.dialysis_present AS rrt

FROM mimic_icu.icustays ie
-- join to custom tables to get more data....
LEFT JOIN first_day_lab labs
  ON ie.stay_id = labs.stay_id
LEFT JOIN first_day_rrt r
  ON ie.stay_id = r.stay_id
)
, score as
(
  SELECT 
    subject_id
    , hadm_id
    , stay_id
    , rrt
    , creatinine_max
    , bilirubin_total_max
    , inr_max
    , sodium_min

    -- TODO: Corrected Sodium
    , CASE
        WHEN sodium_min is null
          THEN 0.0
        WHEN sodium_min > 137
          THEN 0.0
        WHEN sodium_min < 125
          THEN 12.0 -- 137 - 125 = 12
        else 137.0-sodium_min
      end as sodium_score

    -- if hemodialysis, value for Creatinine is automatically set to 4.0
    , CASE
        WHEN rrt = 1 or creatinine_max > 4.0
          THEN (0.957 * ln(4))
        -- if creatinine < 1, score is 1
        WHEN creatinine_max < 1
          THEN (0.957 * ln(1))
        else 0.957 * coalesce(ln(creatinine_max),ln(1))
      end as creatinine_score

    , CASE
        -- if value < 1, score is 1
        WHEN bilirubin_total_max < 1
          THEN 0.378 * ln(1)
        else 0.378 * coalesce(ln(bilirubin_total_max),ln(1))
      end as bilirubin_score

    , CASE
        WHEN inr_max < 1
          THEN ( 1.120 * ln(1) + 0.643 )
        else ( 1.120 * coalesce(ln(inr_max),ln(1)) + 0.643 )
      end as inr_score

  FROM cohort
)
, score2 as
(
  SELECT
    subject_id
    , hadm_id
    , stay_id
    , rrt
    , creatinine_max
    , bilirubin_total_max
    , inr_max
    , sodium_min

    , creatinine_score
    , sodium_score
    , bilirubin_score
    , inr_score

    , CASE
        WHEN (creatinine_score + bilirubin_score + inr_score) > 4
          THEN 40.0
        else
          round(cast(creatinine_score + bilirubin_score + inr_score as numeric),1)*10
        end as meld_initial
  FROM score
)
SELECT
  subject_id
  , hadm_id
  , stay_id

  -- MELD Score without sodium change
  , meld_initial

  -- MELD Score (2016) = MELD*10 + 1.32*(137-Na) – [0.033*MELD*10*(137-Na)]
  , CASE
      WHEN meld_initial > 11
        THEN meld_initial + 1.32*sodium_score - 0.033*meld_initial*sodium_score
      else
        meld_initial
      end as meld

  -- original variables
  , rrt
  , creatinine_max
  , bilirubin_total_max
  , inr_max
  , sodium_min

FROM score2
;
-- 
-- score

-- APS III（阿帕奇评分？）
-- apsiii
-- ------------------------------------------------------------------
-- Title: Acute Physiology Score III (APS III)
-- This query extracts the acute physiology score III.
-- This score is a measure of patient severity of illness.
-- The score is calculated on the first day of each ICU patients' stay.
-- ------------------------------------------------------------------

-- Reference for APS III:（参考文献）
--    Knaus WA, Wagner DP, Draper EA, Zimmerman JE, Bergner M, Bastos PG, Sirio CA, Murphy DJ, Lotring T, Damiano A.
--    The APACHE III prognostic system. Risk prediction of hospital mortality for critically ill hospitalized adults.
--    Chest Journal. 1991 Dec 1;100(6):1619-36.

-- Reference for the equation for calibrating APS III to hospital mortality:
--    Johnson, A. E. W. (2015). Mortality prediction and acuity assessment in critical care.
--    University of Oxford, Oxford, UK.

-- Variables used in APS III:（需要用到的变量）
--  GCS
--  VITALS: Heart rate, mean blood pressure, temperature, respiration rate
--  FLAGS: ventilation/cpap, chronic dialysis
--  IO: urine output
--  LABS: pao2, A-aDO2, hematocrit, WBC, creatinine
--        , blood urea nitrogen, sodium, albumin, bilirubin, glucose, pH, pCO2

-- Note:
--  The score is calculated for *all* ICU patients, with the assumption that the user will subselect appropriate stay_ids.
--  For example, the score is calculated for neonates, but it is likely inappropriate to actually use the score values for these patients.

-- List of TODO:
-- The site of temperature is not incorporated. Axillary measurements should be increased by 1 degree.
-- Unfortunately the data for metavision is not available at the moment.
--  674 | Temp. Site
--  224642 | Temperature Site
create materialized view if not exists apsiii as 
with pa as
(
  select ie.stay_id, bg.charttime
  , po2 as PaO2
  , ROW_NUMBER() over (partition by ie.stay_id ORDER BY bg.po2 DESC) as rn
  from bg bg
  INNER JOIN mimic_icu.icustays ie
    ON bg.hadm_id = ie.hadm_id
    AND bg.charttime >= ie.intime AND bg.charttime < ie.outtime
  left join ventilation vd
    on ie.stay_id = vd.stay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
    and vd.ventilation_status = 'InvasiveVent'
  WHERE vd.stay_id is null -- patient is *not* ventilated
  -- and fio2 < 50, or if no fio2, assume room air
  AND coalesce(fio2, fio2_chartevents, 21) < 50
  AND bg.po2 IS NOT NULL
  AND bg.specimen_pred = 'ART.'
)
, aa as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  -- also join to cpap table for the same purpose
  select ie.stay_id, bg.charttime
  , bg.aado2
  , ROW_NUMBER() over (partition by ie.stay_id ORDER BY bg.aado2 DESC) as rn
  -- row number indicating the highest AaDO2
  from bg bg
  INNER JOIN mimic_icu.icustays ie
    ON bg.hadm_id = ie.hadm_id
    AND bg.charttime >= ie.intime AND bg.charttime < ie.outtime
  INNER JOIN ventilation vd
    on ie.stay_id = vd.stay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
    and vd.ventilation_status = 'InvasiveVent'
  WHERE vd.stay_id is not null -- patient is ventilated
  AND coalesce(fio2, fio2_chartevents) >= 50
  AND bg.aado2 IS NOT NULL
  AND bg.specimen_pred = 'ART.'
)
-- because ph/pco2 rules are an interaction *within* a blood gas, we calculate them here
-- the worse score is then taken for the final calculation
, acidbase as
(
  select ie.stay_id
  , ph, pco2 as paco2
  , case
      when ph is null or pco2 is null then null
      when ph < 7.20 then
        case
          when pco2 < 50 then 12
          else 4
        end
      when ph < 7.30 then
        case
          when pco2 < 30 then 9
          when pco2 < 40 then 6
          when pco2 < 50 then 3
          else 2
        end
      when ph < 7.35 then
        case
          when pco2 < 30 then 9
          when pco2 < 45 then 0
          else 1
        end
      when ph < 7.45 then
        case
          when pco2 < 30 then 5
          when pco2 < 45 then 0
          else 1
        end
      when ph < 7.50 then
        case
          when pco2 < 30 then 5
          when pco2 < 35 then 0
          when pco2 < 45 then 2
          else 12
        end
      when ph < 7.60 then
        case
          when pco2 < 40 then 3
          else 12
        end
      else -- ph >= 7.60
        case
          when pco2 < 25 then 0
          when pco2 < 40 then 3
          else 12
        end
    end as acidbase_score
  from bg bg
  INNER JOIN mimic_icu.icustays ie
    ON bg.hadm_id = ie.hadm_id
    AND bg.charttime >= ie.intime AND bg.charttime < ie.outtime
  where ph is not null and pco2 is not null
  AND bg.specimen_pred = 'ART.'
)
, acidbase_max as
(
  select stay_id, acidbase_score, ph, paco2
    -- create integer which indexes maximum value of score with 1
  , ROW_NUMBER() over (partition by stay_id ORDER BY acidbase_score DESC) as acidbase_rn
  from acidbase
)
-- define acute renal failure (ARF) as:
--  creatinine >=1.5 mg/dl
--  and urine output <410 cc/day
--  and no chronic dialysis
, arf as
(
  select ie.stay_id
    , case
        when labs.creatinine_max >= 1.5
        and  uo.urineoutput < 410
        -- acute renal failure is only coded if the patient is not on chronic dialysis
        -- we use ICD-9 coding of ESRD as a proxy for chronic dialysis
        and  icd.ckd = 0
          then 1
      else 0 end as arf
  FROM mimic_icu.icustays ie
  left join first_day_urine_output uo
    on ie.stay_id = uo.stay_id
  left join first_day_lab labs
    on ie.stay_id = labs.stay_id
  left join
  (
    select hadm_id
      , max(case
          -- severe kidney failure requiring use of dialysis
          when icd_version = 9 AND SUBSTR(icd_code, 1, 4) in ('5854','5855','5856') then 1
          when icd_version = 10 AND SUBSTR(icd_code, 1, 4) in ('N184','N185','N186') then 1
          -- we do not include 5859 as that is sometimes coded for acute-on-chronic ARF
        else 0 end)
      as ckd
    from mimic_hosp.diagnoses_icd
    group by hadm_id
  ) icd
    on ie.hadm_id = icd.hadm_id
)
-- first day mechanical ventilation
, vent AS
(
    SELECT ie.stay_id
    , MAX(
        CASE WHEN v.stay_id IS NOT NULL THEN 1 ELSE 0 END
    ) AS vent
    FROM mimic_icu.icustays ie
    LEFT JOIN ventilation v
        ON ie.stay_id = v.stay_id
        AND (
            v.starttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
        OR v.endtime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
        OR v.starttime <= ie.intime AND v.endtime >= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
        )
        AND v.ventilation_status = 'InvasiveVent'
    GROUP BY ie.stay_id
)
, cohort as
(
select ie.subject_id, ie.hadm_id, ie.stay_id
      , ie.intime
      , ie.outtime

      , vital.heart_rate_min
      , vital.heart_rate_max
      , vital.mbp_min
      , vital.mbp_max
      , vital.temperature_min
      , vital.temperature_max
      , vital.resp_rate_min
      , vital.resp_rate_max

      , pa.pao2
      , aa.aado2

      , ab.ph
      , ab.paco2
      , ab.acidbase_score

      , labs.hematocrit_min
      , labs.hematocrit_max
      , labs.wbc_min
      , labs.wbc_max
      , labs.creatinine_min
      , labs.creatinine_max
      , labs.bun_min
      , labs.bun_max
      , labs.sodium_min
      , labs.sodium_max
      , labs.albumin_min
      , labs.albumin_max
      , labs.bilirubin_total_min AS bilirubin_min
      , labs.bilirubin_total_max AS bilirubin_max

      , case
          when labs.glucose_max is null and vital.glucose_max is null
            then null
          when labs.glucose_max is null or vital.glucose_max > labs.glucose_max
            then vital.glucose_max
          when vital.glucose_max is null or labs.glucose_max > vital.glucose_max
            then labs.glucose_max
          else labs.glucose_max -- if equal, just pick labs
        end as glucose_max

      , case
          when labs.glucose_min is null and vital.glucose_min is null
            then null
          when labs.glucose_min is null or vital.glucose_min < labs.glucose_min
            then vital.glucose_min
          when vital.glucose_min is null or labs.glucose_min < vital.glucose_min
            then labs.glucose_min
          else labs.glucose_min -- if equal, just pick labs
        end as glucose_min

      -- , labs.bicarbonate_min
      -- , labs.bicarbonate_max
      , vent.vent
      , uo.urineoutput
      -- gcs and its components
      , gcs.gcs_min AS mingcs
      , gcs.gcs_motor, gcs.gcs_verbal,  gcs.gcs_eyes, gcs.gcs_unable
      -- acute renal failure
      , arf.arf as arf

FROM mimic_icu.icustays ie
inner join mimic_hosp.admissions adm
  on ie.hadm_id = adm.hadm_id
inner join mimic_hosp.patients pat
  on ie.subject_id = pat.subject_id

-- join to above views - the row number filters to 1 row per stay_id
left join pa
  on  ie.stay_id = pa.stay_id
  and pa.rn = 1
left join aa
  on  ie.stay_id = aa.stay_id
  and aa.rn = 1
left join acidbase_max ab
  on  ie.stay_id = ab.stay_id
  and ab.acidbase_rn = 1
left join arf
  on ie.stay_id = arf.stay_id

-- join to custom tables to get more data....
left join vent
  on ie.stay_id = vent.stay_id
left join first_day_gcs gcs
  on ie.stay_id = gcs.stay_id
left join first_day_vitalsign vital
  on ie.stay_id = vital.stay_id
left join first_day_urine_output uo
  on ie.stay_id = uo.stay_id
left join first_day_lab labs
  on ie.stay_id = labs.stay_id
)
-- First, we calculate the score for the minimum values
, score_min as
(
  select cohort.subject_id, cohort.hadm_id, cohort.stay_id
  , case
      when heart_rate_min is null then null
      when heart_rate_min <   40 then 8
      when heart_rate_min <   50 then 5
      when heart_rate_min <  100 then 0
      when heart_rate_min <  110 then 1
      when heart_rate_min <  120 then 5
      when heart_rate_min <  140 then 7
      when heart_rate_min <  155 then 13
      when heart_rate_min >= 155 then 17
    end as hr_score

  , case
      when mbp_min is null then null
      when mbp_min <   40 then 23
      when mbp_min <   60 then 15
      when mbp_min <   70 then 7
      when mbp_min <   80 then 6
      when mbp_min <  100 then 0
      when mbp_min <  120 then 4
      when mbp_min <  130 then 7
      when mbp_min <  140 then 9
      when mbp_min >= 140 then 10
    end as mbp_score

  -- TODO: add 1 degree to axillary measurements
  , case
      when temperature_min is null then null
      when temperature_min <  33.0 then 20
      when temperature_min <  33.5 then 16
      when temperature_min <  34.0 then 13
      when temperature_min <  35.0 then 8
      when temperature_min <  36.0 then 2
      when temperature_min <  40.0 then 0
      when temperature_min >= 40.0 then 4
    end as temp_score

  , case
      when resp_rate_min is null then null
      -- special case for ventilated patients
      when vent = 1 and resp_rate_min < 14 then 0
      when resp_rate_min <   6 then 17
      when resp_rate_min <  12 then 8
      when resp_rate_min <  14 then 7
      when resp_rate_min <  25 then 0
      when resp_rate_min <  35 then 6
      when resp_rate_min <  40 then 9
      when resp_rate_min <  50 then 11
      when resp_rate_min >= 50 then 18
    end as resp_rate_score

  , case
      when hematocrit_min is null then null
      when hematocrit_min <   41.0 then 3
      when hematocrit_min <   50.0 then 0
      when hematocrit_min >=  50.0 then 3
    end as hematocrit_score

  , case
      when wbc_min is null then null
      when wbc_min <   1.0 then 19
      when wbc_min <   3.0 then 5
      when wbc_min <  20.0 then 0
      when wbc_min <  25.0 then 1
      when wbc_min >= 25.0 then 5
    end as wbc_score

  , case
      when creatinine_min is null then null
      when arf = 1 and creatinine_min <  1.5 then 0
      when arf = 1 and creatinine_min >= 1.5 then 10
      when creatinine_min <   0.5 then 3
      when creatinine_min <   1.5 then 0
      when creatinine_min <  1.95 then 4
      when creatinine_min >= 1.95 then 7
    end as creatinine_score

  , case
      when bun_min is null then null
      when bun_min <  17.0 then 0
      when bun_min <  20.0 then 2
      when bun_min <  40.0 then 7
      when bun_min <  80.0 then 11
      when bun_min >= 80.0 then 12
    end as bun_score

  , case
      when sodium_min is null then null
      when sodium_min <  120 then 3
      when sodium_min <  135 then 2
      when sodium_min <  155 then 0
      when sodium_min >= 155 then 4
    end as sodium_score

  , case
      when albumin_min is null then null
      when albumin_min <  2.0 then 11
      when albumin_min <  2.5 then 6
      when albumin_min <  4.5 then 0
      when albumin_min >= 4.5 then 4
    end as albumin_score

  , case
      when bilirubin_min is null then null
      when bilirubin_min <  2.0 then 0
      when bilirubin_min <  3.0 then 5
      when bilirubin_min <  5.0 then 6
      when bilirubin_min <  8.0 then 8
      when bilirubin_min >= 8.0 then 16
    end as bilirubin_score

  , case
      when glucose_min is null then null
      when glucose_min <   40 then 8
      when glucose_min <   60 then 9
      when glucose_min <  200 then 0
      when glucose_min <  350 then 3
      when glucose_min >= 350 then 5
    end as glucose_score

from cohort
)
, score_max as
(
  select cohort.subject_id, cohort.hadm_id, cohort.stay_id
    , case
        when heart_rate_max is null then null
        when heart_rate_max <   40 then 8
        when heart_rate_max <   50 then 5
        when heart_rate_max <  100 then 0
        when heart_rate_max <  110 then 1
        when heart_rate_max <  120 then 5
        when heart_rate_max <  140 then 7
        when heart_rate_max <  155 then 13
        when heart_rate_max >= 155 then 17
      end as hr_score

    , case
        when mbp_max is null then null
        when mbp_max <   40 then 23
        when mbp_max <   60 then 15
        when mbp_max <   70 then 7
        when mbp_max <   80 then 6
        when mbp_max <  100 then 0
        when mbp_max <  120 then 4
        when mbp_max <  130 then 7
        when mbp_max <  140 then 9
        when mbp_max >= 140 then 10
      end as mbp_score

    -- TODO: add 1 degree to axillary measurements
    , case
        when temperature_max is null then null
        when temperature_max <  33.0 then 20
        when temperature_max <  33.5 then 16
        when temperature_max <  34.0 then 13
        when temperature_max <  35.0 then 8
        when temperature_max <  36.0 then 2
        when temperature_max <  40.0 then 0
        when temperature_max >= 40.0 then 4
      end as temp_score

    , case
        when resp_rate_max is null then null
        -- special case for ventilated patients
        when vent = 1 and resp_rate_max < 14 then 0
        when resp_rate_max <   6 then 17
        when resp_rate_max <  12 then 8
        when resp_rate_max <  14 then 7
        when resp_rate_max <  25 then 0
        when resp_rate_max <  35 then 6
        when resp_rate_max <  40 then 9
        when resp_rate_max <  50 then 11
        when resp_rate_max >= 50 then 18
      end as resp_rate_score

    , case
        when hematocrit_max is null then null
        when hematocrit_max <   41.0 then 3
        when hematocrit_max <   50.0 then 0
        when hematocrit_max >=  50.0 then 3
      end as hematocrit_score

    , case
        when wbc_max is null then null
        when wbc_max <   1.0 then 19
        when wbc_max <   3.0 then 5
        when wbc_max <  20.0 then 0
        when wbc_max <  25.0 then 1
        when wbc_max >= 25.0 then 5
      end as wbc_score

    , case
        when creatinine_max is null then null
        when arf = 1 and creatinine_max <  1.5 then 0
        when arf = 1 and creatinine_max >= 1.5 then 10
        when creatinine_max <   0.5 then 3
        when creatinine_max <   1.5 then 0
        when creatinine_max <  1.95 then 4
        when creatinine_max >= 1.95 then 7
      end as creatinine_score

    , case
        when bun_max is null then null
        when bun_max <  17.0 then 0
        when bun_max <  20.0 then 2
        when bun_max <  40.0 then 7
        when bun_max <  80.0 then 11
        when bun_max >= 80.0 then 12
      end as bun_score

    , case
        when sodium_max is null then null
        when sodium_max <  120 then 3
        when sodium_max <  135 then 2
        when sodium_max <  155 then 0
        when sodium_max >= 155 then 4
      end as sodium_score

    , case
        when albumin_max is null then null
        when albumin_max <  2.0 then 11
        when albumin_max <  2.5 then 6
        when albumin_max <  4.5 then 0
        when albumin_max >= 4.5 then 4
      end as albumin_score

    , case
        when bilirubin_max is null then null
        when bilirubin_max <  2.0 then 0
        when bilirubin_max <  3.0 then 5
        when bilirubin_max <  5.0 then 6
        when bilirubin_max <  8.0 then 8
        when bilirubin_max >= 8.0 then 16
      end as bilirubin_score

    , case
        when glucose_max is null then null
        when glucose_max <   40 then 8
        when glucose_max <   60 then 9
        when glucose_max <  200 then 0
        when glucose_max <  350 then 3
        when glucose_max >= 350 then 5
      end as glucose_score

from cohort
)
-- Combine together the scores for min/max, using the following rules:
--  1) select the value furthest from a predefined normal value
--  2) if both equidistant, choose the one which gives a worse score
--  3) calculate score for acid-base abnormalities as it requires interactions
-- sometimes the code is a bit redundant, i.e. we know the max would always be furthest from 0
, scorecomp as
(
  select co.*
  -- The rules for APS III require the definition of a "worst" value
  -- This value is defined as whatever value is furthest from a predefined normal
  -- e.g., for heart rate, worst is defined as furthest from 75
  , case
      when heart_rate_max is null then null
      when abs(heart_rate_max-75) > abs(heart_rate_min-75)
        then smax.hr_score
      when abs(heart_rate_max-75) < abs(heart_rate_min-75)
        then smin.hr_score
      when abs(heart_rate_max-75) = abs(heart_rate_min-75)
      and  smax.hr_score >= smin.hr_score
        then smax.hr_score
      when abs(heart_rate_max-75) = abs(heart_rate_min-75)
      and  smax.hr_score < smin.hr_score
        then smin.hr_score
    end as hr_score

  , case
      when mbp_max is null then null
      when abs(mbp_max-90) > abs(mbp_min-90)
        then smax.mbp_score
      when abs(mbp_max-90) < abs(mbp_min-90)
        then smin.mbp_score
      -- values are equidistant - pick the larger score
      when abs(mbp_max-90) = abs(mbp_min-90)
      and  smax.mbp_score >= smin.mbp_score
        then smax.mbp_score
      when abs(mbp_max-90) = abs(mbp_min-90)
      and  smax.mbp_score < smin.mbp_score
        then smin.mbp_score
    end as mbp_score

  , case
      when temperature_max is null then null
      when abs(temperature_max-38) > abs(temperature_min-38)
        then smax.temp_score
      when abs(temperature_max-38) < abs(temperature_min-38)
        then smin.temp_score
      -- values are equidistant - pick the larger score
      when abs(temperature_max-38) = abs(temperature_min-38)
      and  smax.temp_score >= smin.temp_score
        then smax.temp_score
      when abs(temperature_max-38) = abs(temperature_min-38)
      and  smax.temp_score < smin.temp_score
        then smin.temp_score
    end as temp_score

  , case
      when resp_rate_max is null then null
      when abs(resp_rate_max-19) > abs(resp_rate_min-19)
        then smax.resp_rate_score
      when abs(resp_rate_max-19) < abs(resp_rate_min-19)
        then smin.resp_rate_score
      -- values are equidistant - pick the larger score
      when abs(resp_rate_max-19) = abs(resp_rate_max-19)
      and  smax.resp_rate_score >= smin.resp_rate_score
        then smax.resp_rate_score
      when abs(resp_rate_max-19) = abs(resp_rate_max-19)
      and  smax.resp_rate_score < smin.resp_rate_score
        then smin.resp_rate_score
    end as resp_rate_score

  , case
      when hematocrit_max is null then null
      when abs(hematocrit_max-45.5) > abs(hematocrit_min-45.5)
        then smax.hematocrit_score
      when abs(hematocrit_max-45.5) < abs(hematocrit_min-45.5)
        then smin.hematocrit_score
      -- values are equidistant - pick the larger score
      when abs(hematocrit_max-45.5) = abs(hematocrit_max-45.5)
      and  smax.hematocrit_score >= smin.hematocrit_score
        then smax.hematocrit_score
      when abs(hematocrit_max-45.5) = abs(hematocrit_max-45.5)
      and  smax.hematocrit_score < smin.hematocrit_score
        then smin.hematocrit_score
    end as hematocrit_score

  , case
      when wbc_max is null then null
      when abs(wbc_max-11.5) > abs(wbc_min-11.5)
        then smax.wbc_score
      when abs(wbc_max-11.5) < abs(wbc_min-11.5)
        then smin.wbc_score
      -- values are equidistant - pick the larger score
      when abs(wbc_max-11.5) = abs(wbc_max-11.5)
      and  smax.wbc_score >= smin.wbc_score
        then smax.wbc_score
      when abs(wbc_max-11.5) = abs(wbc_max-11.5)
      and  smax.wbc_score < smin.wbc_score
        then smin.wbc_score
    end as wbc_score


  -- For some labs, "furthest from normal" doesn't make sense
  -- e.g. creatinine w/ ARF, the minimum could be 0.3, and the max 1.6
  -- while the minimum of 0.3 is "further from 1", seems like the max should be scored

  , case
      when creatinine_max is null then null
      -- if they have arf then use the max to score
      when arf = 1 then smax.creatinine_score
      -- otherwise furthest from 1
      when abs(creatinine_max-1) > abs(creatinine_min-1)
        then smax.creatinine_score
      when abs(creatinine_max-1) < abs(creatinine_min-1)
        then smin.creatinine_score
      -- values are equidistant
      when smax.creatinine_score >= smin.creatinine_score
        then smax.creatinine_score
      when smax.creatinine_score < smin.creatinine_score
        then smin.creatinine_score
    end as creatinine_score

  -- the rule for BUN is the furthest from 0.. equivalent to the max value
  , case
      when bun_max is null then null
      else smax.bun_score
    end as bun_score

  , case
      when sodium_max is null then null
      when abs(sodium_max-145.5) > abs(sodium_min-145.5)
        then smax.sodium_score
      when abs(sodium_max-145.5) < abs(sodium_min-145.5)
        then smin.sodium_score
      -- values are equidistant - pick the larger score
      when abs(sodium_max-145.5) = abs(sodium_max-145.5)
      and  smax.sodium_score >= smin.sodium_score
        then smax.sodium_score
      when abs(sodium_max-145.5) = abs(sodium_max-145.5)
      and  smax.sodium_score < smin.sodium_score
        then smin.sodium_score
    end as sodium_score

  , case
      when albumin_max is null then null
      when abs(albumin_max-3.5) > abs(albumin_min-3.5)
        then smax.albumin_score
      when abs(albumin_max-3.5) < abs(albumin_min-3.5)
        then smin.albumin_score
      -- values are equidistant - pick the larger score
      when abs(albumin_max-3.5) = abs(albumin_max-3.5)
      and  smax.albumin_score >= smin.albumin_score
        then smax.albumin_score
      when abs(albumin_max-3.5) = abs(albumin_max-3.5)
      and  smax.albumin_score < smin.albumin_score
        then smin.albumin_score
    end as albumin_score

  , case
      when bilirubin_max is null then null
      else smax.bilirubin_score
    end as bilirubin_score

  , case
      when glucose_max is null then null
      when abs(glucose_max-130) > abs(glucose_min-130)
        then smax.glucose_score
      when abs(glucose_max-130) < abs(glucose_min-130)
        then smin.glucose_score
      -- values are equidistant - pick the larger score
      when abs(glucose_max-130) = abs(glucose_max-130)
      and  smax.glucose_score >= smin.glucose_score
        then smax.glucose_score
      when abs(glucose_max-130) = abs(glucose_max-130)
      and  smax.glucose_score < smin.glucose_score
        then smin.glucose_score
    end as glucose_score


  -- Below are interactions/special cases where only 1 value is important
  , case
      when urineoutput is null then null
      when urineoutput <   400 then 15
      when urineoutput <   600 then 8
      when urineoutput <   900 then 7
      when urineoutput <  1500 then 5
      when urineoutput <  2000 then 4
      when urineoutput <  4000 then 0
      when urineoutput >= 4000 then 1
  end as uo_score

  , case
      when gcs_unable = 1
        -- here they are intubated, so their verbal score is inappropriate
        -- normally you are supposed to use "clinical judgement"
        -- we don't have that, so we just assume normal (as was done in the original study)
        then 0
      when gcs_eyes = 1
        then case
          when gcs_verbal = 1 and gcs_motor in (1,2)
            then 48
          when gcs_verbal = 1 and gcs_motor in (3,4)
            then 33
          when gcs_verbal = 1 and gcs_motor in (5,6)
            then 16
          when gcs_verbal in (2,3) and gcs_motor in (1,2)
            then 29
          when gcs_verbal in (2,3) and gcs_motor in (3,4)
            then 24
          when gcs_verbal in (2,3) and gcs_motor >= 5
            -- highly unlikely clinical combination
            then null
          when gcs_verbal >= 4
            then null
          end
      when gcs_eyes > 1
        then case
          when gcs_verbal = 1 and gcs_motor in (1,2)
            then 29
          when gcs_verbal = 1 and gcs_motor in (3,4)
            then 24
          when gcs_verbal = 1 and gcs_motor in (5,6)
            then 15
          when gcs_verbal in (2,3) and gcs_motor in (1,2)
            then 29
          when gcs_verbal in (2,3) and gcs_motor in (3,4)
            then 24
          when gcs_verbal in (2,3) and gcs_motor = 5
            then 13
          when gcs_verbal in (2,3) and gcs_motor = 6
            then 10
          when gcs_verbal = 4 and gcs_motor in (1,2,3,4)
            then 13
          when gcs_verbal = 4 and gcs_motor = 5
            then 8
          when gcs_verbal = 4 and gcs_motor = 6
            then 3
          when gcs_verbal = 5 and gcs_motor in (1,2,3,4,5)
            then 3
          when gcs_verbal = 5 and gcs_motor = 6
            then 0
          end
      else null
    end as gcs_score

  , case
      when pao2 is null and aado2 is null
        then null
      when pao2 is not null then
        case
          when pao2 < 50 then 15
          when pao2 < 70 then 5
          when pao2 < 80 then 2
        else 0 end
      when aado2 is not null then
        case
          when aado2 <  100 then 0
          when aado2 <  250 then 7
          when aado2 <  350 then 9
          when aado2 <  500 then 11
          when aado2 >= 500 then 14
        else 0 end
      end as pao2_aado2_score

from cohort co
left join score_min smin
  on co.stay_id = smin.stay_id
left join score_max smax
  on co.stay_id = smax.stay_id
)
-- tabulate the APS III using the scores from the worst values
, score as
(
  select s.*
  -- coalesce statements impute normal score of zero if data element is missing
  , coalesce(hr_score,0)
  + coalesce(mbp_score,0)
  + coalesce(temp_score,0)
  + coalesce(resp_rate_score,0)
  + coalesce(pao2_aado2_score,0)
  + coalesce(hematocrit_score,0)
  + coalesce(wbc_score,0)
  + coalesce(creatinine_score,0)
  + coalesce(uo_score,0)
  + coalesce(bun_score,0)
  + coalesce(sodium_score,0)
  + coalesce(albumin_score,0)
  + coalesce(bilirubin_score,0)
  + coalesce(glucose_score,0)
  + coalesce(acidbase_score,0)
  + coalesce(gcs_score,0)
    as apsiii
  from scorecomp s
)
select ie.subject_id, ie.hadm_id, ie.stay_id
, apsiii
-- Calculate probability of hospital mortality using equation from Johnson 2014.
, 1 / (1 + exp(- (-4.4360 + 0.04726*(apsiii) ))) as apsiii_prob
, hr_score
, mbp_score
, temp_score
, resp_rate_score
, pao2_aado2_score
, hematocrit_score
, wbc_score
, creatinine_score
, uo_score
, bun_score
, sodium_score
, albumin_score
, bilirubin_score
, glucose_score
, acidbase_score
, gcs_score
FROM mimic_icu.icustays ie
left join score s
  on ie.stay_id = s.stay_id
;

-- LODS评分
-- lods
-- ------------------------------------------------------------------
-- Title: Logistic Organ Dysfunction Score (LODS)
-- This query extracts the logistic organ dysfunction system.
-- This score is a measure of organ failure in a patient.
-- The score is calculated on the first day of each ICU patients' stay.
-- ------------------------------------------------------------------

-- Reference for LODS:（参考文献）
--  Le Gall, J. R., Klar, J., Lemeshow, S., Saulnier, F., Alberti, C., Artigas, A., & Teres, D.
--  The Logistic Organ Dysfunction system: a new way to assess organ dysfunction in the intensive care unit.
--  JAMA 276.10 (1996): 802-810.

-- Variables used in LODS:（需要用到的变量）
--  GCS
--  VITALS: Heart rate, systolic blood pressure
--  FLAGS: ventilation/cpap
--  IO: urine output
--  LABS: blood urea nitrogen, WBC, bilirubin, creatinine, prothrombin time (PT), platelets
--  ABG: PaO2 with associated FiO2

-- Note:
--  The score is calculated for *all* ICU patients, with the assumption that the user will subselect appropriate stay_ids.
--  For example, the score is calculated for neonates, but it is likely inappropriate to actually use the score values for these patients.

-- extract CPAP from the "Oxygen Delivery Device" fields
create materialized view if not exists lods as 
with cpap as
(
  select ie.stay_id
    , min(DATETIME_SUB(charttime, INTERVAL '1' HOUR)) as starttime
    , max(DATETIME_ADD(charttime, INTERVAL '4' HOUR)) as endtime
    , max(CASE
          WHEN lower(ce.value) LIKE '%cpap%' THEN 1
          WHEN lower(ce.value) LIKE '%bipap mask%' THEN 1
        else 0 end) as cpap
  FROM mimic_icu.icustays ie
  inner join mimic_icu.chartevents ce
    on ie.stay_id = ce.stay_id
    and ce.charttime between ie.intime and DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
  where itemid = 226732
  and (lower(ce.value) LIKE '%cpap%' or lower(ce.value) LIKE '%bipap mask%')
  group by ie.stay_id
)
, pafi1 as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  -- also join to cpap table for the same purpose
  select ie.stay_id, bg.charttime
  , pao2fio2ratio
  , case when vd.stay_id is not null then 1 else 0 end as vent
  , case when cp.stay_id is not null then 1 else 0 end as cpap
  from bg bg
  INNER JOIN mimic_icu.icustays ie
    ON bg.hadm_id = ie.hadm_id
    AND bg.charttime >= ie.intime AND bg.charttime < ie.outtime
  left join ventilation vd
    on ie.stay_id = vd.stay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
    and vd.ventilation_status = 'InvasiveVent'
  left join cpap cp
    on ie.stay_id = cp.stay_id
    and bg.charttime >= cp.starttime
    and bg.charttime <= cp.endtime
)
, pafi2 as
(
  -- get the minimum PaO2/FiO2 ratio *only for ventilated/cpap patients*
  select stay_id
  , min(pao2fio2ratio) as pao2fio2_vent_min
  from pafi1
  where vent = 1 or cpap = 1
  group by stay_id
)
, cohort as
(
select  ie.subject_id
      , ie.hadm_id
      , ie.stay_id
      , ie.intime
      , ie.outtime

      , gcs.gcs_min
      , vital.heart_rate_max
      , vital.heart_rate_min
      , vital.sbp_max
      , vital.sbp_min

      -- this value is non-null iff the patient is on vent/cpap
      , pf.pao2fio2_vent_min

      , labs.bun_max
      , labs.bun_min
      , labs.wbc_max
      , labs.wbc_min
      , labs.bilirubin_total_max AS bilirubin_max
      , labs.creatinine_max
      , labs.pt_min
      , labs.pt_max
      , labs.platelets_min AS platelet_min

      , uo.urineoutput

FROM mimic_icu.icustays ie
inner join mimic_hosp.admissions adm
  on ie.hadm_id = adm.hadm_id
inner join mimic_hosp.patients pat
  on ie.subject_id = pat.subject_id

-- join to above view to get pao2/fio2 ratio
left join pafi2 pf
  on ie.stay_id = pf.stay_id

-- join to custom tables to get more data....
left join first_day_gcs gcs
  on ie.stay_id = gcs.stay_id
left join first_day_vitalsign vital
  on ie.stay_id = vital.stay_id
left join first_day_urine_output uo
  on ie.stay_id = uo.stay_id
left join first_day_lab labs
  on ie.stay_id = labs.stay_id
)
, scorecomp as
(
select
  cohort.*
  -- Below code calculates the component scores needed for SAPS

  -- neurologic
  , case
    when gcs_min is null then null
      when gcs_min <  3 then null -- erroneous value/on trach
      when gcs_min <=  5 then 5
      when gcs_min <=  8 then 3
      when gcs_min <= 13 then 1
    else 0
  end as neurologic

  -- cardiovascular
  , case
      when heart_rate_max is null
      and sbp_min is null then null
      when heart_rate_min < 30 then 5
      when sbp_min < 40 then 5
      when sbp_min <  70 then 3
      when sbp_max >= 270 then 3
      when heart_rate_max >= 140 then 1
      when sbp_max >= 240 then 1
      when sbp_min < 90 then 1
    else 0
  end as cardiovascular

  -- renal
  , case
      when bun_max is null
        or urineoutput is null
        or creatinine_max is null
        then null
      when urineoutput <   500.0 then 5
      when bun_max >= 56.0 then 5
      when creatinine_max >= 1.60 then 3
      when urineoutput <   750.0 then 3
      when bun_max >= 28.0 then 3
      when urineoutput >= 10000.0 then 3
      when creatinine_max >= 1.20 then 1
      when bun_max >= 17.0 then 1
      when bun_max >= 7.50 then 1
    else 0
  end as renal

  -- pulmonary
  , case
      when pao2fio2_vent_min is null then 0
      when pao2fio2_vent_min >= 150 then 1
      when pao2fio2_vent_min < 150 then 3
    else null
  end as pulmonary

  -- hematologic
  , case
      when wbc_max is null
        and platelet_min is null
          then null
      when wbc_min <   1.0 then 3
      when wbc_min <   2.5 then 1
      when platelet_min < 50.0 then 1
      when wbc_max >= 50.0 then 1
    else 0
  end as hematologic

  
  -- We have defined the "standard" PT as 12 seconds.
  -- This is an assumption and subsequent analyses may be affected by this assumption.
  , case
      when pt_max is null
        and bilirubin_max is null
          then null
      when bilirubin_max >= 2.0 then 1
      when pt_max > (12+3) then 1
      when pt_min < (12*0.25) then 1
    else 0
  end as hepatic

from cohort
)
select ie.subject_id, ie.hadm_id, ie.stay_id
-- coalesce statements impute normal score of zero if data element is missing
, coalesce(neurologic,0)
+ coalesce(cardiovascular,0)
+ coalesce(renal,0)
+ coalesce(pulmonary,0)
+ coalesce(hematologic,0)
+ coalesce(hepatic,0)
  as LODS
, neurologic
, cardiovascular
, renal
, pulmonary
, hematologic
, hepatic
FROM mimic_icu.icustays ie
left join scorecomp s
  on ie.stay_id = s.stay_id
;

-- OASIS评分
-- oasis
-- ------------------------------------------------------------------
-- Title: Oxford Acute Severity of Illness Score (oasis)
-- This query extracts the Oxford acute severity of illness score.
-- This score is a measure of severity of illness for patients in the ICU.
-- The score is calculated on the first day of each ICU patients' stay.
-- ------------------------------------------------------------------

-- Reference for OASIS:（参考文献）
--    Johnson, Alistair EW, Andrew A. Kramer, and Gari D. Clifford.
--    "A new severity of illness scale using a subset of acute physiology and chronic health evaluation data elements shows comparable predictive accuracy*."
--    Critical care medicine 41, no. 7 (2013): 1711-1718.

-- Variables used in OASIS:（需要用到的变量）
--  Heart rate, GCS, MAP, Temperature, Respiratory rate, Ventilation status (sourced FROM mimic_icu.chartevents)
--  Urine output (sourced from OUTPUTEVENTS)
--  Elective surgery (sourced FROM mimic_hosp.admissions and SERVICES)
--  Pre-ICU in-hospital length of stay (sourced FROM mimic_hosp.admissions and ICUSTAYS)
--  Age (sourced FROM mimic_hosp.patients)

-- Regarding missing values:
--  The ventilation flag is always 0/1. It cannot be missing, since VENT=0 if no data is found for vent settings.

-- Note:
--  The score is calculated for *all* ICU patients, with the assumption that the user will subselect appropriate stay_ids.
--  For example, the score is calculated for neonates, but it is likely inappropriate to actually use the score values for these patients.

create materialized view if not exists oasis as 
with surgflag as
(
  select ie.stay_id
    , max(case
        when lower(curr_service) like '%surg%' then 1
        when curr_service = 'ORTHO' then 1
    else 0 end) as surgical
  FROM mimic_icu.icustays ie
  left join mimic_hosp.services se
    on ie.hadm_id = se.hadm_id
    and se.transfertime < DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
  group by ie.stay_id
)
-- first day ventilation
, vent AS
(
    SELECT ie.stay_id
    , MAX(
        CASE WHEN v.stay_id IS NOT NULL THEN 1 ELSE 0 END
    ) AS vent
    FROM mimic_icu.icustays ie
    LEFT JOIN ventilation v
        ON ie.stay_id = v.stay_id
        AND (
            v.starttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
        OR v.endtime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
        OR v.starttime <= ie.intime AND v.endtime >= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
        )
        AND v.ventilation_status = 'InvasiveVent'
    GROUP BY ie.stay_id
)
, cohort as
(
select ie.subject_id, ie.hadm_id, ie.stay_id
      , ie.intime
      , ie.outtime
      , adm.deathtime
      , DATETIME_DIFF(ie.intime, adm.admittime, 'MINUTE') as preiculos
      , ag.age
      , gcs.gcs_min
      , vital.heart_rate_max
      , vital.heart_rate_min
      , vital.mbp_max
      , vital.mbp_min
      , vital.resp_rate_max
      , vital.resp_rate_min
      , vital.temperature_max
      , vital.temperature_min
      , vent.vent as mechvent
      , uo.urineoutput

      , case
          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
            then 1
          when adm.ADMISSION_TYPE is null or sf.surgical is null
            then null
          else 0
        end as electivesurgery

      -- mortality flags
      , case
          when adm.deathtime between ie.intime and ie.outtime
            then 1
          when adm.deathtime <= ie.intime -- sometimes there are typographical errors in the death date
            then 1
          when adm.dischtime <= ie.outtime and adm.discharge_location = 'DEAD/EXPIRED'
            then 1
          else 0 end
        as icustay_expire_flag
      , adm.hospital_expire_flag
FROM mimic_icu.icustays ie
inner join mimic_hosp.admissions adm
  on ie.hadm_id = adm.hadm_id
inner join mimic_hosp.patients pat
  on ie.subject_id = pat.subject_id
LEFT JOIN age ag
  ON ie.hadm_id = ag.hadm_id
left join surgflag sf
  on ie.stay_id = sf.stay_id
-- join to custom tables to get more data....
left join first_day_gcs gcs
  on ie.stay_id = gcs.stay_id
left join first_day_vitalsign vital
  on ie.stay_id = vital.stay_id
left join first_day_urine_output uo
  on ie.stay_id = uo.stay_id
left join vent
  on ie.stay_id = vent.stay_id
)
, scorecomp as
(
select co.subject_id, co.hadm_id, co.stay_id
, co.icustay_expire_flag
, co.hospital_expire_flag

-- Below code calculates the component scores needed for oasis
, case when preiculos is null then null
     when preiculos < 10.2 then 5
     when preiculos < 297 then 3
     when preiculos < 1440 then 0
     when preiculos < 18708 then 1
     else 2 end as preiculos_score
,  case when age is null then null
      when age < 24 then 0
      when age <= 53 then 3
      when age <= 77 then 6
      when age <= 89 then 9
      when age >= 90 then 7
      else 0 end as age_score
,  case when gcs_min is null then null
      when gcs_min <= 7 then 10
      when gcs_min < 14 then 4
      when gcs_min = 14 then 3
      else 0 end as gcs_score
,  case when heart_rate_max is null then null
      when heart_rate_max > 125 then 6
      when heart_rate_min < 33 then 4
      when heart_rate_max >= 107 and heart_rate_max <= 125 then 3
      when heart_rate_max >= 89 and heart_rate_max <= 106 then 1
      else 0 end as heart_rate_score
,  case when mbp_min is null then null
      when mbp_min < 20.65 then 4
      when mbp_min < 51 then 3
      when mbp_max > 143.44 then 3
      when mbp_min >= 51 and mbp_min < 61.33 then 2
      else 0 end as mbp_score
,  case when resp_rate_min is null then null
      when resp_rate_min <   6 then 10
      when resp_rate_max >  44 then  9
      when resp_rate_max >  30 then  6
      when resp_rate_max >  22 then  1
      when resp_rate_min <  13 then 1 else 0
      end as resp_rate_score
,  case when temperature_max is null then null
      when temperature_max > 39.88 then 6
      when temperature_min >= 33.22 and temperature_min <= 35.93 then 4
      when temperature_max >= 33.22 and temperature_max <= 35.93 then 4
      when temperature_min < 33.22 then 3
      when temperature_min > 35.93 and temperature_min <= 36.39 then 2
      when temperature_max >= 36.89 and temperature_max <= 39.88 then 2
      else 0 end as temp_score
,  case when UrineOutput is null then null
      when UrineOutput < 671.09 then 10
      when UrineOutput > 6896.80 then 8
      when UrineOutput >= 671.09
       and UrineOutput <= 1426.99 then 5
      when UrineOutput >= 1427.00
       and UrineOutput <= 2544.14 then 1
      else 0 end as urineoutput_score
,  case when mechvent is null then null
      when mechvent = 1 then 9
      else 0 end as mechvent_score
,  case when electivesurgery is null then null
      when electivesurgery = 1 then 0
      else 6 end as electivesurgery_score


-- The below code gives the component associated with each score
-- This is not needed to calculate oasis, but provided for user convenience.
-- If both the min/max are in the normal range (score of 0), then the average value is stored.
, preiculos
, age
, gcs_min as gcs
,  case when heart_rate_max is null then null
      when heart_rate_max > 125 then heart_rate_max
      when heart_rate_min < 33 then heart_rate_min
      when heart_rate_max >= 107 and heart_rate_max <= 125 then heart_rate_max
      when heart_rate_max >= 89 and heart_rate_max <= 106 then heart_rate_max
      else (heart_rate_min+heart_rate_max)/2 end as heartrate
,  case when mbp_min is null then null
      when mbp_min < 20.65 then mbp_min
      when mbp_min < 51 then mbp_min
      when mbp_max > 143.44 then mbp_max
      when mbp_min >= 51 and mbp_min < 61.33 then mbp_min
      else (mbp_min+mbp_max)/2 end as meanbp
,  case when resp_rate_min is null then null
      when resp_rate_min <   6 then resp_rate_min
      when resp_rate_max >  44 then resp_rate_max
      when resp_rate_max >  30 then resp_rate_max
      when resp_rate_max >  22 then resp_rate_max
      when resp_rate_min <  13 then resp_rate_min
      else (resp_rate_min+resp_rate_max)/2 end as resprate
,  case when temperature_max is null then null
      when temperature_max > 39.88 then temperature_max
      when temperature_min >= 33.22 and temperature_min <= 35.93 then temperature_min
      when temperature_max >= 33.22 and temperature_max <= 35.93 then temperature_max
      when temperature_min < 33.22 then temperature_min
      when temperature_min > 35.93 and temperature_min <= 36.39 then temperature_min
      when temperature_max >= 36.89 and temperature_max <= 39.88 then temperature_max
      else (temperature_min+temperature_max)/2 end as temp
,  UrineOutput
,  mechvent
,  electivesurgery
from cohort co
)
, score as
(
select s.*
    , coalesce(age_score,0)
    + coalesce(preiculos_score,0)
    + coalesce(gcs_score,0)
    + coalesce(heart_rate_score,0)
    + coalesce(mbp_score,0)
    + coalesce(resp_rate_score,0)
    + coalesce(temp_score,0)
    + coalesce(urineoutput_score,0)
    + coalesce(mechvent_score,0)
    + coalesce(electivesurgery_score,0)
    as oasis
from scorecomp s
)
select
  subject_id, hadm_id, stay_id
  , oasis
  -- Calculate the probability of in-hospital mortality
  , 1 / (1 + exp(- (-6.1746 + 0.1275*(oasis) ))) as oasis_prob
  , age, age_score
  , preiculos, preiculos_score
  , gcs, gcs_score
  , heartrate, heart_rate_score
  , meanbp, mbp_score
  , resprate, resp_rate_score
  , temp, temp_score
  , urineoutput, urineoutput_score
  , mechvent, mechvent_score
  , electivesurgery, electivesurgery_score
from score
;

-- SAPS II评分
-- sapsii
-- ------------------------------------------------------------------
-- Title: Simplified Acute Physiology Score II (SAPS II)
-- This query extracts the simplified acute physiology score II.
-- This score is a measure of patient severity of illness.
-- The score is calculated on the first day of each ICU patients' stay.
-- ------------------------------------------------------------------

-- Reference for SAPS II:（参考文献）
--    Le Gall, Jean-Roger, Stanley Lemeshow, and Fabienne Saulnier.
--    "A new simplified acute physiology score (SAPS II) based on a European/North American multicenter study."
--    JAMA 270, no. 24 (1993): 2957-2963.

-- Variables used in SAPS II:（需要用到的变量）
--  Age, GCS
--  VITALS: Heart rate, systolic blood pressure, temperature
--  FLAGS: ventilation/cpap
--  IO: urine output
--  LABS: PaO2/FiO2 ratio, blood urea nitrogen, WBC, potassium, sodium, HCO3
create materialized view if not exists sapsii as 
with co as
(
    select 
        subject_id
        , hadm_id
        , stay_id
        , intime AS starttime
        , DATETIME_ADD(intime, INTERVAL '24' HOUR) AS endtime
    from mimic_icu.icustays ie
)
, cpap as
(
  select 
    co.subject_id
    , co.stay_id
    , GREATEST(min(DATETIME_SUB(charttime, INTERVAL '1' HOUR)), co.starttime) as starttime
    , LEAST(max(DATETIME_ADD(charttime, INTERVAL '4' HOUR)), co.endtime) as endtime
    , max(case when REGEXP_CONTAINS(lower(ce.value), '(cpap mask|bipap)') then 1 else 0 end) as cpap
  from co
  inner join mimic_icu.chartevents ce
    on co.stay_id = ce.stay_id
    and ce.charttime > co.starttime
    and ce.charttime <= co.endtime
  where ce.itemid = 226732
  and REGEXP_CONTAINS(lower(ce.value), '(cpap mask|bipap)')
  group by co.subject_id, co.stay_id, co.starttime,co.endtime
)

-- extract a flag for surgical service
-- this combined with "elective" from admissions table defines elective/non-elective surgery
, surgflag as
(
  select adm.hadm_id
    , case when lower(curr_service) like '%surg%' then 1 else 0 end as surgical
    , ROW_NUMBER() over
    (
      PARTITION BY adm.HADM_ID
      ORDER BY TRANSFERTIME
    ) as serviceOrder
  from mimic_hosp.admissions adm
  left join mimic_hosp.services se
    on adm.hadm_id = se.hadm_id
)
-- icd-9 diagnostic codes are our best source for comorbidity information
-- unfortunately, they are technically a-causal
-- however, this shouldn't matter too much for the SAPS II comorbidities
, comorb as
(
select hadm_id
-- these are slightly different than elixhauser comorbidities, but based on them
-- they include some non-comorbid ICD-9 codes (e.g. 20302, relapse of multiple myeloma)
  , MAX(CASE
    WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 3) BETWEEN '042' AND '044'
      THEN 1
    WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'B20' AND 'B22' THEN 1
    WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) = 'B24' THEN 1
  ELSE 0 END) AS aids  /* HIV and AIDS */
  , MAX(
    CASE WHEN icd_version = 9 THEN
      CASE
        WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20000' AND '20238' THEN 1 -- lymphoma
        WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20240' AND '20248' THEN 1 -- leukemia
        WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20250' AND '20302' THEN 1 -- lymphoma
        WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20310' AND '20312' THEN 1 -- leukemia
        WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20302' AND '20382' THEN 1 -- lymphoma
        WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20400' AND '20522' THEN 1 -- chronic leukemia
        WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20580' AND '20702' THEN 1 -- other myeloid leukemia
        WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20720' AND '20892' THEN 1 -- other myeloid leukemia
        WHEN SUBSTR(icd_code, 1, 4) IN ('2386', '2733') then 1 -- lymphoma
      ELSE 0 END
    WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'C81' AND 'C96' THEN 1
  ELSE 0 END) as hem
  , MAX(CASE
    WHEN icd_version = 9 THEN
      CASE
      WHEN SUBSTR(icd_code, 1, 4) BETWEEN '1960' AND '1991' THEN 1
      WHEN SUBSTR(icd_code, 1, 5) BETWEEN '20970' AND '20975' THEN 1
      WHEN SUBSTR(icd_code, 1, 5) IN ('20979', '78951') THEN 1
      ELSE 0 END
    WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'C77' AND 'C79' THEN 1
    WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 4) = 'C800' THEN 1
    ELSE 0 END) as mets      /* Metastatic cancer */
    from mimic_hosp.diagnoses_icd
  group by hadm_id
)

, pafi1 as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  -- also join to cpap table for the same purpose
  select 
    co.stay_id
  , bg.charttime
  , pao2fio2ratio AS PaO2FiO2
  , case when vd.stay_id is not null then 1 else 0 end as vent
  , case when cp.subject_id is not null then 1 else 0 end as cpap
  from co
  LEFT JOIN bg bg
    ON co.subject_id = bg.subject_id
    AND bg.specimen_pred = 'ART.'
    AND bg.charttime > co.starttime
    AND bg.charttime <= co.endtime
  left join ventilation vd
    on co.stay_id = vd.stay_id
    and bg.charttime > vd.starttime
    and bg.charttime <= vd.endtime
    and vd.ventilation_status = 'InvasiveVent'
  left join cpap cp
    on bg.subject_id = cp.subject_id
    and bg.charttime > cp.starttime
    and bg.charttime <= cp.endtime
)
, pafi2 as
(
  -- get the minimum PaO2/FiO2 ratio *only for ventilated/cpap patients*
  select stay_id
  , min(PaO2FiO2) as PaO2FiO2_vent_min
  from pafi1
  where vent = 1 or cpap = 1
  group by stay_id
)

, gcs AS
(
    select co.stay_id
    , MIN(gcs.gcs) AS mingcs
    FROM co
    left join gcs gcs
    ON co.stay_id = gcs.stay_id
    AND co.starttime < gcs.charttime
    AND gcs.charttime <= co.endtime
    GROUP BY co.stay_id
)

, vital AS 
(
    SELECT 
        co.stay_id
      , MIN(vital.heart_rate) AS heartrate_min
      , MAX(vital.heart_rate) AS heartrate_max
      , MIN(vital.sbp) AS sysbp_min
      , MAX(vital.sbp) AS sysbp_max
      , MIN(vital.temperature) AS tempc_min
      , MAX(vital.temperature) AS tempc_max
    FROM co
    left join vitalsign vital
      on co.subject_id = vital.subject_id
      AND co.starttime < vital.charttime
      AND co.endtime >= vital.charttime
    GROUP BY co.stay_id
)
, uo AS
(
    SELECT 
        co.stay_id
      , SUM(uo.urineoutput) as urineoutput
    FROM co
    left join urine_output uo
      on co.stay_id = uo.stay_id
      AND co.starttime < uo.charttime
      AND co.endtime >= uo.charttime
    GROUP BY co.stay_id
)
, labs AS
(
    SELECT 
        co.stay_id
      , MIN(labs.bun) AS bun_min
      , MAX(labs.bun) AS bun_max
      , MIN(labs.potassium) AS potassium_min
      , MAX(labs.potassium) AS potassium_max
      , MIN(labs.sodium) AS sodium_min
      , MAX(labs.sodium) AS sodium_max
      , MIN(labs.bicarbonate) AS bicarbonate_min
      , MAX(labs.bicarbonate) AS bicarbonate_max               
    FROM co
    left join chemistry labs
      on co.subject_id = labs.subject_id
      AND co.starttime < labs.charttime
      AND co.endtime >= labs.charttime
    group by co.stay_id
)
, cbc AS
(
    SELECT 
        co.stay_id
      , MIN(cbc.wbc) AS wbc_min
      , MAX(cbc.wbc) AS wbc_max  
    FROM co
    LEFT JOIN complete_blood_count cbc
      ON co.subject_id = cbc.subject_id
      AND co.starttime < cbc.charttime
      AND co.endtime >= cbc.charttime
    GROUP BY co.stay_id
)
, enz AS
(
    SELECT 
        co.stay_id
      , MIN(enz.bilirubin_total) AS bilirubin_min
      , MAX(enz.bilirubin_total) AS bilirubin_max  
    FROM co
    LEFT JOIN enzyme enz
      ON co.subject_id = enz.subject_id
      AND co.starttime < enz.charttime
      AND co.endtime >= enz.charttime
    GROUP BY co.stay_id
)

, cohort as
(
select 
    ie.subject_id, ie.hadm_id, ie.stay_id
      , ie.intime
      , ie.outtime
      , va.age
      , co.starttime
      , co.endtime
    
      , vital.heartrate_max
      , vital.heartrate_min
      , vital.sysbp_max
      , vital.sysbp_min
      , vital.tempc_max
      , vital.tempc_min

      -- this value is non-null iff the patient is on vent/cpap
      , pf.PaO2FiO2_vent_min

      , uo.urineoutput

      , labs.bun_min
      , labs.bun_max
      , cbc.wbc_min
      , cbc.wbc_max
      , labs.potassium_min
      , labs.potassium_max
      , labs.sodium_min
      , labs.sodium_max
      , labs.bicarbonate_min
      , labs.bicarbonate_max
    
      , enz.bilirubin_min
      , enz.bilirubin_max

      , gcs.mingcs

      , comorb.AIDS
      , comorb.HEM
      , comorb.METS

      , case
          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
            then 'ScheduledSurgical'
          when adm.ADMISSION_TYPE != 'ELECTIVE' and sf.surgical = 1
            then 'UnscheduledSurgical'
          else 'Medical'
        end as AdmissionType


from mimic_icu.icustays ie
inner join mimic_hosp.admissions adm
  on ie.hadm_id = adm.hadm_id
LEFT JOIN age va
  on ie.hadm_id = va.hadm_id
inner join co
  on ie.stay_id = co.stay_id
    
-- join to above views
left join pafi2 pf
  on ie.stay_id = pf.stay_id
left join surgflag sf
  on adm.hadm_id = sf.hadm_id and sf.serviceOrder = 1
left join comorb
  on ie.hadm_id = comorb.hadm_id

-- join to custom tables to get more data....
left join gcs gcs
  on ie.stay_id = gcs.stay_id
left join vital
  on ie.stay_id = vital.stay_id
left join uo
  on ie.stay_id = uo.stay_id
left join labs
  on ie.stay_id = labs.stay_id
left join cbc
  on ie.stay_id = cbc.stay_id
left join enz
  on ie.stay_id = enz.stay_id
)
, scorecomp as
(
select
  cohort.*
  -- Below code calculates the component scores needed for SAPS
  , case
      when age is null then null
      when age <  40 then 0
      when age <  60 then 7
      when age <  70 then 12
      when age <  75 then 15
      when age <  80 then 16
      when age >= 80 then 18
    end as age_score

  , case
      when heartrate_max is null then null
      when heartrate_min <   40 then 11
      when heartrate_max >= 160 then 7
      when heartrate_max >= 120 then 4
      when heartrate_min  <  70 then 2
      when  heartrate_max >= 70 and heartrate_max < 120
        and heartrate_min >= 70 and heartrate_min < 120
      then 0
    end as hr_score

  , case
      when  sysbp_min is null then null
      when  sysbp_min <   70 then 13
      when  sysbp_min <  100 then 5
      when  sysbp_max >= 200 then 2
      when  sysbp_max >= 100 and sysbp_max < 200
        and sysbp_min >= 100 and sysbp_min < 200
        then 0
    end as sysbp_score

  , case
      when tempc_max is null then null
      when tempc_max >= 39.0 then 3
      when tempc_min <  39.0 then 0
    end as temp_score

  , case
      when PaO2FiO2_vent_min is null then null
      when PaO2FiO2_vent_min <  100 then 11
      when PaO2FiO2_vent_min <  200 then 9
      when PaO2FiO2_vent_min >= 200 then 6
    end as PaO2FiO2_score

  , case
      when UrineOutput is null then null
      when UrineOutput <   500.0 then 11
      when UrineOutput <  1000.0 then 4
      when UrineOutput >= 1000.0 then 0
    end as uo_score

  , case
      when bun_max is null then null
      when bun_max <  28.0 then 0
      when bun_max <  84.0 then 6
      when bun_max >= 84.0 then 10
    end as bun_score

  , case
      when wbc_max is null then null
      when wbc_min <   1.0 then 12
      when wbc_max >= 20.0 then 3
      when wbc_max >=  1.0 and wbc_max < 20.0
       and wbc_min >=  1.0 and wbc_min < 20.0
        then 0
    end as wbc_score

  , case
      when potassium_max is null then null
      when potassium_min <  3.0 then 3
      when potassium_max >= 5.0 then 3
      when potassium_max >= 3.0 and potassium_max < 5.0
       and potassium_min >= 3.0 and potassium_min < 5.0
        then 0
      end as potassium_score

  , case
      when sodium_max is null then null
      when sodium_min  < 125 then 5
      when sodium_max >= 145 then 1
      when sodium_max >= 125 and sodium_max < 145
       and sodium_min >= 125 and sodium_min < 145
        then 0
      end as sodium_score

  , case
      when bicarbonate_max is null then null
      when bicarbonate_min <  15.0 then 5
      when bicarbonate_min <  20.0 then 3
      when bicarbonate_max >= 20.0
       and bicarbonate_min >= 20.0
          then 0
      end as bicarbonate_score

  , case
      when bilirubin_max is null then null
      when bilirubin_max  < 4.0 then 0
      when bilirubin_max  < 6.0 then 4
      when bilirubin_max >= 6.0 then 9
      end as bilirubin_score

   , case
      when mingcs is null then null
        when mingcs <  3 then null -- erroneous value/on trach
        when mingcs <  6 then 26
        when mingcs <  9 then 13
        when mingcs < 11 then 7
        when mingcs < 14 then 5
        when mingcs >= 14
         and mingcs <= 15
          then 0
        end as gcs_score

    , case
        when AIDS = 1 then 17
        when HEM  = 1 then 10
        when METS = 1 then 9
        else 0
      end as comorbidity_score

    , case
        when AdmissionType = 'ScheduledSurgical' then 0
        when AdmissionType = 'Medical' then 6
        when AdmissionType = 'UnscheduledSurgical' then 8
        else null
      end as admissiontype_score

from cohort
)
-- Calculate SAPS II here so we can use it in the probability calculation below
, score as
(
  select s.*
  -- coalesce statements impute normal score of zero if data element is missing
  , coalesce(age_score,0)
  + coalesce(hr_score,0)
  + coalesce(sysbp_score,0)
  + coalesce(temp_score,0)
  + coalesce(PaO2FiO2_score,0)
  + coalesce(uo_score,0)
  + coalesce(bun_score,0)
  + coalesce(wbc_score,0)
  + coalesce(potassium_score,0)
  + coalesce(sodium_score,0)
  + coalesce(bicarbonate_score,0)
  + coalesce(bilirubin_score,0)
  + coalesce(gcs_score,0)
  + coalesce(comorbidity_score,0)
  + coalesce(admissiontype_score,0)
    as SAPSII
  from scorecomp s
)
select s.subject_id, s.hadm_id, s.stay_id
, s.starttime
, s.endtime
, sapsii
, 1 / (1 + exp(- (-7.7631 + 0.0737*(SAPSII) + 0.9971*(ln(SAPSII + 1))) )) as sapsii_prob
, age_score
, hr_score
, sysbp_score
, temp_score
, PaO2FiO2_score
, uo_score
, bun_score
, wbc_score
, potassium_score
, sodium_score
, bicarbonate_score
, bilirubin_score
, gcs_score
, comorbidity_score
, admissiontype_score
from score s
;

-- SIRS评分
-- sirs
-- ------------------------------------------------------------------
-- Title: Systemic inflammatory response syndrome (SIRS) criteria
-- This query extracts the Systemic inflammatory response syndrome (SIRS) criteria
-- The criteria quantify the level of inflammatory response of the body
-- The score is calculated on the first day of each ICU patients' stay.
-- ------------------------------------------------------------------

-- Reference for SIRS:（参考文献）
--    American College of Chest Physicians/Society of Critical Care Medicine Consensus Conference:
--    definitions for sepsis and organ failure and guidelines for the use of innovative therapies in sepsis"
--    Crit. Care Med. 20 (6): 864–74. 1992.
--    doi:10.1097/00003246-199206000-00025. PMID 1597042.

-- Variables used in SIRS:（需要用到的变量）
--  Body temperature (min and max)
--  Heart rate (max)
--  Respiratory rate (max)
--  PaCO2 (min)
--  White blood cell count (min and max)
--  the presence of greater than 10% immature neutrophils (band forms)

-- Note:
--  The score is calculated for *all* ICU patients, with the assumption that the user will subselect appropriate stay_ids.
--  For example, the score is calculated for neonates, but it is likely inappropriate to actually use the score values for these patients.

-- Aggregate the components for the score
create materialized view if not exists sirs as 
with scorecomp as
(
select ie.stay_id
  , v.temperature_min
  , v.temperature_max
  , v.heart_rate_max
  , v.resp_rate_max
  , bg.pco2_min AS paco2_min
  , l.wbc_min
  , l.wbc_max
  , l.bands_max
FROM mimic_icu.icustays ie
left join first_day_bg_art bg
 on ie.stay_id = bg.stay_id
left join first_day_vitalsign v
  on ie.stay_id = v.stay_id
left join first_day_lab l
  on ie.stay_id = l.stay_id
)
, scorecalc as
(
  -- Calculate the final score
  -- note that if the underlying data is missing, the component is null
  -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
  select stay_id

  , case
      when temperature_min < 36.0 then 1
      when temperature_max > 38.0 then 1
      when temperature_min is null then null
      else 0
    end as temp_score


  , case
      when heart_rate_max > 90.0  then 1
      when heart_rate_max is null then null
      else 0
    end as heart_rate_score

  , case
      when resp_rate_max > 20.0  then 1
      when paco2_min < 32.0  then 1
      when coalesce(resp_rate_max, paco2_min) is null then null
      else 0
    end as resp_score

  , case
      when wbc_min <  4.0  then 1
      when wbc_max > 12.0  then 1
      when bands_max > 10 then 1-- > 10% immature neurophils (band forms)
      when coalesce(wbc_min, bands_max) is null then null
      else 0
    end as wbc_score

  from scorecomp
)
select
  ie.subject_id, ie.hadm_id, ie.stay_id
  -- Combine all the scores to get SOFA
  -- Impute 0 if the score is missing
  , coalesce(temp_score,0)
  + coalesce(heart_rate_score,0)
  + coalesce(resp_score,0)
  + coalesce(wbc_score,0)
    as sirs
  , temp_score, heart_rate_score, resp_score, wbc_score
FROM mimic_icu.icustays ie
left join scorecalc s
  on ie.stay_id = s.stay_id
;

-- SOFA评分
-- sofa
-- ------------------------------------------------------------------
-- Title: Sequential Organ Failure Assessment (SOFA)
-- This query extracts the sequential organ failure assessment (formally: sepsis-related organ failure assessment).
-- This score is a measure of organ failure for patients in the ICU.
-- The score is calculated for **every hour** of the patient's ICU stay.
-- However, as the calculation window is 24 hours, care should be taken when
-- using the score before the end of the first day, as the data window is limited.
-- ------------------------------------------------------------------

-- Reference for SOFA:（参考文献）
--    Jean-Louis Vincent, Rui Moreno, Jukka Takala, Sheila Willatts, Arnaldo De Mendonça,
--    Hajo Bruining, C. K. Reinhart, Peter M Suter, and L. G. Thijs.
--    "The SOFA (Sepsis-related Organ Failure Assessment) score to describe organ dysfunction/failure."
--    Intensive care medicine 22, no. 7 (1996): 707-710.

-- Variables used in SOFA:（需要用到的变量）
--  GCS, MAP, FiO2, Ventilation status (sourced FROM mimic_icu.chartevents)
--  Creatinine, Bilirubin, FiO2, PaO2, Platelets (sourced FROM mimic_icu.labevents)
--  Dopamine, Dobutamine, Epinephrine, Norepinephrine (sourced FROM mimic_icu.inputevents_mv and INPUTEVENTS_CV)
--  Urine output (sourced from OUTPUTEVENTS)

-- generate a row for every hour the patient was in the ICU
-- here, we generate a starttime/endtime for every hour of the patient's ICU stay
-- all of our joins to data will use these times to extract data pertinent to only that hour
create materialized view if not exists sofa as 
WITH co AS
(
  select ih.stay_id, ie.hadm_id
  , hr
  -- start/endtime can be used to filter to values within this hour
  , DATETIME_SUB(ih.endtime, INTERVAL '1' HOUR) AS starttime
  , ih.endtime
  from icustay_hourly ih
  INNER JOIN mimic_icu.icustays ie
    ON ih.stay_id = ie.stay_id
)
, pafi as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  select ie.stay_id
  , bg.charttime
  -- because pafi has an interaction between vent/PaO2:FiO2, we need two columns for the score
  -- it can happen that the lowest unventilated PaO2/FiO2 is 68, but the lowest ventilated PaO2/FiO2 is 120
  -- in this case, the SOFA score is 3, *not* 4.
  , case when vd.stay_id is null then pao2fio2ratio else null end pao2fio2ratio_novent
  , case when vd.stay_id is not null then pao2fio2ratio else null end pao2fio2ratio_vent
  FROM mimic_icu.icustays ie
  inner join bg bg
    on ie.subject_id = bg.subject_id
  left join ventilation vd
    on ie.stay_id = vd.stay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
    and vd.ventilation_status = 'InvasiveVent'
  WHERE specimen_pred = 'ART.'
)
, vs AS
(
    
  select co.stay_id, co.hr
  -- vitals
  , min(vs.mbp) as meanbp_min
  from co
  left join vitalsign vs
    on co.stay_id = vs.stay_id
    and co.starttime < vs.charttime
    and co.endtime >= vs.charttime
  group by co.stay_id, co.hr
)
, gcs AS
(
  select co.stay_id, co.hr
  -- gcs
  , min(gcs.gcs) as gcs_min
  from co
  left join gcs gcs
    on co.stay_id = gcs.stay_id
    and co.starttime < gcs.charttime
    and co.endtime >= gcs.charttime
  group by co.stay_id, co.hr
)
, bili AS
(
  select co.stay_id, co.hr
  , max(enz.bilirubin_total) as bilirubin_max
  from co
  left join enzyme enz
    on co.hadm_id = enz.hadm_id
    and co.starttime < enz.charttime
    and co.endtime >= enz.charttime
  group by co.stay_id, co.hr
)
, cr AS
(
  select co.stay_id, co.hr
  , max(chem.creatinine) as creatinine_max
  from co
  left join chemistry chem
    on co.hadm_id = chem.hadm_id
    and co.starttime < chem.charttime
    and co.endtime >= chem.charttime
  group by co.stay_id, co.hr
)
, plt AS
(
  select co.stay_id, co.hr
  , min(cbc.platelet) as platelet_min
  from co
  left join complete_blood_count cbc
    on co.hadm_id = cbc.hadm_id
    and co.starttime < cbc.charttime
    and co.endtime >= cbc.charttime
  group by co.stay_id, co.hr
)
, pf AS
(
  select co.stay_id, co.hr
  , min(pafi.pao2fio2ratio_novent) AS pao2fio2ratio_novent
  , min(pafi.pao2fio2ratio_vent) AS pao2fio2ratio_vent
  from co
  -- bring in blood gases that occurred during this hour
  left join pafi
    on co.stay_id = pafi.stay_id
    and co.starttime < pafi.charttime
    and co.endtime  >= pafi.charttime
  group by co.stay_id, co.hr
)
-- sum uo separately to prevent duplicating values
, uo as
(
  select co.stay_id, co.hr
  -- uo
  , MAX(
      CASE WHEN uo.uo_tm_24hr >= 22 AND uo.uo_tm_24hr <= 30
          THEN uo.urineoutput_24hr / uo.uo_tm_24hr * 24
  END) as uo_24hr
  from co
  left join urine_output_rate uo
    on co.stay_id = uo.stay_id
    and co.starttime < uo.charttime
    and co.endtime >= uo.charttime
  group by co.stay_id, co.hr
)
-- collapse vasopressors into 1 row per hour
-- also ensures only 1 row per chart time
, vaso AS
(
    SELECT 
        co.stay_id
        , co.hr
        , MAX(epi.vaso_rate) as rate_epinephrine
        , MAX(nor.vaso_rate) as rate_norepinephrine
        , MAX(dop.vaso_rate) as rate_dopamine
        , MAX(dob.vaso_rate) as rate_dobutamine
    FROM co
    LEFT JOIN epinephrine epi
        on co.stay_id = epi.stay_id
        and co.endtime > epi.starttime
        and co.endtime <= epi.endtime
    LEFT JOIN norepinephrine nor
        on co.stay_id = nor.stay_id
        and co.endtime > nor.starttime
        and co.endtime <= nor.endtime
    LEFT JOIN dopamine dop
        on co.stay_id = dop.stay_id
        and co.endtime > dop.starttime
        and co.endtime <= dop.endtime
    LEFT JOIN dobutamine dob
        on co.stay_id = dob.stay_id
        and co.endtime > dob.starttime
        and co.endtime <= dob.endtime
    WHERE epi.stay_id IS NOT NULL
    OR nor.stay_id IS NOT NULL
    OR dop.stay_id IS NOT NULL
    OR dob.stay_id IS NOT NULL
    GROUP BY co.stay_id, co.hr
)
, scorecomp as
(
  select
      co.stay_id
    , co.hr
    , co.starttime, co.endtime
    , pf.pao2fio2ratio_novent
    , pf.pao2fio2ratio_vent
    , vaso.rate_epinephrine
    , vaso.rate_norepinephrine
    , vaso.rate_dopamine
    , vaso.rate_dobutamine
    , vs.meanbp_min
    , gcs.gcs_min
    -- uo
    , uo.uo_24hr
    -- labs
    , bili.bilirubin_max
    , cr.creatinine_max
    , plt.platelet_min
  from co
  left join vs
    on co.stay_id = vs.stay_id
    and co.hr = vs.hr
  left join gcs
    on co.stay_id = gcs.stay_id
    and co.hr = gcs.hr
  left join bili
    on co.stay_id = bili.stay_id
    and co.hr = bili.hr
  left join cr
    on co.stay_id = cr.stay_id
    and co.hr = cr.hr
  left join plt
    on co.stay_id = plt.stay_id
    and co.hr = plt.hr
  left join pf
    on co.stay_id = pf.stay_id
    and co.hr = pf.hr
  left join uo
    on co.stay_id = uo.stay_id
    and co.hr = uo.hr
  left join vaso
    on co.stay_id = vaso.stay_id
    and co.hr = vaso.hr
)
, scorecalc as
(
  -- Calculate the final score
  -- note that if the underlying data is missing, the component is null
  -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
  select scorecomp.*
  -- Respiration
  , case
      when pao2fio2ratio_vent   < 100 then 4
      when pao2fio2ratio_vent   < 200 then 3
      when pao2fio2ratio_novent < 300 then 2
      when pao2fio2ratio_vent   < 300 then 2
      when pao2fio2ratio_novent < 400 then 1
      when pao2fio2ratio_vent   < 400 then 1
      when coalesce(pao2fio2ratio_vent, pao2fio2ratio_novent) is null then null
      else 0
    end as respiration

  -- Coagulation
  , case
      when platelet_min < 20  then 4
      when platelet_min < 50  then 3
      when platelet_min < 100 then 2
      when platelet_min < 150 then 1
      when platelet_min is null then null
      else 0
    end as coagulation

  -- Liver
  , case
      -- Bilirubin checks in mg/dL
        when bilirubin_max >= 12.0 then 4
        when bilirubin_max >= 6.0  then 3
        when bilirubin_max >= 2.0  then 2
        when bilirubin_max >= 1.2  then 1
        when bilirubin_max is null then null
        else 0
      end as liver

  -- Cardiovascular
  , case
      when rate_dopamine > 15 or rate_epinephrine >  0.1 or rate_norepinephrine >  0.1 then 4
      when rate_dopamine >  5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
      when rate_dopamine >  0 or rate_dobutamine > 0 then 2
      when meanbp_min < 70 then 1
      when coalesce(meanbp_min, rate_dopamine, rate_dobutamine, rate_epinephrine, rate_norepinephrine) is null then null
      else 0
    end as cardiovascular

  -- Neurological failure (GCS)
  , case
      when (gcs_min >= 13 and gcs_min <= 14) then 1
      when (gcs_min >= 10 and gcs_min <= 12) then 2
      when (gcs_min >=  6 and gcs_min <=  9) then 3
      when  gcs_min <   6 then 4
      when  gcs_min is null then null
      else 0
    end as cns

  -- Renal failure - high creatinine or low urine output
  , case
    when (creatinine_max >= 5.0) then 4
    when uo_24hr < 200 then 4
    when (creatinine_max >= 3.5 and creatinine_max < 5.0) then 3
    when uo_24hr < 500 then 3
    when (creatinine_max >= 2.0 and creatinine_max < 3.5) then 2
    when (creatinine_max >= 1.2 and creatinine_max < 2.0) then 1
    when coalesce (uo_24hr, creatinine_max) is null then null
    else 0 
  end as renal
  from scorecomp
)
, score_final as
(
  select s.*
    -- Combine all the scores to get SOFA
    -- Impute 0 if the score is missing
   -- the window function takes the max over the last 24 hours
    , coalesce(
        MAX(respiration) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as respiration_24hours
     , coalesce(
         MAX(coagulation) OVER (PARTITION BY stay_id ORDER BY HR
         ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
        ,0) as coagulation_24hours
    , coalesce(
        MAX(liver) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as liver_24hours
    , coalesce(
        MAX(cardiovascular) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as cardiovascular_24hours
    , coalesce(
        MAX(cns) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as cns_24hours
    , coalesce(
        MAX(renal) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0) as renal_24hours

    -- sum together data for final SOFA
    , coalesce(
        MAX(respiration) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
         MAX(coagulation) OVER (PARTITION BY stay_id ORDER BY HR
         ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
        MAX(liver) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
        MAX(cardiovascular) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
        MAX(cns) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
     + coalesce(
        MAX(renal) OVER (PARTITION BY stay_id ORDER BY HR
        ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING)
      ,0)
    as sofa_24hours
  from scorecalc s
  WINDOW W as
  (
    PARTITION BY stay_id
    ORDER BY hr
    ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING
  )
)
select * from score_final
where hr >= 0;
-- 
-- comorbidity

-- CCI指数
-- charlson
-- ------------------------------------------------------------------
-- This query extracts Charlson Comorbidity Index (CCI) based on the recorded ICD-9 and ICD-10 codes.
--
-- Reference for CCI:（参考文献）
-- (1) Charlson ME, Pompei P, Ales KL, MacKenzie CR. (1987) A new method of classifying prognostic 
-- comorbidity in longitudinal studies: development and validation.J Chronic Dis; 40(5):373-83.
--
-- (2) Charlson M, Szatrowski TP, Peterson J, Gold J. (1994) Validation of a combined comorbidity 
-- index. J Clin Epidemiol; 47(11):1245-51.
-- 
-- Reference for ICD-9-CM and ICD-10 Coding Algorithms for Charlson Comorbidities:
-- (3) Quan H, Sundararajan V, Halfon P, et al. Coding algorithms for defining Comorbidities in ICD-9-CM
-- and ICD-10 administrative data. Med Care. 2005 Nov; 43(11): 1130-9.
-- ------------------------------------------------------------------
create materialized view if not exists charlson as 
WITH diag AS
(
    SELECT 
        hadm_id
        , CASE WHEN icd_version = 9 THEN icd_code ELSE NULL END AS icd9_code
        , CASE WHEN icd_version = 10 THEN icd_code ELSE NULL END AS icd10_code
    FROM mimic_hosp.diagnoses_icd diag
)
, com AS
(
    SELECT
        ad.hadm_id

        -- Myocardial infarction
        , MAX(CASE WHEN
            SUBSTR(icd9_code, 1, 3) IN ('410','412')
            OR
            SUBSTR(icd10_code, 1, 3) IN ('I21','I22')
            OR
            SUBSTR(icd10_code, 1, 4) = 'I252'
            THEN 1 
            ELSE 0 END) AS myocardial_infarct

        -- Congestive heart failure
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) = '428'
            OR
            SUBSTR(icd9_code, 1, 5) IN ('39891','40201','40211','40291','40401','40403',
                          '40411','40413','40491','40493')
            OR 
            SUBSTR(icd9_code, 1, 4) BETWEEN '4254' AND '4259'
            OR
            SUBSTR(icd10_code, 1, 3) IN ('I43','I50')
            OR
            SUBSTR(icd10_code, 1, 4) IN ('I099','I110','I130','I132','I255','I420',
                                                   'I425','I426','I427','I428','I429','P290')
            THEN 1 
            ELSE 0 END) AS congestive_heart_failure

        -- Peripheral vascular disease
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) IN ('440','441')
            OR
            SUBSTR(icd9_code, 1, 4) IN ('0930','4373','4471','5571','5579','V434')
            OR
            SUBSTR(icd9_code, 1, 4) BETWEEN '4431' AND '4439'
            OR
            SUBSTR(icd10_code, 1, 3) IN ('I70','I71')
            OR
            SUBSTR(icd10_code, 1, 4) IN ('I731','I738','I739','I771','I790',
                                                   'I792','K551','K558','K559','Z958','Z959')
            THEN 1 
            ELSE 0 END) AS peripheral_vascular_disease

        -- Cerebrovascular disease
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) BETWEEN '430' AND '438'
            OR
            SUBSTR(icd9_code, 1, 5) = '36234'
            OR
            SUBSTR(icd10_code, 1, 3) IN ('G45','G46')
            OR 
            SUBSTR(icd10_code, 1, 3) BETWEEN 'I60' AND 'I69'
            OR
            SUBSTR(icd10_code, 1, 4) = 'H340'
            THEN 1 
            ELSE 0 END) AS cerebrovascular_disease

        -- Dementia
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) = '290'
            OR
            SUBSTR(icd9_code, 1, 4) IN ('2941','3312')
            OR
            SUBSTR(icd10_code, 1, 3) IN ('F00','F01','F02','F03','G30')
            OR
            SUBSTR(icd10_code, 1, 4) IN ('F051','G311')
            THEN 1 
            ELSE 0 END) AS dementia

        -- Chronic pulmonary disease
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) BETWEEN '490' AND '505'
            OR
            SUBSTR(icd9_code, 1, 4) IN ('4168','4169','5064','5081','5088')
            OR 
            SUBSTR(icd10_code, 1, 3) BETWEEN 'J40' AND 'J47'
            OR 
            SUBSTR(icd10_code, 1, 3) BETWEEN 'J60' AND 'J67'
            OR
            SUBSTR(icd10_code, 1, 4) IN ('I278','I279','J684','J701','J703')
            THEN 1 
            ELSE 0 END) AS chronic_pulmonary_disease

        -- Rheumatic disease
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) = '725'
            OR
            SUBSTR(icd9_code, 1, 4) IN ('4465','7100','7101','7102','7103',
                                                  '7104','7140','7141','7142','7148')
            OR
            SUBSTR(icd10_code, 1, 3) IN ('M05','M06','M32','M33','M34')
            OR
            SUBSTR(icd10_code, 1, 4) IN ('M315','M351','M353','M360')
            THEN 1 
            ELSE 0 END) AS rheumatic_disease

        -- Peptic ulcer disease
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) IN ('531','532','533','534')
            OR
            SUBSTR(icd10_code, 1, 3) IN ('K25','K26','K27','K28')
            THEN 1 
            ELSE 0 END) AS peptic_ulcer_disease

        -- Mild liver disease
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) IN ('570','571')
            OR
            SUBSTR(icd9_code, 1, 4) IN ('0706','0709','5733','5734','5738','5739','V427')
            OR
            SUBSTR(icd9_code, 1, 5) IN ('07022','07023','07032','07033','07044','07054')
            OR
            SUBSTR(icd10_code, 1, 3) IN ('B18','K73','K74')
            OR
            SUBSTR(icd10_code, 1, 4) IN ('K700','K701','K702','K703','K709','K713',
                                                   'K714','K715','K717','K760','K762',
                                                   'K763','K764','K768','K769','Z944')
            THEN 1 
            ELSE 0 END) AS mild_liver_disease

        -- Diabetes without chronic complication
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 4) IN ('2500','2501','2502','2503','2508','2509') 
            OR
            SUBSTR(icd10_code, 1, 4) IN ('E100','E10l','E106','E108','E109','E110','E111',
                                                   'E116','E118','E119','E120','E121','E126','E128',
                                                   'E129','E130','E131','E136','E138','E139','E140',
                                                   'E141','E146','E148','E149')
            THEN 1 
            ELSE 0 END) AS diabetes_without_cc

        -- Diabetes with chronic complication
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 4) IN ('2504','2505','2506','2507')
            OR
            SUBSTR(icd10_code, 1, 4) IN ('E102','E103','E104','E105','E107','E112','E113',
                                                   'E114','E115','E117','E122','E123','E124','E125',
                                                   'E127','E132','E133','E134','E135','E137','E142',
                                                   'E143','E144','E145','E147')
            THEN 1 
            ELSE 0 END) AS diabetes_with_cc

        -- Hemiplegia or paraplegia
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) IN ('342','343')
            OR
            SUBSTR(icd9_code, 1, 4) IN ('3341','3440','3441','3442',
                                                  '3443','3444','3445','3446','3449')
            OR 
            SUBSTR(icd10_code, 1, 3) IN ('G81','G82')
            OR 
            SUBSTR(icd10_code, 1, 4) IN ('G041','G114','G801','G802','G830',
                                                   'G831','G832','G833','G834','G839')
            THEN 1 
            ELSE 0 END) AS paraplegia

        -- Renal disease
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) IN ('582','585','586','V56')
            OR
            SUBSTR(icd9_code, 1, 4) IN ('5880','V420','V451')
            OR
            SUBSTR(icd9_code, 1, 4) BETWEEN '5830' AND '5837'
            OR
            SUBSTR(icd9_code, 1, 5) IN ('40301','40311','40391','40402','40403','40412','40413','40492','40493')          
            OR
            SUBSTR(icd10_code, 1, 3) IN ('N18','N19')
            OR
            SUBSTR(icd10_code, 1, 4) IN ('I120','I131','N032','N033','N034',
                                                   'N035','N036','N037','N052','N053',
                                                   'N054','N055','N056','N057','N250',
                                                   'Z490','Z491','Z492','Z940','Z992')
            THEN 1 
            ELSE 0 END) AS renal_disease

        -- Any malignancy, including lymphoma and leukemia, except malignant neoplasm of skin
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) BETWEEN '140' AND '172'
            OR
            SUBSTR(icd9_code, 1, 4) BETWEEN '1740' AND '1958'
            OR
            SUBSTR(icd9_code, 1, 3) BETWEEN '200' AND '208'
            OR
            SUBSTR(icd9_code, 1, 4) = '2386'
            OR
            SUBSTR(icd10_code, 1, 3) IN ('C43','C88')
            OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'C00' AND 'C26'
            OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'C30' AND 'C34'
            OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'C37' AND 'C41'
            OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'C45' AND 'C58'
            OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'C60' AND 'C76'
            OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'C81' AND 'C85'
            OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'C90' AND 'C97'
            THEN 1 
            ELSE 0 END) AS malignant_cancer

        -- Moderate or severe liver disease
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 4) IN ('4560','4561','4562')
            OR
            SUBSTR(icd9_code, 1, 4) BETWEEN '5722' AND '5728'
            OR
            SUBSTR(icd10_code, 1, 4) IN ('I850','I859','I864','I982','K704','K711',
                                                   'K721','K729','K765','K766','K767')
            THEN 1 
            ELSE 0 END) AS severe_liver_disease

        -- Metastatic solid tumor
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) IN ('196','197','198','199')
            OR 
            SUBSTR(icd10_code, 1, 3) IN ('C77','C78','C79','C80')
            THEN 1 
            ELSE 0 END) AS metastatic_solid_tumor

        -- AIDS/HIV
        , MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) IN ('042','043','044')
            OR 
            SUBSTR(icd10_code, 1, 3) IN ('B20','B21','B22','B24')
            THEN 1 
            ELSE 0 END) AS aids
    FROM mimic_hosp.admissions ad
    LEFT JOIN diag
    ON ad.hadm_id = diag.hadm_id
    GROUP BY ad.hadm_id
)
, ag AS
(
    SELECT 
        hadm_id
        , age
        , CASE WHEN age <= 40 THEN 0
    WHEN age <= 50 THEN 1
    WHEN age <= 60 THEN 2
    WHEN age <= 70 THEN 3
    ELSE 4 END AS age_score
    FROM age
)
SELECT 
    ad.subject_id
    , ad.hadm_id
    , ag.age_score
    , myocardial_infarct
    , congestive_heart_failure
    , peripheral_vascular_disease
    , cerebrovascular_disease
    , dementia
    , chronic_pulmonary_disease
    , rheumatic_disease
    , peptic_ulcer_disease
    , mild_liver_disease
    , diabetes_without_cc
    , diabetes_with_cc
    , paraplegia
    , renal_disease
    , malignant_cancer
    , severe_liver_disease 
    , metastatic_solid_tumor 
    , aids
    -- Calculate the Charlson Comorbidity Score using the original
    -- weights from Charlson, 1987.
    , age_score
    + myocardial_infarct + congestive_heart_failure + peripheral_vascular_disease
    + cerebrovascular_disease + dementia + chronic_pulmonary_disease
    + rheumatic_disease + peptic_ulcer_disease
    + GREATEST(mild_liver_disease, 3*severe_liver_disease)
    + GREATEST(2*diabetes_with_cc, diabetes_without_cc)
    + GREATEST(2*malignant_cancer, 6*metastatic_solid_tumor)
    + 2*paraplegia + 2*renal_disease 
    + 6*aids
    AS charlson_comorbidity_index
FROM mimic_hosp.admissions ad
LEFT JOIN com
ON ad.hadm_id = com.hadm_id
LEFT JOIN ag
ON com.hadm_id = ag.hadm_id
;

-- 疑似感染
-- suspicion_of_infection
-- note this duplicates prescriptions
-- each ICU stay in the same hospitalization will get a copy of all prescriptions for that hospitalization
create materialized view if not exists suspicion_of_infection as 
WITH ab_tbl AS 
(
  select
      abx.subject_id, abx.hadm_id, abx.stay_id
    , abx.antibiotic
    , abx.starttime AS antibiotic_time
    -- date is used to match microbiology cultures with only date available
    , DATE_TRUNC('day', abx.starttime) AS antibiotic_date
    , abx.stoptime AS antibiotic_stoptime
    -- create a unique identifier for each patient antibiotic
    , ROW_NUMBER() OVER
    (
      PARTITION BY subject_id
      ORDER BY starttime, stoptime, antibiotic
    ) AS ab_id
  from antibiotic abx
)
, me as
(
  select micro_specimen_id
    -- the following columns are identical for all rows of the same micro_specimen_id
    -- these aggregates simply collapse duplicates down to 1 row
    , MAX(subject_id) AS subject_id
    , MAX(hadm_id) AS hadm_id
    , MAX(chartdate) AS chartdate
    , MAX(charttime) AS charttime
    , MAX(spec_type_desc) AS spec_type_desc
    , max(case when org_name is not null and org_name != '' then 1 else 0 end) as PositiveCulture
  from mimic_hosp.microbiologyevents
  group by micro_specimen_id
)
-- culture followed by an antibiotic
, me_then_ab AS
(
  select
    ab_tbl.subject_id
    , ab_tbl.hadm_id
    , ab_tbl.stay_id
    , ab_tbl.ab_id
    
    , me72.micro_specimen_id
    , coalesce(me72.charttime, me72.chartdate) as last72_charttime
    , me72.positiveculture as last72_positiveculture
    , me72.spec_type_desc as last72_specimen

    -- we will use this partition to select the earliest culture before this abx
    -- this ensures each antibiotic is only matched to a single culture
    -- and consequently we have 1 row per antibiotic
    , ROW_NUMBER() OVER
    (
      PARTITION BY ab_tbl.subject_id, ab_tbl.ab_id
      ORDER BY me72.chartdate, me72.charttime NULLS LAST
    ) AS micro_seq
  from ab_tbl
  -- abx taken after culture, but no more than 72 hours after
  LEFT JOIN me me72
    on ab_tbl.subject_id = me72.subject_id
    and
    (
      (
      -- if charttime is available, use it
          me72.charttime is not null
      and ab_tbl.antibiotic_time > me72.charttime
      and ab_tbl.antibiotic_time <= DATETIME_ADD(me72.charttime, INTERVAL '72' HOUR) 
      )
      OR
      (
      -- if charttime is not available, use chartdate
          me72.charttime is null
      and antibiotic_date >= me72.chartdate
      and antibiotic_date <= me72.chartdate + INTERVAL '3' DAY
      )
    )
)
, ab_then_me AS
(
  select
      ab_tbl.subject_id
    , ab_tbl.hadm_id
    , ab_tbl.stay_id
    , ab_tbl.ab_id
    
    , me24.micro_specimen_id
    , COALESCE(me24.charttime, me24.chartdate) as next24_charttime
    , me24.positiveculture as next24_positiveculture
    , me24.spec_type_desc as next24_specimen

    -- we will use this partition to select the earliest culture before this abx
    -- this ensures each antibiotic is only matched to a single culture
    -- and consequently we have 1 row per antibiotic
    , ROW_NUMBER() OVER
    (
      PARTITION BY ab_tbl.subject_id, ab_tbl.ab_id
      ORDER BY me24.chartdate, me24.charttime NULLS LAST
    ) AS micro_seq
  from ab_tbl
  -- culture in subsequent 24 hours
  LEFT JOIN me me24
    on ab_tbl.subject_id = me24.subject_id
    and
    (
      (
          -- if charttime is available, use it
          me24.charttime is not null
      and ab_tbl.antibiotic_time >= DATETIME_SUB(me24.charttime, INTERVAL '24' HOUR)  
      and ab_tbl.antibiotic_time < me24.charttime
      )
      OR
      (
          -- if charttime is not available, use chartdate
          me24.charttime is null
      and ab_tbl.antibiotic_date >= me24.chartdate-INTERVAL '1' DAY
      and ab_tbl.antibiotic_date <= me24.chartdate
      )
    )
)
SELECT
ab_tbl.subject_id
, ab_tbl.stay_id
, ab_tbl.hadm_id
, ab_tbl.ab_id
, ab_tbl.antibiotic
, ab_tbl.antibiotic_time

, CASE
  WHEN last72_specimen IS NULL AND next24_specimen IS NULL
    THEN 0
  ELSE 1 
  END AS suspected_infection
-- time of suspected infection:
--    (1) the culture time (if before antibiotic)
--    (2) or the antibiotic time (if before culture)
, CASE
  WHEN last72_specimen IS NULL AND next24_specimen IS NULL
    THEN NULL
  ELSE COALESCE(last72_charttime, antibiotic_time)
  END AS suspected_infection_time

, COALESCE(last72_charttime, next24_charttime) AS culture_time

-- the specimen that was cultured
, COALESCE(last72_specimen, next24_specimen) AS specimen

-- whether the cultured specimen ended up being positive or not
, COALESCE(last72_positiveculture, next24_positiveculture) AS positive_culture

FROM ab_tbl
LEFT JOIN ab_then_me ab2me
    ON ab_tbl.subject_id = ab2me.subject_id
    AND ab_tbl.ab_id = ab2me.ab_id
    AND ab2me.micro_seq = 1
LEFT JOIN me_then_ab me2ab
    ON ab_tbl.subject_id = me2ab.subject_id
    AND ab_tbl.ab_id = me2ab.ab_id
    AND me2ab.micro_seq = 1
;

-- 脓毒血症3.0
-- sepsis3
-- Creates a table with "onset" time of Sepsis-3 in the ICU.
-- That is, the earliest time at which a patient had SOFA >= 2 and suspicion of infection.
-- As many variables used in SOFA are only collected in the ICU, this query can only
-- define sepsis-3 onset within the ICU.

-- extract rows with SOFA >= 2
-- implicitly this assumes baseline SOFA was 0 before ICU admission.
create materialized view if not exists sepsis3 as 
WITH sofa AS
(
  SELECT stay_id
    , starttime, endtime
    , respiration_24hours as respiration
    , coagulation_24hours as coagulation
    , liver_24hours as liver
    , cardiovascular_24hours as cardiovascular
    , cns_24hours as cns
    , renal_24hours as renal
    , sofa_24hours as sofa_score
  FROM sofa
  WHERE sofa_24hours >= 2
)
, s1 as
(
  SELECT 
    soi.subject_id
    , soi.stay_id
    -- suspicion columns
    , soi.ab_id
    , soi.antibiotic
    , soi.antibiotic_time
    , soi.culture_time
    , soi.suspected_infection
    , soi.suspected_infection_time
    , soi.specimen
    , soi.positive_culture
    -- sofa columns
    , starttime, endtime
    , respiration, coagulation, liver, cardiovascular, cns, renal
    , sofa_score
    -- All rows have an associated suspicion of infection event
    -- Therefore, Sepsis-3 is defined as SOFA >= 2.
    -- Implicitly, the baseline SOFA score is assumed to be zero, as we do not know
    -- if the patient has preexisting (acute or chronic) organ dysfunction 
    -- before the onset of infection.
    , sofa_score >= 2 and suspected_infection = 1 as sepsis3
    -- subselect to the earliest suspicion/antibiotic/SOFA row
    , ROW_NUMBER() OVER
    (
        PARTITION BY soi.stay_id
        ORDER BY suspected_infection_time, antibiotic_time, culture_time, endtime
    ) AS rn_sus
  FROM suspicion_of_infection as soi
  INNER JOIN sofa
    ON soi.stay_id = sofa.stay_id 
    AND sofa.endtime >= DATETIME_SUB(soi.suspected_infection_time, INTERVAL '48' HOUR)
    AND sofa.endtime <= DATETIME_ADD(soi.suspected_infection_time, INTERVAL '24' HOUR)
  -- only include in-ICU rows
  WHERE soi.stay_id is not null
)
SELECT 
subject_id, stay_id
-- note: there may be more than one antibiotic given at this time
, antibiotic_time
-- culture times may be dates, rather than times
, culture_time
, suspected_infection_time
-- endtime is latest time at which the SOFA score is valid
, endtime as sofa_time
, sofa_score
, respiration, coagulation, liver, cardiovascular, cns, renal
, sepsis3
FROM s1
WHERE rn_sus = 1;

-- 米力农
-- milrinone
create materialized view if not exists milrinone as 
select
stay_id, linkorderid
-- all rows in mcg/kg/min
, rate as vaso_rate
, amount as vaso_amount
, starttime
, endtime
from mimic_icu.inputevents
where itemid = 221986;

-- 血管活性物质
-- vasoactive_agent
create materialized view if not exists vasoactive_agent as 
WITH tm AS
(
    SELECT stay_id, starttime AS vasotime FROM dobutamine
    UNION DISTINCT
    SELECT stay_id, starttime AS vasotime FROM dopamine
    UNION DISTINCT
    SELECT stay_id, starttime AS vasotime FROM epinephrine
    UNION DISTINCT
    SELECT stay_id, starttime AS vasotime FROM norepinephrine
    UNION DISTINCT
    SELECT stay_id, starttime AS vasotime FROM phenylephrine
    UNION DISTINCT
    SELECT stay_id, starttime AS vasotime FROM vasopressin
    UNION DISTINCT
    SELECT stay_id, starttime AS vasotime FROM milrinone
    UNION DISTINCT
    -- combine end times from the same tables
    SELECT stay_id, endtime AS vasotime FROM dobutamine
    UNION DISTINCT
    SELECT stay_id, endtime AS vasotime FROM dopamine
    UNION DISTINCT
    SELECT stay_id, endtime AS vasotime FROM epinephrine
    UNION DISTINCT
    SELECT stay_id, endtime AS vasotime FROM norepinephrine
    UNION DISTINCT
    SELECT stay_id, endtime AS vasotime FROM phenylephrine
    UNION DISTINCT
    SELECT stay_id, endtime AS vasotime FROM vasopressin
    UNION DISTINCT
    SELECT stay_id, endtime AS vasotime FROM milrinone
)
-- create starttime/endtime from all possible times collected
, tm_lag AS
(
    SELECT stay_id
    , vasotime AS starttime
    -- note: the last row for each partition (stay_id) will have a NULL endtime
    -- we can drop this row later, as we know that no vasopressor will start at this time
    -- (otherwise, we would have a later end time, which would mean it's not the last row!)
    -- QED? :)
    , LEAD(vasotime, 1) OVER (PARTITION BY stay_id ORDER BY vasotime) AS endtime
    FROM tm
)
-- left join to raw data tables to combine doses
SELECT t.stay_id, t.starttime, t.endtime
-- inopressors/vasopressors
, dop.vaso_rate AS dopamine
, epi.vaso_rate AS epinephrine
, nor.vaso_rate AS norepinephrine
, phe.vaso_rate AS phenylephrine
, vas.vaso_rate AS vasopressin
-- inodialators
, dob.vaso_rate AS dobutamine
, mil.vaso_rate AS milrinone
-- isoproterenol is used in CCU/CVICU but not in metavision
-- other drugs not included here but (rarely) used in the BIDMC:
-- angiotensin II, methylene blue
FROM tm_lag t
LEFT JOIN dobutamine dob
    ON t.stay_id = dob.stay_id
    AND t.starttime >= dob.starttime
    AND t.endtime <= dob.endtime
LEFT JOIN dopamine dop
    ON t.stay_id = dop.stay_id
    AND t.starttime >= dop.starttime
    AND t.endtime <= dop.endtime
LEFT JOIN epinephrine epi
    ON t.stay_id = epi.stay_id
    AND t.starttime >= epi.starttime
    AND t.endtime <= epi.endtime
LEFT JOIN norepinephrine nor
    ON t.stay_id = nor.stay_id
    AND t.starttime >= nor.starttime
    AND t.endtime <= nor.endtime
LEFT JOIN phenylephrine phe
    ON t.stay_id = phe.stay_id
    AND t.starttime >= phe.starttime
    AND t.endtime <= phe.endtime
LEFT JOIN vasopressin vas
    ON t.stay_id = vas.stay_id
    AND t.starttime >= vas.starttime
    AND t.endtime <= vas.endtime
LEFT JOIN milrinone mil
    ON t.stay_id = mil.stay_id
    AND t.starttime >= mil.starttime
    AND t.endtime <= mil.endtime
-- remove the final row for each stay_id
-- it will not have any infusions associated with it
WHERE t.endtime IS NOT NULL;

-- 去甲肾上腺素当量
-- norepinephrine_equivalent_dose
create materialized view if not exists norepinephrine_equivalent_dose as 
SELECT stay_id, starttime, endtime
-- calculate the dose
, ROUND((COALESCE(norepinephrine, 0)
  + COALESCE(epinephrine, 0)
  + COALESCE(phenylephrine/10, 0)
  + COALESCE(dopamine/100, 0)
  -- + metaraminol/8 -- metaraminol not used in BIDMC
  + COALESCE(vasopressin*2.5, 0))::numeric
  -- angotensin_ii*10 -- angitensin ii rarely used, currently not incorporated
  -- (it could be included due to norepinephrine sparing effects)
  , 4) AS norepinephrine_equivalent_dose
  -- angotensin_ii*10 -- angitensin ii rarely used, currently not incorporated
  -- (it could be included due to norepinephrine sparing effects)
FROM vasoactive_agent
WHERE norepinephrine IS NOT NULL
OR epinephrine IS NOT NULL
OR phenylephrine IS NOT NULL
OR dopamine IS NOT NULL
OR vasopressin IS NOT NULL;