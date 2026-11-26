-- 02_dq_pipeline.sql
-- 用途：对 staging.flights 应用 DQ-1 到 DQ-8，生成 dq schema 下的所有对象
-- 输入：staging.flights
-- 输出：dq.flights_checked  每一行 + 时间戳 + 标记列 + quarantine_reason + dropped_row
--       dq.quarantine       有隔离原因的行（审计快照）
--       dq.dq_results       每月 x 每条规则一行（13 条规则），含比率、阈值、PASS/WARN/FAIL
--       dq.month_status     每月取最严重的状态（view）
--       dq.flights_clean    未隔离、非重复、所在月份没有 FAIL 的行（view），分析层从这里读
-- 可重复运行：每个对象都先删除再重建。整个文件从上到下一次跑完。
-- 验证：运行 03_dq_checks.sql，应全部 PASS。

-- 0. 先删 view（view 依赖下面的表，不先删的话 DROP TABLE 会报错）
DROP VIEW IF EXISTS dq.flights_clean;
DROP VIEW IF EXISTS dq.month_status;
CREATE SCHEMA IF NOT EXISTS dq;

-- 1. 逐行检查：dq.flights_checked
DROP TABLE IF EXISTS dq.flights_checked;
CREATE TABLE dq.flights_checked AS
WITH src AS (
    SELECT *
    FROM staging.flights
    -- WHERE year = 2025 AND month = 12   -- 调试时可以只跑一个月
),

-- ===== DQ-1 第 1 层 =====
padded AS (
    SELECT *,
           crsdeptime ~ '^[0-9]{1,4}$' AS dep_is_digits,
           LPAD(crsdeptime, 4, '0')    AS dep_padded
    FROM src
),

-- ===== DQ-1 第 2 层 =====
validated AS (
    SELECT *,
        CASE WHEN dep_is_digits
        AND (dep_padded = '2400'
            OR (LEFT(dep_padded, 2) < '24'
                AND TRY_CAST(LEFT(dep_padded, 2) || ':' || RIGHT(dep_padded, 2) AS TIME) IS NOT NULL))
        THEN TRUE ELSE FALSE END AS dep_is_valid
    FROM padded
),

-- ===== DQ-3 阈值（读 src） =====
p_dep AS (
    SELECT year, month,
           PERCENTILE_CONT(0.995) WITHIN GROUP (ORDER BY depdelay) AS dep_cutoff
    FROM src GROUP BY year, month
),
p_arr  AS (   SELECT year, month, percentile_cont (0.995) within group (order by arrdelay) AS arr_cutoff
    FROM src
    GROUP BY year, month ),
p_late AS (    SELECT year, month, percentile_cont (0.995) within group (order by lateaircraftdelay) AS late_cutoff
    FROM src
    GROUP BY year, month ),
p_taxi AS (    SELECT year, month, percentile_cont (0.995) within group (order by taxiout) AS taxi_cutoff
    FROM src
    GROUP BY year, month ),
cutoffs AS (
    SELECT d.year, d.month, d.dep_cutoff, a.arr_cutoff, l.late_cutoff, t.taxi_cutoff
    FROM p_dep d
    JOIN p_arr  a ON d.year = a.year AND d.month = a.month
    JOIN p_late l ON d.year = l.year AND d.month = l.month
    JOIN p_taxi t ON d.year = t.year AND d.month = t.month
),

-- ===== DQ-1 第 3 层 + DQ-7 窗口函数 =====
windowed AS (
    SELECT v.*,
           c.dep_cutoff, c.arr_cutoff, c.late_cutoff, c.taxi_cutoff,
            CASE WHEN dep_is_valid THEN DATEADD(minute, CAST(LEFT(dep_padded, 2) AS INT) * 60 + CAST(RIGHT(dep_padded, 2) AS INT),flightdate::TIMESTAMP) END AS scheduled_departure,
            COUNT(*) OVER (PARTITION BY flightdate, reporting_airline,flight_number_reporting_airline, origin,dest,crsdeptime)AS key_count,
            COUNT(*) OVER (PARTITION BY flightdate, reporting_airline,flight_number_reporting_airline, origin,dest,crsdeptime,depdelay,arrdelay,cancelled,tail_number)AS same_count,
            ROW_NUMBER() OVER (PARTITION BY flightdate, reporting_airline,flight_number_reporting_airline, origin,dest,crsdeptime ORDER BY source_file) AS rn
    FROM validated v
    JOIN cutoffs c ON v.year = c.year AND v.month = c.month
),

-- ===== DQ-1 第 4 层 + 所有标记和原因 =====
flagged AS (
    SELECT *,
           DATEADD(minute, CAST(depdelay AS INT), scheduled_departure) AS actual_departure,

           CASE WHEN cancelled = 1 AND NULLIF(TRIM(deptime), '') IS NOT NULL  THEN TRUE ELSE FALSE END AS dq_cancelled_with_deptime,
           CASE WHEN depdelay>dep_cutoff OR arrdelay > arr_cutoff OR lateaircraftdelay > late_cutoff OR taxiout > taxi_cutoff THEN TRUE ELSE FALSE END AS dq_extreme_value,
           CASE WHEN NULLIF(TRIM(tail_number), '') IS NULL  THEN TRUE ELSE FALSE END AS dq_missing_tail_number,
           CASE WHEN cancelled = 1 AND NULLIF(TRIM(cancellationcode), '') IS NULL THEN TRUE ELSE FALSE END AS dq_cancelled_without_code,
           CASE WHEN key_count>1 AND same_count= key_count AND RN<>1 THEN TRUE ELSE FALSE END AS dropped_row,
           CASE WHEN cancelled = 0 AND diverted = 0 AND arrdel15 = 1
                AND carrierdelay = 0 AND weatherdelay = 0 AND nasdelay = 0
                AND securitydelay = 0 AND lateaircraftdelay = 0
                THEN TRUE ELSE FALSE END AS dq_unattributed_delay,

              CASE WHEN crsdeptime IS NOT NULL AND NOT dep_is_valid THEN 'DQ-1;' ELSE '' END
           || CASE WHEN lateaircraftdelay < 0 OR carrierdelay < 0 OR weatherdelay < 0 OR nasdelay < 0 OR securitydelay < 0     THEN 'DQ-3;' ELSE '' END
           || CASE WHEN flightdate IS NULL
                    OR NULLIF(TRIM(reporting_airline), '') IS NULL
                    OR flight_number_reporting_airline IS NULL
                    OR NULLIF(TRIM(origin), '') IS NULL
                    OR NULLIF(TRIM(dest), '') IS NULL
                    OR NULLIF(TRIM(crsdeptime), '') IS NULL
                    OR cancelled IS NULL
                    OR diverted IS NULL
                    OR (cancelled = 0 AND diverted = 0 AND (depdelay IS NULL OR arrdelay IS NULL))      THEN 'DQ-4;' ELSE '' END
           || CASE WHEN cancelled = 0 and NULLIF(TRIM(cancellationcode), '') IS not NULL THEN 'DQ-6;' ELSE '' END
           || CASE WHEN key_count>same_count  THEN 'DQ-7;' ELSE '' END
           || CASE WHEN cancelled = 0 AND diverted = 0 AND (
                            (arrdel15 = 1 AND (
                                (carrierdelay + weatherdelay + nasdelay + securitydelay + lateaircraftdelay <> arrdelayminutes
                                AND  carrierdelay + weatherdelay + nasdelay + securitydelay + lateaircraftdelay <> 0)
                                                OR carrierdelay IS NULL OR weatherdelay IS NULL OR nasdelay IS NULL
                                                OR securitydelay IS NULL OR lateaircraftdelay IS NULL))
                            OR (arrdel15 = 0 AND (carrierdelay IS NOT NULL OR weatherdelay IS NOT NULL OR nasdelay IS NOT NULL
                                                OR securitydelay IS NOT NULL OR lateaircraftdelay IS NOT NULL))
                            )      THEN 'DQ-8;' ELSE '' END AS quarantine_reason
    FROM windowed 
)

-- 输出全部列，建成 dq.flights_checked
SELECT *
FROM flagged;


-- 2. 隔离记录：dq.quarantine
DROP TABLE IF EXISTS dq.quarantine;
CREATE TABLE dq.quarantine AS
SELECT * FROM dq.flights_checked WHERE quarantine_reason <> '';


-- 3. 每月 x 每条规则的结果：dq.dq_results
DROP TABLE IF EXISTS dq.dq_results;
CREATE TABLE dq.dq_results AS
WITH per_month AS (
    SELECT year, month,
           -- 分母
           COUNT(*)                                                                    AS total_rows,
           SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)                              AS cancelled_rows,
           SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END)                              AS not_cancelled_rows,
           SUM(CASE WHEN cancelled = 0 AND diverted = 0 AND arrdel15 = 1 THEN 1 ELSE 0 END) AS delayed_rows,
           -- 违规数
           SUM(CASE WHEN quarantine_reason LIKE '%DQ-1;%' THEN 1 ELSE 0 END)           AS v_dq1,
           SUM(CASE WHEN dq_cancelled_with_deptime THEN 1 ELSE 0 END)                  AS v_dq2,
           SUM(CASE WHEN dq_extreme_value THEN 1 ELSE 0 END)                           AS v_dq3a,
           SUM(CASE WHEN quarantine_reason LIKE '%DQ-3;%' THEN 1 ELSE 0 END)           AS v_dq3b,
           SUM(CASE WHEN quarantine_reason LIKE '%DQ-4;%' THEN 1 ELSE 0 END)           AS v_dq4,
           SUM(CASE WHEN dq_missing_tail_number AND cancelled = 0 THEN 1 ELSE 0 END)   AS v_dq5a,
           SUM(CASE WHEN dq_missing_tail_number AND cancelled = 1 THEN 1 ELSE 0 END)   AS v_dq5b,
           SUM(CASE WHEN dq_cancelled_without_code THEN 1 ELSE 0 END)                  AS v_dq6a,
           SUM(CASE WHEN quarantine_reason LIKE '%DQ-6;%' THEN 1 ELSE 0 END)           AS v_dq6b,
           SUM(CASE WHEN dropped_row THEN 1 ELSE 0 END)                                AS v_dq7a,
           SUM(CASE WHEN quarantine_reason LIKE '%DQ-7;%' THEN 1 ELSE 0 END)           AS v_dq7b,
           SUM(CASE WHEN dq_unattributed_delay THEN 1 ELSE 0 END)                      AS v_dq8a,
           SUM(CASE WHEN quarantine_reason LIKE '%DQ-8;%' THEN 1 ELSE 0 END)           AS v_dq8b
    FROM dq.flights_checked
    GROUP BY year, month
),
long AS (
    SELECT year, month,
           'DQ-1'::VARCHAR(10)                AS rule_id,
           'invalid crsdeptime'::VARCHAR(100) AS check_name,
           v_dq1                              AS violations,
           total_rows                         AS denominator,
           NULL::FLOAT                        AS warn_threshold,
           0.001::FLOAT                       AS fail_threshold
    FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-2', 'cancelled with deptime', v_dq2, total_rows, 0.007, NULL FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-3a', 'extreme value', v_dq3a, total_rows, NULL, NULL FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-3b', 'negative delay cause', v_dq3b, total_rows, 0.005, 0.007 FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-4', 'missing required field', v_dq4, total_rows, NULL, 0.005 FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-5a', 'missing tail, not cancelled', v_dq5a, not_cancelled_rows, 0, NULL FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-5b', 'missing tail, cancelled', v_dq5b, cancelled_rows, 0.20, NULL FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-6a', 'cancelled without code', v_dq6a, total_rows, NULL, NULL FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-6b', 'code without cancellation', v_dq6b, total_rows, NULL, 0.001 FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-7a', 'exact duplicate dropped', v_dq7a, total_rows, NULL, 0.0001 FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-7b', 'conflicting duplicate', v_dq7b, total_rows, NULL, 0.0001 FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-8a', 'delay not attributed', v_dq8a, delayed_rows, 0.005, NULL FROM per_month
    UNION ALL
    SELECT year, month, 'DQ-8b', 'cause sum mismatch', v_dq8b, delayed_rows, NULL, 0.001 FROM per_month
),
rated AS (
    SELECT *, violations::FLOAT / NULLIF(denominator, 0) AS rate FROM long
)
SELECT *,
       CASE WHEN rate > fail_threshold THEN 'FAIL'
            WHEN rate > warn_threshold THEN 'WARN'
            ELSE 'PASS' END AS status
FROM rated;

-- 4. 月度闸门：一个月只要有一条规则 FAIL，整月不进入分析层；WARN 只提醒，不拦截
CREATE VIEW dq.month_status AS
SELECT year, month,
       CASE MAX(CASE status WHEN 'FAIL' THEN 3 WHEN 'WARN' THEN 2 ELSE 1 END)
            WHEN 3 THEN 'FAIL' WHEN 2 THEN 'WARN' ELSE 'PASS' END AS month_status
FROM dq.dq_results
GROUP BY year, month;

-- 5. 干净数据：分析层（星型模型）从这里读
CREATE VIEW dq.flights_clean AS
SELECT f.*
FROM dq.flights_checked f
JOIN dq.month_status m ON f.year = m.year AND f.month = m.month
WHERE m.month_status <> 'FAIL'
  AND f.quarantine_reason = ''
  AND NOT f.dropped_row;
