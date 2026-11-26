-- 03_dq_checks.sql
-- 用途：验证 02_dq_pipeline.sql 的输出。只读，不建任何对象，可以随时运行。
-- 用法：每次跑完 pipeline 后执行。结果应该全部是 PASS。
--
-- 两类检查：
--   invariant  不变量：无论数据是什么都必须成立。失败 = pipeline 有 bug。
--   regression 回归：当前 12 个月数据（2025-09 到 2026-08）的已知结果。
--              加载新月份后，这些预期值要有意识地更新。
--
-- 某条规则在 dq_results 里不存在时，actual 是 NULL，状态会显示 FAIL（不会误判为 PASS）。

WITH
staging_counts AS (
    SELECT year, month, COUNT(*) AS n
    FROM staging.flights
    GROUP BY year, month
),
checked_counts AS (
    SELECT year, month,
           COUNT(*) AS n,
           SUM(CASE WHEN dropped_row THEN 1 ELSE 0 END) AS dropped,
           SUM(CASE WHEN NOT dropped_row AND quarantine_reason <> '' THEN 1 ELSE 0 END) AS quarantined,
           SUM(CASE WHEN NOT dropped_row AND quarantine_reason =  '' THEN 1 ELSE 0 END) AS clean
    FROM dq.flights_checked
    GROUP BY year, month
),
rule_totals AS (
    SELECT rule_id, SUM(violations) AS v
    FROM dq.dq_results
    GROUP BY rule_id
),
checks AS (

    -- ================= 不变量 =================

    -- 1. 每月行数：flights_checked 必须等于 staging（actual = 对不上的月份数）
    SELECT 1                                                    AS check_no,
           'invariant'::VARCHAR(20)                             AS check_type,
           'rows per month: flights_checked = staging'::VARCHAR(100) AS check_name,
           0::BIGINT                                            AS expected,
           COUNT(*)::BIGINT                                     AS actual
    FROM staging_counts s
    FULL OUTER JOIN checked_counts c ON s.year = c.year AND s.month = c.month
    WHERE s.n IS NULL OR c.n IS NULL OR s.n <> c.n

    UNION ALL
    -- 2. 标记列和原因列不能有 NULL（NULL 会让行从 flights_clean 里悄悄消失）
    SELECT 2, 'invariant', 'no NULL in flag or reason columns', 0,
           COUNT(*)
    FROM dq.flights_checked
    WHERE quarantine_reason IS NULL
       OR dropped_row IS NULL
       OR dq_cancelled_with_deptime IS NULL
       OR dq_extreme_value IS NULL
       OR dq_missing_tail_number IS NULL
       OR dq_cancelled_without_code IS NULL
       OR dq_unattributed_delay IS NULL

    UNION ALL
    -- 3. 每月互斥分类：dropped + quarantined + clean = 总行数（actual = 对不上的月份数）
    SELECT 3, 'invariant', 'dropped + quarantined + clean = rows, per month', 0,
           COUNT(*)
    FROM checked_counts
    WHERE dropped + quarantined + clean <> n

    UNION ALL
    -- 4. dq.quarantine 必须正好是 flights_checked 里有隔离原因的行
    SELECT 4, 'invariant', 'quarantine table = rows with a reason', 0,
           q.n - c.n
    FROM (SELECT COUNT(*) AS n FROM dq.quarantine) q
    CROSS JOIN (SELECT COUNT(*) AS n FROM dq.flights_checked WHERE quarantine_reason <> '') c

    UNION ALL
    -- 5. flights_clean 里不能有：隔离行、被删除的重复行、FAIL 月份的行
    SELECT 5, 'invariant', 'flights_clean has no quarantined, dropped or FAIL-month rows', 0,
           COUNT(*)
    FROM dq.flights_clean f
    JOIN dq.month_status m ON f.year = m.year AND f.month = m.month
    WHERE f.quarantine_reason <> ''
       OR f.dropped_row
       OR m.month_status = 'FAIL'

    UNION ALL
    -- 6. dq_results 完整：每个月 13 条规则各一行
    --    DQ-1, 2, 3a, 3b, 4, 5a, 5b, 6a, 6b, 7a, 7b, 8a, 8b
    SELECT 6, 'invariant', 'dq_results rows = months x 13 rules',
           m.n * 13,
           r.n
    FROM (SELECT COUNT(*) AS n FROM checked_counts) m
    CROSS JOIN (SELECT COUNT(*) AS n FROM dq.dq_results) r

    -- ================= 回归（当前 12 个月的已知结果） =================

    UNION ALL SELECT 11, 'regression', 'DQ-1 invalid crsdeptime',        0,     SUM(v) FROM rule_totals WHERE rule_id = 'DQ-1'
    UNION ALL SELECT 12, 'regression', 'DQ-2 cancelled with deptime',    5422,  SUM(v) FROM rule_totals WHERE rule_id = 'DQ-2'
    UNION ALL SELECT 13, 'regression', 'DQ-3a extreme value',            69319, SUM(v) FROM rule_totals WHERE rule_id = 'DQ-3a'
    UNION ALL SELECT 14, 'regression', 'DQ-3b negative delay cause',     0,     SUM(v) FROM rule_totals WHERE rule_id = 'DQ-3b'
    UNION ALL SELECT 15, 'regression', 'DQ-4 missing required field',    1,     SUM(v) FROM rule_totals WHERE rule_id = 'DQ-4'    -- 2025-09, YX 5859
    UNION ALL SELECT 16, 'regression', 'DQ-5a + DQ-5b missing tail',     18077, SUM(v) FROM rule_totals WHERE rule_id IN ('DQ-5a', 'DQ-5b')
    UNION ALL SELECT 17, 'regression', 'DQ-6a cancelled without code',   0,     SUM(v) FROM rule_totals WHERE rule_id = 'DQ-6a'
    UNION ALL SELECT 18, 'regression', 'DQ-6b code without cancellation',0,     SUM(v) FROM rule_totals WHERE rule_id = 'DQ-6b'
    UNION ALL SELECT 19, 'regression', 'DQ-7a exact duplicate dropped',  0,     SUM(v) FROM rule_totals WHERE rule_id = 'DQ-7a'
    UNION ALL SELECT 20, 'regression', 'DQ-7b conflicting duplicate',    0,     SUM(v) FROM rule_totals WHERE rule_id = 'DQ-7b'
    UNION ALL SELECT 21, 'regression', 'DQ-8a delay not attributed',     147,   SUM(v) FROM rule_totals WHERE rule_id = 'DQ-8a'   -- 全部是 WN，见 analysis/dq8_wn_investigation.sql
    UNION ALL SELECT 22, 'regression', 'DQ-8b cause sum mismatch',       1,     SUM(v) FROM rule_totals WHERE rule_id = 'DQ-8b'   -- F9，2026-07，缺一项原因

    UNION ALL
    -- 23. 进入分析层的总行数
    SELECT 23, 'regression', 'flights_clean rows', 7049867,
           COUNT(*)
    FROM dq.flights_clean

    UNION ALL
    -- 24. 非 PASS 的月份数
    --     已知：2026-02 WARN（DQ-5b 21.5%），原因是 2/22-24 东北部暴风雪期间航空公司提前取消航班，
    --     已调查，不改阈值。见 analysis/dq5_feb2026_warn.sql
    SELECT 24, 'regression', 'months not PASS (known: 2026-02 WARN)', 1,
           COUNT(*)
    FROM dq.month_status
    WHERE month_status <> 'PASS'
)
SELECT check_no, check_type, check_name, expected, actual,
       CASE WHEN expected = actual THEN 'PASS' ELSE 'FAIL' END AS status
FROM checks
ORDER BY check_no;


-- ---------------------------------------------------------------
-- 有 FAIL 时用下面的查询看细节
-- ---------------------------------------------------------------

-- 非 PASS 的规则和月份
SELECT year, month, rule_id, check_name, violations, denominator, rate,
       warn_threshold, fail_threshold, status
FROM dq.dq_results
WHERE status <> 'PASS'
ORDER BY year, month, rule_id;

-- 每月对账明细
SELECT s.year, s.month, s.n AS staging_rows, c.n AS checked_rows,
       c.dropped, c.quarantined, c.clean
FROM (SELECT year, month, COUNT(*) AS n FROM staging.flights GROUP BY 1, 2) s
FULL OUTER JOIN (
    SELECT year, month, COUNT(*) AS n,
           SUM(CASE WHEN dropped_row THEN 1 ELSE 0 END) AS dropped,
           SUM(CASE WHEN NOT dropped_row AND quarantine_reason <> '' THEN 1 ELSE 0 END) AS quarantined,
           SUM(CASE WHEN NOT dropped_row AND quarantine_reason =  '' THEN 1 ELSE 0 END) AS clean
    FROM dq.flights_checked GROUP BY 1, 2
) c ON s.year = c.year AND s.month = c.month
ORDER BY 1, 2;
