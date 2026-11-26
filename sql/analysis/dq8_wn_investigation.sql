-- dq8_wn_investigation.sql
-- 问题：全量运行时 DQ-8（延误原因加总）隔离了 148 行，而 2025-12 单月测试时是 0 行。
-- 结论（2026-11-25）：
--   * 148 行中 147 行来自 Southwest (WN)，1 行来自 Frontier (F9)。
--   * WN 的 147 行：arrdel15 = 1、arrdelayminutes 在 100 分钟以上，但五项延误原因全部为 0。
--     即延误已上报，但没有归因（用 0 占位），不是加总算错。
--   * 分布：2025-09 到 11 月 1/2/11 行，2025-12 到 2026-02 为 0，2026-03 起每月 18-29 行。
--   * 按 WN 延误航班数归一化后，每月只有 0.005%-0.085%（2026-08 最高：29 / 34,312），
--     从 3 月起大致稳定。原始计数的上升主要来自夏季延误航班增多。
--   * F9 的 1 行（2026-07）是缺少一项原因（NULL），属于真正的数据缺失。
-- 决定：把 DQ-8 拆成两条
--   DQ-8a 延误未归因（五项全为 0）→ 标记 dq_unattributed_delay，保留在延误指标里，
--         只在按原因拆分的分析中排除。WARN 阈值 0.5%（约为当前最大值的 6 倍）。
--   DQ-8b 原因缺失或非零加总不一致 → 仍然隔离，FAIL 阈值 0.1%。
-- 理由：缺信息标记、信息矛盾隔离。若隔离这 147 行，会删掉一批最长的真实延误，
--       让准点表现看起来比实际更好。

-- 1. 148 行按月份、问题类型分布（拆分前，在 02_dq_pipeline.sql 的 flagged CTE 结尾运行）
SELECT year, month,
       arrdel15,
       CASE WHEN carrierdelay IS NULL OR weatherdelay IS NULL OR nasdelay IS NULL
              OR securitydelay IS NULL OR lateaircraftdelay IS NULL THEN 'missing cause'
            ELSE 'sum mismatch' END AS problem,
       COUNT(*) AS n
FROM flagged
WHERE quarantine_reason LIKE '%DQ-8;%'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;

-- 2. 抽查一个月的原始值：五项原因全为 0，arrdelayminutes 为 106-374
SELECT year, month, arrdel15,
       carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay, arrdelayminutes
FROM flagged
WHERE quarantine_reason LIKE '%DQ-8;%'
  AND year = 2026 AND month = 8;

-- 3. 按航空公司：WN 147，F9 1
SELECT reporting_airline, COUNT(*)
FROM flagged
WHERE quarantine_reason LIKE '%DQ-8;%'
GROUP BY 1 ORDER BY 2 DESC;

-- 4. 归一化：WN 每月受影响的比例
SELECT year, month,
       SUM(CASE WHEN quarantine_reason LIKE '%DQ-8;%' THEN 1 ELSE 0 END) AS dq8_rows,
       SUM(CASE WHEN cancelled = 0 AND diverted = 0 AND arrdel15 = 1 THEN 1 ELSE 0 END) AS delayed_flights
FROM flagged
WHERE reporting_airline = 'WN'
GROUP BY 1, 2 ORDER BY 1, 2;

-- 5. 拆分后的复查（可直接在 dq.flights_checked 上运行）：8a = 147，8b = 1
SELECT reporting_airline,
       SUM(CASE WHEN dq_unattributed_delay THEN 1 ELSE 0 END)            AS dq8a_flagged,
       SUM(CASE WHEN quarantine_reason LIKE '%DQ-8;%' THEN 1 ELSE 0 END) AS dq8b_quarantined
FROM dq.flights_checked
WHERE dq_unattributed_delay OR quarantine_reason LIKE '%DQ-8;%'
GROUP BY 1;
