-- dq5_feb2026_warn.sql
-- 问题：dq.month_status 显示 2026-02 为 WARN。
--       原因是 DQ-5b（取消航班缺尾号）：2,497 / 11,605 = 21.5%，超过 20% 的警告线。
-- 结论（2026-11-25）：
--   * 2 月 22-24 日 3 天占全月取消的 66%（7,711 / 11,605），占缺尾号取消的 87%（2,163 / 2,497）。
--   * 去掉这 3 天，其余 25 天的比例为 8.6%（334 / 3,894），与 2025-12 的 9.5% 接近。
--   * 这 3 天是美国东北部暴风雪（2026-02-22 至 24）。航空公司在风暴前一天批量提前取消航班，
--     当时还没有分配飞机，所以没有尾号。
--     支持这一解释的细节：23 日（大多前一天取消）缺尾号比例 33.8%，
--     高于 22 日（较多当天取消）的 19.7%。
--   * 外部来源：Aviation Week, "U.S. Airlines Ground Flights Amid Northeast Blizzard";
--     Skift, "Airlines Cancel 10,000+ Flights as Blizzard Slams Northeast" (2026-02-23)
-- 决定：规则正常工作，不修改阈值。WARN 不拦截数据，2 月照常进入 flights_clean。
--       若因一次有合理解释的 WARN 放宽阈值，下一次真正的数据问题可能会被漏掉。

-- 1. 2 月按天：取消航班数、其中缺尾号的数量和比例
SELECT flightdate,
       SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled,
       SUM(CASE WHEN cancelled = 1 AND dq_missing_tail_number THEN 1 ELSE 0 END) AS cancelled_no_tail,
       SUM(CASE WHEN cancelled = 1 AND dq_missing_tail_number THEN 1 ELSE 0 END)::FLOAT
         / NULLIF(SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END), 0) AS no_tail_rate
FROM dq.flights_checked
WHERE year = 2026 AND month = 2
GROUP BY 1
ORDER BY cancelled DESC
LIMIT 10;

-- 2. 风暴 3 天与其余天数对比
SELECT CASE WHEN flightdate BETWEEN DATE '2026-02-22' AND DATE '2026-02-24'
            THEN 'storm 2/22-24' ELSE 'other days' END AS period,
       SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled,
       SUM(CASE WHEN cancelled = 1 AND dq_missing_tail_number THEN 1 ELSE 0 END) AS cancelled_no_tail,
       SUM(CASE WHEN cancelled = 1 AND dq_missing_tail_number THEN 1 ELSE 0 END)::FLOAT
         / NULLIF(SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END), 0) AS no_tail_rate
FROM dq.flights_checked
WHERE year = 2026 AND month = 2
GROUP BY 1;
