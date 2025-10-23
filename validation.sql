/* =======================================================================
   validation.sql  —  Fitness & Nutrition Data Pipeline
   Purpose: Validate resulting data structures after ETL
   MySQL 8+
   ======================================================================= */

-- =========================
-- 0) CONFIG (edit if needed)
-- =========================
-- If you want to lock to a specific DB, uncomment and set it:
-- USE your_database_name;

-- Logical min/max bounds (tune for your dataset)
SET @MIN_HEIGHT = 1.20;     -- meters
SET @MAX_HEIGHT = 2.30;
SET @MIN_WEIGHT = 30;       -- kg
SET @MAX_WEIGHT = 250;
SET @MIN_BMI    = 10;
SET @MAX_BMI    = 60;
SET @MAX_WORKOUT_HOURS = 6;

-- Accepted domains
-- (You can turn these into reference dims; here they’re for quick checks.)
-- Gender: keep in sync with your Dim_User domain (if any)
-- Workout/Nutrition types are validated via FK joins, not hard-coded lists.

-- =========================
-- 1) Inventory & schema
-- =========================
SELECT 'TABLE INVENTORY' AS section, table_name, table_rows
FROM information_schema.tables
WHERE table_schema = DATABASE()
ORDER BY table_name;

-- Optional: flag expected tables that are missing
WITH expected AS (
  SELECT 'Dim_User' AS t UNION ALL
  SELECT 'Dim_Date' UNION ALL
  SELECT 'Dim_FoodItem' UNION ALL
  SELECT 'Dim_WorkoutType' UNION ALL
  SELECT 'Dim_MealType' UNION ALL
  SELECT 'Dim_MetricType' UNION ALL
  SELECT 'Dim_HealthCondition' UNION ALL
  SELECT 'Fact_UserSnapshot' UNION ALL
  SELECT 'Fact_WorkoutSession' UNION ALL
  SELECT 'Fact_NutritionLog' UNION ALL
  SELECT 'Fact_HealthMetric' UNION ALL
  SELECT 'Bridge_User_HealthCondition'
)
SELECT 'MISSING TABLES' AS section, e.t AS expected_table
FROM expected e
LEFT JOIN information_schema.tables t
  ON t.table_schema = DATABASE() AND t.table_name = e.t
WHERE t.table_name IS NULL;

-- =========================
-- 2) Primary key uniqueness
--    (edit PK column names if yours differ)
-- =========================
SELECT 'PK CHECK: Dim_User' AS check_name,
       COUNT(*) AS rows_total,
       COUNT(DISTINCT UserKey) AS distinct_pk,
       COUNT(*) - COUNT(DISTINCT UserKey) AS duplicate_pk
FROM Dim_User;

SELECT 'PK CHECK: Dim_Date', COUNT(*), COUNT(DISTINCT DateKey),
       COUNT(*) - COUNT(DISTINCT DateKey)
FROM Dim_Date;

SELECT 'PK CHECK: Dim_FoodItem', COUNT(*), COUNT(DISTINCT FoodKey),
       COUNT(*) - COUNT(DISTINCT FoodKey)
FROM Dim_FoodItem;

SELECT 'PK CHECK: Dim_WorkoutType', COUNT(*), COUNT(DISTINCT WorkoutTypeKey),
       COUNT(*) - COUNT(DISTINCT WorkoutTypeKey)
FROM Dim_WorkoutType;

SELECT 'PK CHECK: Dim_MealType', COUNT(*), COUNT(DISTINCT MealTypeKey),
       COUNT(*) - COUNT(DISTINCT MealTypeKey)
FROM Dim_MealType;

SELECT 'PK CHECK: Dim_MetricType', COUNT(*), COUNT(DISTINCT MetricTypeKey),
       COUNT(*) - COUNT(DISTINCT MetricTypeKey)
FROM Dim_MetricType;

SELECT 'PK CHECK: Dim_HealthCondition', COUNT(*), COUNT(DISTINCT ConditionKey),
       COUNT(*) - COUNT(DISTINCT ConditionKey)
FROM Dim_HealthCondition;

SELECT 'PK CHECK: Fact_UserSnapshot', COUNT(*), COUNT(DISTINCT SnapshotKey),
       COUNT(*) - COUNT(DISTINCT SnapshotKey)
FROM Fact_UserSnapshot;

SELECT 'PK CHECK: Fact_WorkoutSession', COUNT(*), COUNT(DISTINCT SessionKey),
       COUNT(*) - COUNT(DISTINCT SessionKey)
FROM Fact_WorkoutSession;

SELECT 'PK CHECK: Fact_NutritionLog', COUNT(*), COUNT(DISTINCT NutritionLogKey),
       COUNT(*) - COUNT(DISTINCT NutritionLogKey)
FROM Fact_NutritionLog;

SELECT 'PK CHECK: Fact_HealthMetric', COUNT(*), COUNT(DISTINCT HealthMetricKey),
       COUNT(*) - COUNT(DISTINCT HealthMetricKey)
FROM Fact_HealthMetric;

SELECT 'PK CHECK: Bridge_User_HealthCondition', COUNT(*), COUNT(DISTINCT CONCAT(UserKey,':',ConditionKey)),
       COUNT(*) - COUNT(DISTINCT CONCAT(UserKey,':',ConditionKey))
FROM Bridge_User_HealthCondition;

-- =========================
-- 3) Foreign key orphan checks
-- =========================
-- WorkoutSession must have User, Date, WorkoutType
SELECT 'ORPHANS: Fact_WorkoutSession -> Dim_User' AS check_name, COUNT(*) AS orphan_count
FROM Fact_WorkoutSession f
LEFT JOIN Dim_User u ON f.UserKey = u.UserKey
WHERE u.UserKey IS NULL;

SELECT 'ORPHANS: Fact_WorkoutSession -> Dim_Date', COUNT(*)
FROM Fact_WorkoutSession f
LEFT JOIN Dim_Date d ON f.DateKey = d.DateKey
WHERE d.DateKey IS NULL;

SELECT 'ORPHANS: Fact_WorkoutSession -> Dim_WorkoutType', COUNT(*)
FROM Fact_WorkoutSession f
LEFT JOIN Dim_WorkoutType wt ON f.WorkoutTypeKey = wt.WorkoutTypeKey
WHERE wt.WorkoutTypeKey IS NULL;

-- NutritionLog must have User, Date, Food, MealType
SELECT 'ORPHANS: Fact_NutritionLog -> Dim_User', COUNT(*)
FROM Fact_NutritionLog n
LEFT JOIN Dim_User u ON n.UserKey = u.UserKey
WHERE u.UserKey IS NULL;

SELECT 'ORPHANS: Fact_NutritionLog -> Dim_Date', COUNT(*)
FROM Fact_NutritionLog n
LEFT JOIN Dim_Date d ON n.DateKey = d.DateKey
WHERE d.DateKey IS NULL;

SELECT 'ORPHANS: Fact_NutritionLog -> Dim_FoodItem', COUNT(*)
FROM Fact_NutritionLog n
LEFT JOIN Dim_FoodItem fi ON n.FoodKey = fi.FoodKey
WHERE fi.FoodKey IS NULL;

SELECT 'ORPHANS: Fact_NutritionLog -> Dim_MealType', COUNT(*)
FROM Fact_NutritionLog n
LEFT JOIN Dim_MealType mt ON n.MealTypeKey = mt.MealTypeKey
WHERE mt.MealTypeKey IS NULL;

-- HealthMetric must have User, Date, MetricType
SELECT 'ORPHANS: Fact_HealthMetric -> Dim_User', COUNT(*)
FROM Fact_HealthMetric h
LEFT JOIN Dim_User u ON h.UserKey = u.UserKey
WHERE u.UserKey IS NULL;

SELECT 'ORPHANS: Fact_HealthMetric -> Dim_Date', COUNT(*)
FROM Fact_HealthMetric h
LEFT JOIN Dim_Date d ON h.DateKey = d.DateKey
WHERE d.DateKey IS NULL;

SELECT 'ORPHANS: Fact_HealthMetric -> Dim_MetricType', COUNT(*)
FROM Fact_HealthMetric h
LEFT JOIN Dim_MetricType mt ON h.MetricTypeKey = mt.MetricTypeKey
WHERE mt.MetricTypeKey IS NULL;

-- Bridge must have User and Condition
SELECT 'ORPHANS: Bridge_User_HealthCondition -> Dim_User', COUNT(*)
FROM Bridge_User_HealthCondition b
LEFT JOIN Dim_User u ON b.UserKey = u.UserKey
WHERE u.UserKey IS NULL;

SELECT 'ORPHANS: Bridge_User_HealthCondition -> Dim_HealthCondition', COUNT(*)
FROM Bridge_User_HealthCondition b
LEFT JOIN Dim_HealthCondition c ON b.ConditionKey = c.ConditionKey
WHERE c.ConditionKey IS NULL;

-- =========================
-- 4) Nullability (required columns)
-- =========================
SELECT 'NULL VIOL: Dim_User.UserKey' AS rule, COUNT(*) AS violations FROM Dim_User WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Dim_User.Age', COUNT(*) FROM Dim_User WHERE Age IS NULL;

SELECT 'NULL VIOL: Fact_WorkoutSession.UserKey', COUNT(*) FROM Fact_WorkoutSession WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Fact_WorkoutSession.DateKey', COUNT(*) FROM Fact_WorkoutSession WHERE DateKey IS NULL;
SELECT 'NULL VIOL: Fact_WorkoutSession.WorkoutTypeKey', COUNT(*) FROM Fact_WorkoutSession WHERE WorkoutTypeKey IS NULL;

SELECT 'NULL VIOL: Fact_NutritionLog.UserKey', COUNT(*) FROM Fact_NutritionLog WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Fact_NutritionLog.DateKey', COUNT(*) FROM Fact_NutritionLog WHERE DateKey IS NULL;
SELECT 'NULL VIOL: Fact_NutritionLog.FoodKey', COUNT(*) FROM Fact_NutritionLog WHERE FoodKey IS NULL;
SELECT 'NULL VIOL: Fact_NutritionLog.MealTypeKey', COUNT(*) FROM Fact_NutritionLog WHERE MealTypeKey IS NULL;
SELECT 'NULL VIOL: Fact_NutritionLog.ServingAmount', COUNT(*) FROM Fact_NutritionLog WHERE ServingAmount IS NULL;

SELECT 'NULL VIOL: Fact_HealthMetric.UserKey', COUNT(*) FROM Fact_HealthMetric WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Fact_HealthMetric.DateKey', COUNT(*) FROM Fact_HealthMetric WHERE DateKey IS NULL;
SELECT 'NULL VIOL: Fact_HealthMetric.MetricTypeKey', COUNT(*) FROM Fact_HealthMetric WHERE MetricTypeKey IS NULL;
SELECT 'NULL VIOL: Fact_HealthMetric.Value', COUNT(*) FROM Fact_HealthMetric WHERE Value IS NULL;

SELECT 'NULL VIOL: Fact_UserSnapshot.UserKey', COUNT(*) FROM Fact_UserSnapshot WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Fact_UserSnapshot.DateKey', COUNT(*) FROM Fact_UserSnapshot WHERE DateKey IS NULL;

-- =========================
-- 5) Domain & range validity
-- =========================
-- Gender domain (informative)
SELECT 'DOMAIN: Dim_User.Gender distinct' AS metric,
       GROUP_CONCAT(DISTINCT Gender ORDER BY Gender) AS distinct_values
FROM Dim_User;

-- Height/Weight/BMI bounds in snapshots (if columns exist)
-- (If your column names differ, adjust below)
SELECT 'RANGE: Height BETWEEN' AS rule, CONCAT(@MIN_HEIGHT,' AND ',@MAX_HEIGHT) AS bounds,
       SUM(NOT (Height BETWEEN @MIN_HEIGHT AND @MAX_HEIGHT)) AS violations
FROM Fact_UserSnapshot;

SELECT 'RANGE: Weight BETWEEN', CONCAT(@MIN_WEIGHT,' AND ',@MAX_WEIGHT),
       SUM(NOT (Weight BETWEEN @MIN_WEIGHT AND @MAX_WEIGHT)) AS violations
FROM Fact_UserSnapshot;

SELECT 'RANGE: BMI BETWEEN', CONCAT(@MIN_BMI,' AND ',@MAX_BMI),
       SUM(NOT (BMI BETWEEN @MIN_BMI AND @MAX_BMI)) AS violations
FROM Fact_UserSnapshot;

-- Workout duration/calories bounds (adjust names if needed)
SELECT 'RANGE: Fact_WorkoutSession.DurationHours 0..@MAX_WORKOUT_HOURS' AS rule,
       SUM(NOT (DurationHours BETWEEN 0 AND @MAX_WORKOUT_HOURS)) AS violations
FROM Fact_WorkoutSession;

SELECT 'RANGE: Fact_WorkoutSession.CaloriesBurned >= 0' AS rule,
       SUM(CASE WHEN CaloriesBurned < 0 THEN 1 ELSE 0 END) AS violations
FROM Fact_WorkoutSession;

-- =========================
-- 6) Cross-table consistency (coverage)
-- =========================
SELECT 'COVERAGE: Fact_WorkoutSession fully resolved (%)' AS metric,
       ROUND(100 * AVG(CASE WHEN u.UserKey IS NOT NULL AND d.DateKey IS NOT NULL AND wt.WorkoutTypeKey IS NOT NULL
                            THEN 1 ELSE 0 END), 2) AS pct_resolved
FROM Fact_WorkoutSession f
LEFT JOIN Dim_User u ON f.UserKey = u.UserKey
LEFT JOIN Dim_Date d ON f.DateKey = d.DateKey
LEFT JOIN Dim_WorkoutType wt ON f.WorkoutTypeKey = wt.WorkoutTypeKey;

SELECT 'COVERAGE: Fact_NutritionLog fully resolved (%)' AS metric,
       ROUND(100 * AVG(CASE WHEN u.UserKey IS NOT NULL AND d.DateKey IS NOT NULL
                               AND fi.FoodKey IS NOT NULL AND mt.MealTypeKey IS NOT NULL
                            THEN 1 ELSE 0 END), 2) AS pct_resolved
FROM Fact_NutritionLog n
LEFT JOIN Dim_User u ON n.UserKey = u.UserKey
LEFT JOIN Dim_Date d ON n.DateKey = d.DateKey
LEFT JOIN Dim_FoodItem fi ON n.FoodKey = fi.FoodKey
LEFT JOIN Dim_MealType mt ON n.MealTypeKey = mt.MealTypeKey;

SELECT 'COVERAGE: Fact_HealthMetric fully resolved (%)' AS metric,
       ROUND(100 * AVG(CASE WHEN u.UserKey IS NOT NULL AND d.DateKey IS NOT NULL AND mt.MetricTypeKey IS NOT NULL
                            THEN 1 ELSE 0 END), 2) AS pct_resolved
FROM Fact_HealthMetric h
LEFT JOIN Dim_User u ON h.UserKey = u.UserKey
LEFT JOIN Dim_Date d ON h.DateKey = d.DateKey
LEFT JOIN Dim_MetricType mt ON h.MetricTypeKey = mt.MetricTypeKey;

-- =========================
-- 7) Basic distributions (plausibility)
-- =========================
SELECT 'BMI stats' AS metric, MIN(BMI) min_bmi, AVG(BMI) avg_bmi, MAX(BMI) max_bmi
FROM Fact_UserSnapshot;

SELECT 'Steps stats' AS metric, MIN(TotalSteps) min_steps, AVG(TotalSteps) avg_steps, MAX(TotalSteps) max_steps
FROM Fact_WorkoutSession;

SELECT 'Calories stats' AS metric, MIN(CaloriesBurned) min_cal, AVG(CaloriesBurned) avg_cal, MAX(CaloriesBurned) max_cal
FROM Fact_WorkoutSession;

-- =========================
-- 8) Analytical smoke tests
-- =========================
-- Top 5 active users by calories in last 30 days
SELECT u.UserKey, SUM(f.CaloriesBurned) AS calories_30d
FROM Fact_WorkoutSession f
JOIN Dim_User u  ON f.UserKey = u.UserKey
JOIN Dim_Date d  ON f.DateKey = d.DateKey
WHERE d.FullDate >= (CURRENT_DATE - INTERVAL 30 DAY)
GROUP BY u.UserKey
ORDER BY calories_30d DESC
LIMIT 5;

-- Monthly avg intake per user (calories & protein)
SELECT u.UserKey, d.Year, d.Month,
       AVG(fi.Calories * n.ServingAmount) AS avg_daily_calories,
       AVG(COALESCE(fi.Protein_g,0) * n.ServingAmount) AS avg_daily_protein_g
FROM Fact_NutritionLog n
JOIN Dim_User u ON n.UserKey = u.UserKey
JOIN Dim_FoodItem fi ON n.FoodKey = fi.FoodKey
JOIN Dim_Date d ON n.DateKey = d.DateKey
GROUP BY u.UserKey, d.Year, d.Month
ORDER BY u.UserKey, d.Year, d.Month;

-- =========================
-- 9) Index & constraint sanity
-- =========================
SELECT 'INDEXES/CONSTRAINTS' AS section,
       table_name, index_name,
       GROUP_CONCAT(column_name ORDER BY seq_in_index) AS cols,
       non_unique
FROM information_schema.statistics
WHERE table_schema = DATABASE()
GROUP BY table_name, index_name, non_unique
ORDER BY table_name, index_name;

-- =========================
-- 10) Optional: materialize violations for report
-- =========================
-- Drop if re-running
DROP TABLE IF EXISTS validation_violations;

CREATE TABLE validation_violations (
  rule VARCHAR(200),
  table_name VARCHAR(100),
  context VARCHAR(200),
  violation_count INT
);

-- Example inserts (extend as needed)
INSERT INTO validation_violations
SELECT 'FK orphan: WorkoutSession->User', 'Fact_WorkoutSession', 'UserKey', COUNT(*)
FROM Fact_WorkoutSession f LEFT JOIN Dim_User u ON f.UserKey=u.UserKey
WHERE u.UserKey IS NULL;

INSERT INTO validation_violations
SELECT 'RANGE: BMI out of bounds', 'Fact_UserSnapshot',
       CONCAT(@MIN_BMI,'..',@MAX_BMI),
       COUNT(*) FROM Fact_UserSnapshot
WHERE NOT (BMI BETWEEN @MIN_BMI AND @MAX_BMI);

SELECT * FROM validation_violations WHERE violation_count > 0;

-- =========================
-- 11) Run summary
-- =========================
SELECT 'VALIDATION COMPLETE' AS status,
       NOW() AS finished_at,
       (SELECT COUNT(*) FROM validation_violations WHERE violation_count > 0) AS total_violations;
