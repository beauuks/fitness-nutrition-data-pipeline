/* validation.sql  —  Fitness & Nutrition Data Pipeline
   Purpose: Validate resulting data structures after ETL*/

-- USE fitness_nutrition_dw;

-- Logical min/max bounds (tune for your dataset)
SET @MIN_HEIGHT = 1.20;     -- meters
SET @MAX_HEIGHT = 2.30;
SET @MIN_WEIGHT = 30;       -- kg
SET @MAX_WEIGHT = 250;
SET @MIN_BMI    = 10;
SET @MAX_BMI    = 60;
SET @MAX_WORKOUT_HOURS = 18;

-- 1) Inventory & schema
SELECT 'TABLE INVENTORY' AS section, table_name, table_rows
FROM information_schema.tables
WHERE table_schema = DATABASE()
ORDER BY table_name;

-- flag expected tables that are missing
WITH expected AS (
  SELECT 'Dim_User' AS t UNION ALL
  SELECT 'Dim_Date' UNION ALL
  SELECT 'Dim_FoodItem' UNION ALL
  SELECT 'Dim_WorkoutType' UNION ALL
  SELECT 'Dim_MealType' UNION ALL
  SELECT 'Dim_MetricType' UNION ALL
  SELECT 'Dim_HealthCondition' UNION ALL
  SELECT 'Dim_FitnessGoal' UNION ALL
  SELECT 'Dim_FitnessType' UNION ALL
  SELECT 'Dim_Exercise' UNION ALL
  SELECT 'Dim_Diet' UNION ALL
  SELECT 'Fact_UserSnapshot' UNION ALL
  SELECT 'Fact_WorkoutSession' UNION ALL
  SELECT 'Fact_NutritionLog' UNION ALL
  SELECT 'Fact_HealthMetric' UNION ALL
  SELECT 'Bridge_User_HealthCondition' UNION ALL
  SELECT 'Bridge_User_WorkoutPreference' UNION ALL
  SELECT 'Bridge_User_DietPreference'
)
SELECT 'MISSING TABLES' AS section, e.t AS expected_table
FROM expected e
LEFT JOIN information_schema.tables t
  ON t.table_schema = DATABASE() AND t.table_name = e.t
WHERE t.table_name IS NULL;

-- 2) Primary key uniqueness
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

SELECT 'PK CHECK: Dim_FitnessGoal', COUNT(*), COUNT(DISTINCT GoalKey),
       COUNT(*) - COUNT(DISTINCT GoalKey)
FROM Dim_FitnessGoal;

SELECT 'PK CHECK: Dim_FitnessType', COUNT(*), COUNT(DISTINCT TypeKey),
       COUNT(*) - COUNT(DISTINCT TypeKey)
FROM Dim_FitnessType;

SELECT 'PK CHECK: Dim_Exercise', COUNT(*), COUNT(DISTINCT ExerciseKey),
       COUNT(*) - COUNT(DISTINCT ExerciseKey)
FROM Dim_Exercise;

SELECT 'PK CHECK: Dim_Diet', COUNT(*), COUNT(DISTINCT DietKey),
       COUNT(*) - COUNT(DISTINCT DietKey)
FROM Dim_Diet;

SELECT 'PK CHECK: Fact_UserSnapshot', COUNT(*), COUNT(DISTINCT SnapshotKey),
       COUNT(*) - COUNT(DISTINCT SnapshotKey)
FROM Fact_UserSnapshot;

SELECT 'PK CHECK: Fact_WorkoutSession', COUNT(*), COUNT(DISTINCT SessionKey),
       COUNT(*) - COUNT(DISTINCT SessionKey)
FROM Fact_WorkoutSession;

SELECT 'PK CHECK: Fact_NutritionLog', COUNT(*), COUNT(DISTINCT LogKey),
       COUNT(*) - COUNT(DISTINCT LogKey)
FROM Fact_NutritionLog;

SELECT 'PK CHECK: Fact_HealthMetric', COUNT(*), COUNT(DISTINCT MetricKey),
       COUNT(*) - COUNT(DISTINCT MetricKey)
FROM Fact_HealthMetric;

SELECT 'PK CHECK: Bridge_User_HealthCondition', COUNT(*), COUNT(DISTINCT CONCAT(UserKey,':',ConditionKey)),
       COUNT(*) - COUNT(DISTINCT CONCAT(UserKey,':',ConditionKey))
FROM Bridge_User_HealthCondition;

SELECT 'PK CHECK: Bridge_User_WorkoutPreference', COUNT(*), COUNT(DISTINCT CONCAT(UserKey,':',ExerciseKey)),
       COUNT(*) - COUNT(DISTINCT CONCAT(UserKey,':',ExerciseKey))
FROM Bridge_User_WorkoutPreference;

SELECT 'PK CHECK: Bridge_User_DietPreference', COUNT(*), COUNT(DISTINCT CONCAT(UserKey,':',DietKey)),
       COUNT(*) - COUNT(DISTINCT CONCAT(UserKey,':',DietKey))
FROM Bridge_User_DietPreference;


-- 3) Foreign key orphan checks
-- Fact_UserSnapshot must have User, Goal, and optionally Type
SELECT 'ORPHANS: Fact_UserSnapshot -> Dim_User' AS check_name, COUNT(*) AS orphan_count
FROM Fact_UserSnapshot f
LEFT JOIN Dim_User u ON f.UserKey = u.UserKey
WHERE u.UserKey IS NULL;

SELECT 'ORPHANS: Fact_UserSnapshot -> Dim_FitnessGoal', COUNT(*)
FROM Fact_UserSnapshot f
LEFT JOIN Dim_FitnessGoal g ON f.GoalKey = g.GoalKey
WHERE g.GoalKey IS NULL;

SELECT 'ORPHANS: Fact_UserSnapshot -> Dim_FitnessType', COUNT(*)
FROM Fact_UserSnapshot f
LEFT JOIN Dim_FitnessType ft ON f.TypeKey = ft.TypeKey
WHERE f.TypeKey IS NOT NULL AND ft.TypeKey IS NULL;

-- WorkoutSession must have User, Date, WorkoutType
SELECT 'ORPHANS: Fact_WorkoutSession -> Dim_User', COUNT(*)
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
WHERE f.WorkoutTypeKey IS NOT NULL AND wt.WorkoutTypeKey IS NULL;

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

-- Bridge tables orphan checks
SELECT 'ORPHANS: Bridge_User_HealthCondition -> Dim_User', COUNT(*)
FROM Bridge_User_HealthCondition b
LEFT JOIN Dim_User u ON b.UserKey = u.UserKey
WHERE u.UserKey IS NULL;

SELECT 'ORPHANS: Bridge_User_HealthCondition -> Dim_HealthCondition', COUNT(*)
FROM Bridge_User_HealthCondition b
LEFT JOIN Dim_HealthCondition c ON b.ConditionKey = c.ConditionKey
WHERE c.ConditionKey IS NULL;

SELECT 'ORPHANS: Bridge_User_WorkoutPreference -> Dim_User', COUNT(*)
FROM Bridge_User_WorkoutPreference b
LEFT JOIN Dim_User u ON b.UserKey = u.UserKey
WHERE u.UserKey IS NULL;

SELECT 'ORPHANS: Bridge_User_WorkoutPreference -> Dim_Exercise', COUNT(*)
FROM Bridge_User_WorkoutPreference b
LEFT JOIN Dim_Exercise e ON b.ExerciseKey = e.ExerciseKey
WHERE e.ExerciseKey IS NULL;

SELECT 'ORPHANS: Bridge_User_DietPreference -> Dim_User', COUNT(*)
FROM Bridge_User_DietPreference b
LEFT JOIN Dim_User u ON b.UserKey = u.UserKey
WHERE u.UserKey IS NULL;

SELECT 'ORPHANS: Bridge_User_DietPreference -> Dim_Diet', COUNT(*)
FROM Bridge_User_DietPreference b
LEFT JOIN Dim_Diet d ON b.DietKey = d.DietKey
WHERE d.DietKey IS NULL;


-- 4) Nullability (required columns)
SELECT 'NULL VIOL: Dim_User.UserKey' AS rule, COUNT(*) AS violations FROM Dim_User WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Dim_User.Source', COUNT(*) FROM Dim_User WHERE Source IS NULL;

SELECT 'NULL VIOL: Fact_UserSnapshot.UserKey', COUNT(*) FROM Fact_UserSnapshot WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Fact_UserSnapshot.GoalKey', COUNT(*) FROM Fact_UserSnapshot WHERE GoalKey IS NULL;

SELECT 'NULL VIOL: Fact_WorkoutSession.UserKey', COUNT(*) FROM Fact_WorkoutSession WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Fact_WorkoutSession.DateKey', COUNT(*) FROM Fact_WorkoutSession WHERE DateKey IS NULL;

SELECT 'NULL VIOL: Fact_NutritionLog.UserKey', COUNT(*) FROM Fact_NutritionLog WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Fact_NutritionLog.DateKey', COUNT(*) FROM Fact_NutritionLog WHERE DateKey IS NULL;
SELECT 'NULL VIOL: Fact_NutritionLog.FoodKey', COUNT(*) FROM Fact_NutritionLog WHERE FoodKey IS NULL;
SELECT 'NULL VIOL: Fact_NutritionLog.MealTypeKey', COUNT(*) FROM Fact_NutritionLog WHERE MealTypeKey IS NULL;

SELECT 'NULL VIOL: Fact_HealthMetric.UserKey', COUNT(*) FROM Fact_HealthMetric WHERE UserKey IS NULL;
SELECT 'NULL VIOL: Fact_HealthMetric.DateKey', COUNT(*) FROM Fact_HealthMetric WHERE DateKey IS NULL;
SELECT 'NULL VIOL: Fact_HealthMetric.MetricTypeKey', COUNT(*) FROM Fact_HealthMetric WHERE MetricTypeKey IS NULL;
SELECT 'NULL VIOL: Fact_HealthMetric.Value', COUNT(*) FROM Fact_HealthMetric WHERE Value IS NULL;


-- 5) Domain & range validity
-- Gender domain (informative)
SELECT 'DOMAIN: Dim_User.Gender distinct' AS metric,
       GROUP_CONCAT(DISTINCT Gender ORDER BY Gender) AS distinct_values
FROM Dim_User;

-- Source domain (informative)
SELECT 'DOMAIN: Dim_User.Source distinct' AS metric,
       GROUP_CONCAT(DISTINCT Source ORDER BY Source) AS distinct_values
FROM Dim_User;

-- Height/Weight/BMI bounds in snapshots
SELECT 'RANGE: Height BETWEEN' AS rule, CONCAT(@MIN_HEIGHT,' AND ',@MAX_HEIGHT) AS bounds,
       SUM(CASE WHEN Height IS NOT NULL AND NOT (Height BETWEEN @MIN_HEIGHT AND @MAX_HEIGHT) THEN 1 ELSE 0 END) AS violations
FROM Fact_UserSnapshot;

SELECT 'RANGE: Weight BETWEEN', CONCAT(@MIN_WEIGHT,' AND ',@MAX_WEIGHT),
       SUM(CASE WHEN Weight IS NOT NULL AND NOT (Weight BETWEEN @MIN_WEIGHT AND @MAX_WEIGHT) THEN 1 ELSE 0 END) AS violations
FROM Fact_UserSnapshot;

SELECT 'RANGE: BMI BETWEEN', CONCAT(@MIN_BMI,' AND ',@MAX_BMI),
       SUM(CASE WHEN BMI IS NOT NULL AND NOT (BMI BETWEEN @MIN_BMI AND @MAX_BMI) THEN 1 ELSE 0 END) AS violations
FROM Fact_UserSnapshot;

-- Workout duration/calories bounds
SELECT 'RANGE: Fact_WorkoutSession.DurationHours 0..@MAX_WORKOUT_HOURS' AS rule,
       SUM(CASE WHEN DurationHours IS NOT NULL AND NOT (DurationHours BETWEEN 0 AND @MAX_WORKOUT_HOURS) THEN 1 ELSE 0 END) AS violations
FROM Fact_WorkoutSession;

SELECT 'RANGE: Fact_WorkoutSession.CaloriesBurned >= 0' AS rule,
       SUM(CASE WHEN CaloriesBurned IS NOT NULL AND CaloriesBurned < 0 THEN 1 ELSE 0 END) AS violations
FROM Fact_WorkoutSession;

SELECT 'RANGE: Fact_WorkoutSession.TotalSteps >= 0' AS rule,
       SUM(CASE WHEN TotalSteps IS NOT NULL AND TotalSteps < 0 THEN 1 ELSE 0 END) AS violations
FROM Fact_WorkoutSession;

-- Nutrition log validation
SELECT 'RANGE: Fact_NutritionLog.ServingSize > 0' AS rule,
       SUM(CASE WHEN ServingSize IS NOT NULL AND ServingSize <= 0 THEN 1 ELSE 0 END) AS violations
FROM Fact_NutritionLog;

SELECT 'RANGE: Fact_NutritionLog.TotalCalories >= 0' AS rule,
       SUM(CASE WHEN TotalCalories IS NOT NULL AND TotalCalories < 0 THEN 1 ELSE 0 END) AS violations
FROM Fact_NutritionLog;


-- 6) Cross-table consistency (coverage)
SELECT 'COVERAGE: Fact_UserSnapshot fully resolved (%)' AS metric,
       ROUND(100 * AVG(CASE WHEN u.UserKey IS NOT NULL AND g.GoalKey IS NOT NULL
                            THEN 1 ELSE 0 END), 2) AS pct_resolved
FROM Fact_UserSnapshot f
LEFT JOIN Dim_User u ON f.UserKey = u.UserKey
LEFT JOIN Dim_FitnessGoal g ON f.GoalKey = g.GoalKey;

SELECT 'COVERAGE: Fact_WorkoutSession fully resolved (%)' AS metric,
       ROUND(100 * AVG(CASE WHEN u.UserKey IS NOT NULL AND d.DateKey IS NOT NULL
                            THEN 1 ELSE 0 END), 2) AS pct_resolved
FROM Fact_WorkoutSession f
LEFT JOIN Dim_User u ON f.UserKey = u.UserKey
LEFT JOIN Dim_Date d ON f.DateKey = d.DateKey;

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


-- 7) Basic distributions (plausibility)
SELECT 'BMI stats' AS metric, 
       MIN(BMI) min_bmi, 
       AVG(BMI) avg_bmi, 
       MAX(BMI) max_bmi,
       COUNT(*) total_records
FROM Fact_UserSnapshot
WHERE BMI IS NOT NULL;

SELECT 'Steps stats' AS metric, 
       MIN(TotalSteps) min_steps, 
       AVG(TotalSteps) avg_steps, 
       MAX(TotalSteps) max_steps,
       COUNT(*) total_records
FROM Fact_WorkoutSession
WHERE TotalSteps IS NOT NULL;

SELECT 'Calories burned stats' AS metric, 
       MIN(CaloriesBurned) min_cal, 
       AVG(CaloriesBurned) avg_cal, 
       MAX(CaloriesBurned) max_cal,
       COUNT(*) total_records
FROM Fact_WorkoutSession
WHERE CaloriesBurned IS NOT NULL;

SELECT 'Nutrition calories stats' AS metric,
       MIN(TotalCalories) min_cal,
       AVG(TotalCalories) avg_cal,
       MAX(TotalCalories) max_cal,
       COUNT(*) total_records
FROM Fact_NutritionLog
WHERE TotalCalories IS NOT NULL;


-- 8) Analytical smoke tests
-- Top 5 active users by calories in last 30 days
SELECT 'TOP 5 USERS BY CALORIES (Last 30 days)' AS analysis;
SELECT u.UserKey, u.Source, SUM(f.CaloriesBurned) AS calories_30d
FROM Fact_WorkoutSession f
JOIN Dim_User u  ON f.UserKey = u.UserKey
JOIN Dim_Date d  ON f.DateKey = d.DateKey
WHERE d.FullDate >= (CURRENT_DATE - INTERVAL 30 DAY)
GROUP BY u.UserKey, u.Source
ORDER BY calories_30d DESC
LIMIT 5;

-- Monthly avg intake per user (calories & protein)
SELECT 'MONTHLY NUTRITION AVERAGES' AS analysis;
SELECT u.UserKey, d.Year, d.Month,
       AVG(n.TotalCalories) AS avg_daily_calories,
       AVG(n.TotalProtein) AS avg_daily_protein_g,
       COUNT(*) AS log_entries
FROM Fact_NutritionLog n
JOIN Dim_User u ON n.UserKey = u.UserKey
JOIN Dim_Date d ON n.DateKey = d.DateKey
GROUP BY u.UserKey, d.Year, d.Month
ORDER BY u.UserKey, d.Year, d.Month
LIMIT 10;

-- Goal distribution
SELECT 'GOAL DISTRIBUTION' AS analysis;
SELECT g.GoalName, COUNT(*) AS user_count
FROM Fact_UserSnapshot s
JOIN Dim_FitnessGoal g ON s.GoalKey = g.GoalKey
GROUP BY g.GoalName
ORDER BY user_count DESC;


-- 9) Index & constraint sanity
SELECT 'INDEXES/CONSTRAINTS' AS section,
       table_name, index_name,
       GROUP_CONCAT(column_name ORDER BY seq_in_index) AS cols,
       non_unique
FROM information_schema.statistics
WHERE table_schema = DATABASE()
GROUP BY table_name, index_name, non_unique
ORDER BY table_name, index_name;


-- 10) materialize violations for report
-- Drop if re-running
DROP TABLE IF EXISTS validation_violations;

CREATE TABLE validation_violations (
  rule VARCHAR(200),
  table_name VARCHAR(100),
  context VARCHAR(200),
  violation_count INT
);

-- Example inserts for common violations
INSERT INTO validation_violations
SELECT 'FK orphan: WorkoutSession->User', 'Fact_WorkoutSession', 'UserKey', COUNT(*)
FROM Fact_WorkoutSession f LEFT JOIN Dim_User u ON f.UserKey=u.UserKey
WHERE u.UserKey IS NULL;

INSERT INTO validation_violations
SELECT 'FK orphan: NutritionLog->User', 'Fact_NutritionLog', 'UserKey', COUNT(*)
FROM Fact_NutritionLog f LEFT JOIN Dim_User u ON f.UserKey=u.UserKey
WHERE u.UserKey IS NULL;

INSERT INTO validation_violations
SELECT 'FK orphan: HealthMetric->User', 'Fact_HealthMetric', 'UserKey', COUNT(*)
FROM Fact_HealthMetric f LEFT JOIN Dim_User u ON f.UserKey=u.UserKey
WHERE u.UserKey IS NULL;

INSERT INTO validation_violations
SELECT 'FK orphan: UserSnapshot->FitnessGoal', 'Fact_UserSnapshot', 'GoalKey', COUNT(*)
FROM Fact_UserSnapshot f LEFT JOIN Dim_FitnessGoal g ON f.GoalKey=g.GoalKey
WHERE g.GoalKey IS NULL;

INSERT INTO validation_violations
SELECT 'RANGE: BMI out of bounds', 'Fact_UserSnapshot',
       CONCAT(@MIN_BMI,'..',@MAX_BMI),
       COUNT(*) FROM Fact_UserSnapshot
WHERE BMI IS NOT NULL AND NOT (BMI BETWEEN @MIN_BMI AND @MAX_BMI);

INSERT INTO validation_violations
SELECT 'RANGE: Negative calories', 'Fact_WorkoutSession', 'CaloriesBurned',
       COUNT(*) FROM Fact_WorkoutSession
WHERE CaloriesBurned IS NOT NULL AND CaloriesBurned < 0;

INSERT INTO validation_violations
SELECT 'RANGE: Negative serving size', 'Fact_NutritionLog', 'ServingSize',
       COUNT(*) FROM Fact_NutritionLog
WHERE ServingSize IS NOT NULL AND ServingSize <= 0;

SELECT * FROM validation_violations WHERE violation_count > 0;


-- 11) Run summary
SELECT 'VALIDATION COMPLETE' AS status,
       NOW() AS finished_at,
       (SELECT COUNT(*) FROM validation_violations WHERE violation_count > 0) AS total_violation_types,
       (SELECT SUM(violation_count) FROM validation_violations) AS total_violations;