-- OLAP data warehouse
-- for analytical query (not transaction)
-- using snowflake schema

-- Create database 
-- CREATE DATABASE fitness_nutrition_dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- USE fitness_nutrition_dw;

-- Drop existing tables in the correct order (child tables first)
DROP TABLE IF EXISTS Fact_NutritionLog;
DROP TABLE IF EXISTS Fact_HealthMetric;
DROP TABLE IF EXISTS Fact_WorkoutSession;
DROP TABLE IF EXISTS Fact_UserSnapshot;
DROP TABLE IF EXISTS Bridge_User_HealthCondition;
DROP TABLE IF EXISTS Bridge_User_WorkoutPreference;
DROP TABLE IF EXISTS Bridge_User_DietPreference;
DROP TABLE IF EXISTS Dim_User;
DROP TABLE IF EXISTS Dim_Date;
DROP TABLE IF EXISTS Dim_FitnessGoal;
DROP TABLE IF EXISTS Dim_FitnessType;
DROP TABLE IF EXISTS Dim_HealthCondition;
DROP TABLE IF EXISTS Dim_Exercise;
DROP TABLE IF EXISTS Dim_Diet;
DROP TABLE IF EXISTS Dim_FoodItem;
DROP TABLE IF EXISTS Dim_MetricType;
DROP TABLE IF EXISTS Dim_MealType;
DROP TABLE IF EXISTS Dim_WorkoutType;


-- DIMENSION TABLES (the context)

-- Dimension: Date
CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY AUTO_INCREMENT,
    FullDate DATE NOT NULL UNIQUE,
    DayOfWeek INT,
    DayName VARCHAR(10),
    Month INT,
    MonthName VARCHAR(10),
    Quarter INT,
    Year INT
) COMMENT='Date dimension for time-series analysis';

-- Dimension: User
CREATE TABLE Dim_User (
    UserKey INT PRIMARY KEY, -- unified_user_id from ETL pipeline
    Source VARCHAR(50) NOT NULL,
    OriginalID VARCHAR(100),
    Age INT,
    Gender VARCHAR(10),
    ExperienceLevel VARCHAR(20),
    ActivityLevel VARCHAR(20)
) COMMENT='User dimension storing user attributes';

-- Dimension: Fitness Goal
CREATE TABLE Dim_FitnessGoal (
    GoalKey INT PRIMARY KEY AUTO_INCREMENT,
    GoalName VARCHAR(50) NOT NULL UNIQUE
) COMMENT='Fitness goals (e.g., lose_weight, build_muscle)';

-- Dimension: Fitness Type
CREATE TABLE Dim_FitnessType (
    TypeKey INT PRIMARY KEY AUTO_INCREMENT,
    TypeName VARCHAR(50) NOT NULL UNIQUE
) COMMENT='Fitness types (e.g., muscular_fitness, cardio)';

-- Dimension: Health Condition
CREATE TABLE Dim_HealthCondition (
    ConditionKey INT PRIMARY KEY AUTO_INCREMENT,
    ConditionName VARCHAR(100) NOT NULL UNIQUE
) COMMENT='Health conditions (e.g., hypertension, diabetes)';

-- Dimension: Exercise
CREATE TABLE Dim_Exercise (
    ExerciseKey INT PRIMARY KEY AUTO_INCREMENT,
    ExerciseName VARCHAR(100) NOT NULL UNIQUE
) COMMENT='Individual exercises (e.g., squats, deadlifts)';

-- Dimension: Diet Preference
CREATE TABLE Dim_Diet (
    DietKey INT PRIMARY KEY AUTO_INCREMENT,
    DietName VARCHAR(100) NOT NULL UNIQUE
) COMMENT='Dietary preferences (e.g., low_carb, high_protein)';

-- Dimension: Food Item (from Nutrition dataset)
CREATE TABLE Dim_FoodItem (
    FoodKey INT PRIMARY KEY AUTO_INCREMENT,
    FoodName VARCHAR(200) NOT NULL UNIQUE,
    FoodCategory VARCHAR(100),
    Calories DECIMAL(8,2),
    Protein DECIMAL(6,2),
    Carbs DECIMAL(6,2),
    Fats DECIMAL(6,2),
    Fiber DECIMAL(6,2)
) COMMENT='Master food database';

-- Dimension: Metric Type
CREATE TABLE Dim_MetricType (
    MetricTypeKey INT PRIMARY KEY AUTO_INCREMENT,
    MetricName VARCHAR(50) NOT NULL UNIQUE COMMENT 'e.g., heart_rate, sleep, weight'
) COMMENT='Types of health metrics that can be measured';

-- Dimension: Meal Type
CREATE TABLE Dim_MealType (
    MealTypeKey INT PRIMARY KEY AUTO_INCREMENT,
    MealName VARCHAR(20) NOT NULL UNIQUE COMMENT 'e.g., breakfast, lunch, dinner'
) COMMENT='Types of meals for nutrition logging';

-- Dimension: Workout Type
CREATE TABLE Dim_WorkoutType (
    WorkoutTypeKey INT PRIMARY KEY AUTO_INCREMENT,
    WorkoutName VARCHAR(50) NOT NULL UNIQUE COMMENT 'e.g., cardio, strength, hiit'
) COMMENT='Categories of workouts';


-- BRIDGE TABLES (Many-to-Many Links)

-- Bridge: User <-> Health Conditions
CREATE TABLE Bridge_User_HealthCondition (
    UserKey INT NOT NULL,
    ConditionKey INT NOT NULL,
    PRIMARY KEY (UserKey, ConditionKey),
    FOREIGN KEY (UserKey) REFERENCES Dim_User(UserKey),
    FOREIGN KEY (ConditionKey) REFERENCES Dim_HealthCondition(ConditionKey)
) COMMENT='Links users to their multiple health conditions';

-- Bridge: User <-> Workout Preferences
CREATE TABLE Bridge_User_WorkoutPreference (
    UserKey INT NOT NULL,
    ExerciseKey INT NOT NULL,
    PRIMARY KEY (UserKey, ExerciseKey),
    FOREIGN KEY (UserKey) REFERENCES Dim_User(UserKey),
    FOREIGN KEY (ExerciseKey) REFERENCES Dim_Exercise(ExerciseKey)
) COMMENT='Links users to their preferred exercises';

-- Bridge: User <-> Diet Preferences
CREATE TABLE Bridge_User_DietPreference (
    UserKey INT NOT NULL,
    DietKey INT NOT NULL,
    PRIMARY KEY (UserKey, DietKey),
    FOREIGN KEY (UserKey) REFERENCES Dim_User(UserKey),
    FOREIGN KEY (DietKey) REFERENCES Dim_Diet(DietKey)
) COMMENT='Links users to their diet preferences';


-- FACT TABLES (the measures)

-- Fact: User Snapshot (Grain: One row per user, per goal)
CREATE TABLE Fact_UserSnapshot (
    SnapshotKey INT PRIMARY KEY AUTO_INCREMENT,
    UserKey INT NOT NULL,
    GoalKey INT NOT NULL,
    TypeKey INT,
    Height DECIMAL(5,2),
    Weight DECIMAL(5,2),
    BMI DECIMAL(5,2),
    FOREIGN KEY (UserKey) REFERENCES Dim_User(UserKey),
    FOREIGN KEY (GoalKey) REFERENCES Dim_FitnessGoal(GoalKey),
    FOREIGN KEY (TypeKey) REFERENCES Dim_FitnessType(TypeKey)
) COMMENT='Snapshot of user profile metrics (height, weight, bmi)';

-- Fact: Workout Session (Grain: One row per workout)
CREATE TABLE Fact_WorkoutSession (
    SessionKey INT PRIMARY KEY AUTO_INCREMENT,
    UserKey INT NOT NULL,
    DateKey INT NOT NULL,
    WorkoutTypeKey INT,
    DurationHours DECIMAL(4,2),
    CaloriesBurned INT,
    TotalSteps INT,
    TotalDistance DECIMAL(6,2),
    ActiveMinutes INT,
    FrequencyPerWeek INT,
    FOREIGN KEY (UserKey) REFERENCES Dim_User(UserKey),
    FOREIGN KEY (DateKey) REFERENCES Dim_Date(DateKey),
    FOREIGN KEY (WorkoutTypeKey) REFERENCES Dim_WorkoutType(WorkoutTypeKey)
) COMMENT='Records of individual workout sessions';

-- Fact: Health Metric (Grain: One row per user, per metric, per day)
CREATE TABLE Fact_HealthMetric (
    MetricKey INT PRIMARY KEY AUTO_INCREMENT,
    UserKey INT NOT NULL,
    DateKey INT NOT NULL,
    MetricTypeKey INT NOT NULL,
    Value DECIMAL(10,2) NOT NULL,
    Unit VARCHAR(20),
    FOREIGN KEY (UserKey) REFERENCES Dim_User(UserKey),
    FOREIGN KEY (DateKey) REFERENCES Dim_Date(DateKey),
    FOREIGN KEY (MetricTypeKey) REFERENCES Dim_MetricType(MetricTypeKey)
) COMMENT='Time-series health data (sleep, heart rate, etc.)';

-- Fact: Nutrition Log (Grain: One row per food log entry)
CREATE TABLE Fact_NutritionLog (
    LogKey INT PRIMARY KEY AUTO_INCREMENT,
    UserKey INT NOT NULL,
    DateKey INT NOT NULL,
    FoodKey INT NOT NULL,
    MealTypeKey INT NOT NULL,
    ServingAmount DECIMAL(6,2),
    FOREIGN KEY (UserKey) REFERENCES Dim_User(UserKey),
    FOREIGN KEY (DateKey) REFERENCES Dim_Date(DateKey),
    FOREIGN KEY (FoodKey) REFERENCES Dim_FoodItem(FoodKey),
    FOREIGN KEY (MealTypeKey) REFERENCES Dim_MealType(MealTypeKey)
) COMMENT='User food consumption logs';

-- Create Indexes for performance
CREATE INDEX idx_fact_workout_user ON Fact_WorkoutSession(UserKey);
CREATE INDEX idx_fact_workout_date ON Fact_WorkoutSession(DateKey);
CREATE INDEX idx_fact_metric_user ON Fact_HealthMetric(UserKey);
CREATE INDEX idx_fact_metric_date ON Fact_HealthMetric(DateKey);
CREATE INDEX idx_fact_metric_type ON Fact_HealthMetric(MetricTypeKey);
CREATE INDEX idx_fact_nutrition_user ON Fact_NutritionLog(UserKey);
CREATE INDEX idx_fact_nutrition_date ON Fact_NutritionLog(DateKey);
CREATE INDEX idx_fact_nutrition_food ON Fact_NutritionLog(FoodKey);