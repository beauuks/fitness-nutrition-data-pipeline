-- Smart Fitness & Nutrition Recommendation Pipeline Database Schema
-- MySQL Database Schema for Part 1 Deliverable

-- Create database (run this separately with appropriate privileges)
-- CREATE DATABASE fitness_nutrition_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- USE fitness_nutrition_db;

-- Drop existing tables if they exist (for development)
DROP TABLE IF EXISTS user_recommendations;
DROP TABLE IF EXISTS nutrition_logs;
DROP TABLE IF EXISTS health_metrics;
DROP TABLE IF EXISTS workout_sessions;
DROP TABLE IF EXISTS nutrition_data;
DROP TABLE IF EXISTS user_profiles;

-- Create user_profiles table
CREATE TABLE user_profiles (
    unified_user_id INT AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    original_id VARCHAR(100),
    age INT CHECK (age >= 13 AND age <= 100),
    gender VARCHAR(10),
    weight DECIMAL(5,2) CHECK (weight >= 30 AND weight <= 300),
    height DECIMAL(5,2) CHECK (height >= 1.0 AND height <= 2.5),
    bmi DECIMAL(5,2),
    fitness_goal VARCHAR(50) NOT NULL DEFAULT 'maintain_health',
    fitness_type VARCHAR(50),
    workout_preference TEXT,
    diet_preference TEXT,
    health_conditions TEXT,
    experience_level VARCHAR(20),
    activity_level VARCHAR(20) DEFAULT 'moderate',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_fitness_goal CHECK (
        fitness_goal IN ('lose_weight', 'build_muscle', 'endurance', 'maintain_health')
    ),
    CONSTRAINT chk_experience_level CHECK (
        experience_level IN ('beginner', 'intermediate', 'advanced', 'expert')
    ),
    CONSTRAINT chk_activity_level CHECK (
        activity_level IN ('sedentary', 'light', 'moderate', 'active', 'very_active')
    )
);

-- Create workout_sessions table
CREATE TABLE workout_sessions (
    session_id INT AUTO_INCREMENT PRIMARY KEY,
    unified_user_id INT NOT NULL,
    workout_type VARCHAR(50),
    duration_hours DECIMAL(4,2) CHECK (duration_hours >= 0 AND duration_hours <= 12),
    calories_burned INT CHECK (calories_burned >= 0),
    total_steps INT CHECK (total_steps >= 0),
    total_distance DECIMAL(6,2) CHECK (total_distance >= 0),
    active_minutes INT CHECK (active_minutes >= 0),
    very_active_minutes INT CHECK (very_active_minutes >= 0),
    fairly_active_minutes INT CHECK (fairly_active_minutes >= 0),
    lightly_active_minutes INT CHECK (lightly_active_minutes >= 0),
    sedentary_minutes INT CHECK (sedentary_minutes >= 0),
    frequency_per_week INT CHECK (frequency_per_week >= 0 AND frequency_per_week <= 7),
    intensity_level VARCHAR(20),
    session_date DATE NOT NULL,
    session_time TIME,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    FOREIGN KEY (unified_user_id) REFERENCES user_profiles(unified_user_id) ON DELETE CASCADE,
    
    -- Constraints
    CONSTRAINT chk_workout_type CHECK (
        workout_type IN ('cardio', 'strength', 'yoga', 'hiit', 'mixed', 'running', 'cycling', 'swimming')
    ),
    CONSTRAINT chk_intensity CHECK (
        intensity_level IN ('low', 'moderate', 'high', 'very_high')
    )
);

-- Create health_metrics table
CREATE TABLE health_metrics (
    metric_id INT AUTO_INCREMENT PRIMARY KEY,
    unified_user_id INT NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    value DECIMAL(10,2) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    measurement_date DATE NOT NULL,
    measurement_time TIME,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    FOREIGN KEY (unified_user_id) REFERENCES user_profiles(unified_user_id) ON DELETE CASCADE,
    
    -- Constraints
    CONSTRAINT chk_metric_type CHECK (
        metric_type IN (
            'weight', 'bmi', 'body_fat_percentage', 'muscle_mass',
            'heart_rate', 'resting_heart_rate', 'max_heart_rate',
            'blood_pressure_systolic', 'blood_pressure_diastolic',
            'sleep_hours', 'sleep_quality', 'stress_level',
            'water_intake', 'energy_level'
        )
    ),
    CONSTRAINT chk_positive_value CHECK (value >= 0)
);

-- Create nutrition_data table (food database)
CREATE TABLE nutrition_data (
    food_id INT AUTO_INCREMENT PRIMARY KEY,
    food_name VARCHAR(200) NOT NULL UNIQUE,
    food_category VARCHAR(100),
    serving_size VARCHAR(50),
    serving_size_grams DECIMAL(8,2),
    calories DECIMAL(8,2) CHECK (calories >= 0),
    protein DECIMAL(6,2) CHECK (protein >= 0),
    carbs DECIMAL(6,2) CHECK (carbs >= 0),
    fats DECIMAL(6,2) CHECK (fats >= 0),
    fiber DECIMAL(6,2) CHECK (fiber >= 0),
    sugar DECIMAL(6,2) CHECK (sugar >= 0),
    sodium DECIMAL(8,2) CHECK (sodium >= 0),
    
    -- Vitamins and minerals
    vitamin_a DECIMAL(6,2) CHECK (vitamin_a >= 0),
    vitamin_b DECIMAL(6,2) CHECK (vitamin_b >= 0),
    vitamin_c DECIMAL(6,2) CHECK (vitamin_c >= 0),
    vitamin_d DECIMAL(6,2) CHECK (vitamin_d >= 0),
    vitamin_e DECIMAL(6,2) CHECK (vitamin_e >= 0),
    calcium DECIMAL(6,2) CHECK (calcium >= 0),
    iron DECIMAL(6,2) CHECK (iron >= 0),
    magnesium DECIMAL(6,2) CHECK (magnesium >= 0),
    zinc DECIMAL(6,2) CHECK (zinc >= 0),
    potassium DECIMAL(8,2) CHECK (potassium >= 0),
    
    -- Additional nutritional info
    cholesterol DECIMAL(6,2) CHECK (cholesterol >= 0),
    saturated_fat DECIMAL(6,2) CHECK (saturated_fat >= 0),
    monounsaturated_fat DECIMAL(6,2) CHECK (monounsaturated_fat >= 0),
    polyunsaturated_fat DECIMAL(6,2) CHECK (polyunsaturated_fat >= 0),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create nutrition_logs table (user food consumption tracking)
CREATE TABLE nutrition_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    unified_user_id INT NOT NULL,
    food_id INT NOT NULL,
    meal_type VARCHAR(20) NOT NULL,
    serving_amount DECIMAL(6,2) NOT NULL CHECK (serving_amount > 0),
    log_date DATE NOT NULL,
    log_time TIME,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (unified_user_id) REFERENCES user_profiles(unified_user_id) ON DELETE CASCADE,
    FOREIGN KEY (food_id) REFERENCES nutrition_data(food_id),
    
    -- Constraints
    CONSTRAINT chk_meal_type CHECK (
        meal_type IN ('breakfast', 'lunch', 'dinner', 'snack', 'pre_workout', 'post_workout')
    )
);

-- Create user_recommendations table (for future personalized recommendations)
CREATE TABLE user_recommendations (
    recommendation_id INT AUTO_INCREMENT PRIMARY KEY,
    unified_user_id INT NOT NULL,
    recommendation_type VARCHAR(50) NOT NULL,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    priority_level INT CHECK (priority_level >= 1 AND priority_level <= 5),
    category VARCHAR(50),
    target_goal VARCHAR(50),
    recommendation_data JSON, -- Store flexible recommendation data
    is_active BOOLEAN DEFAULT TRUE,
    created_date DATE NOT NULL DEFAULT (CURRENT_DATE),
    expires_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    FOREIGN KEY (unified_user_id) REFERENCES user_profiles(unified_user_id) ON DELETE CASCADE,
    
    -- Constraints
    CONSTRAINT chk_recommendation_type CHECK (
        recommendation_type IN ('workout', 'nutrition', 'lifestyle', 'health', 'goal_adjustment')
    ),
    CONSTRAINT chk_category CHECK (
        category IN ('exercise', 'diet', 'sleep', 'hydration', 'stress_management', 'recovery')
    )
);

-- Create indexes for better performance
CREATE INDEX idx_user_profiles_goal ON user_profiles(fitness_goal);
CREATE INDEX idx_user_profiles_source ON user_profiles(source);
CREATE INDEX idx_user_profiles_age_gender ON user_profiles(age, gender);

CREATE INDEX idx_workout_sessions_user ON workout_sessions(unified_user_id);
CREATE INDEX idx_workout_sessions_date ON workout_sessions(session_date);
CREATE INDEX idx_workout_sessions_type ON workout_sessions(workout_type);
CREATE INDEX idx_workout_sessions_user_date ON workout_sessions(unified_user_id, session_date);

CREATE INDEX idx_health_metrics_user ON health_metrics(unified_user_id);
CREATE INDEX idx_health_metrics_type ON health_metrics(metric_type);
CREATE INDEX idx_health_metrics_date ON health_metrics(measurement_date);
CREATE INDEX idx_health_metrics_user_type ON health_metrics(unified_user_id, metric_type);

CREATE INDEX idx_nutrition_data_name ON nutrition_data(food_name);
CREATE INDEX idx_nutrition_data_category ON nutrition_data(food_category);
CREATE INDEX idx_nutrition_data_calories ON nutrition_data(calories);

CREATE INDEX idx_nutrition_logs_user ON nutrition_logs(unified_user_id);
CREATE INDEX idx_nutrition_logs_date ON nutrition_logs(log_date);
CREATE INDEX idx_nutrition_logs_meal_type ON nutrition_logs(meal_type);
CREATE INDEX idx_nutrition_logs_food ON nutrition_logs(food_id);

CREATE INDEX idx_recommendations_user ON user_recommendations(unified_user_id);
CREATE INDEX idx_recommendations_type ON user_recommendations(recommendation_type);
CREATE INDEX idx_recommendations_active ON user_recommendations(is_active);
CREATE INDEX idx_recommendations_date ON user_recommendations(created_date);

-- Create views for common queries

-- View: User summary with latest metrics
CREATE VIEW user_summary AS
SELECT 
    up.unified_user_id,
    up.age,
    up.gender,
    up.fitness_goal,
    up.experience_level,
    up.activity_level,
    COALESCE(latest_weight.value, up.weight) as current_weight,
    COALESCE(latest_bmi.value, up.bmi) as current_bmi,
    workout_stats.total_workouts,
    workout_stats.avg_weekly_frequency,
    sleep_stats.avg_sleep_hours
FROM user_profiles up
LEFT JOIN (
    SELECT h1.unified_user_id, h1.value 
    FROM health_metrics h1
    INNER JOIN (
        SELECT unified_user_id, MAX(measurement_date) as max_date
        FROM health_metrics 
        WHERE metric_type = 'weight'
        GROUP BY unified_user_id
    ) h2 ON h1.unified_user_id = h2.unified_user_id 
        AND h1.measurement_date = h2.max_date
        AND h1.metric_type = 'weight'
) latest_weight ON up.unified_user_id = latest_weight.unified_user_id
LEFT JOIN (
    SELECT h1.unified_user_id, h1.value 
    FROM health_metrics h1
    INNER JOIN (
        SELECT unified_user_id, MAX(measurement_date) as max_date
        FROM health_metrics 
        WHERE metric_type = 'bmi'
        GROUP BY unified_user_id
    ) h2 ON h1.unified_user_id = h2.unified_user_id 
        AND h1.measurement_date = h2.max_date
        AND h1.metric_type = 'bmi'
) latest_bmi ON up.unified_user_id = latest_bmi.unified_user_id
LEFT JOIN (
    SELECT 
        unified_user_id,
        COUNT(*) as total_workouts,
        AVG(frequency_per_week) as avg_weekly_frequency
    FROM workout_sessions 
    WHERE session_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
    GROUP BY unified_user_id
) workout_stats ON up.unified_user_id = workout_stats.unified_user_id
LEFT JOIN (
    SELECT 
        unified_user_id,
        AVG(value) as avg_sleep_hours
    FROM health_metrics 
    WHERE metric_type = 'sleep_hours' 
        AND measurement_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
    GROUP BY unified_user_id
) sleep_stats ON up.unified_user_id = sleep_stats.unified_user_id;

-- View: Weekly fitness summary per user
CREATE VIEW weekly_fitness_summary AS
SELECT 
    ws.unified_user_id,
    DATE_SUB(ws.session_date, INTERVAL WEEKDAY(ws.session_date) DAY) as week_start,
    COUNT(*) as total_sessions,
    SUM(ws.duration_hours) as total_hours,
    SUM(ws.calories_burned) as total_calories,
    AVG(ws.duration_hours) as avg_session_duration,
    GROUP_CONCAT(DISTINCT ws.workout_type SEPARATOR ', ') as workout_types
FROM workout_sessions ws
WHERE ws.session_date >= DATE_SUB(CURRENT_DATE, INTERVAL 12 WEEK)
GROUP BY ws.unified_user_id, DATE_SUB(ws.session_date, INTERVAL WEEKDAY(ws.session_date) DAY)
ORDER BY ws.unified_user_id, week_start;

-- View: Nutritional intake summary
CREATE VIEW daily_nutrition_summary AS
SELECT 
    nl.unified_user_id,
    nl.log_date,
    SUM(nd.calories * nl.serving_amount / COALESCE(nd.serving_size_grams, 100)) as total_calories,
    SUM(nd.protein * nl.serving_amount / COALESCE(nd.serving_size_grams, 100)) as total_protein,
    SUM(nd.carbs * nl.serving_amount / COALESCE(nd.serving_size_grams, 100)) as total_carbs,
    SUM(nd.fats * nl.serving_amount / COALESCE(nd.serving_size_grams, 100)) as total_fats,
    SUM(nd.fiber * nl.serving_amount / COALESCE(nd.serving_size_grams, 100)) as total_fiber,
    COUNT(DISTINCT nl.meal_type) as meals_logged
FROM nutrition_logs nl
JOIN nutrition_data nd ON nl.food_id = nd.food_id
GROUP BY nl.unified_user_id, nl.log_date
ORDER BY nl.unified_user_id, nl.log_date;

-- Insert some sample data for testing (optional)
-- This can be removed in production

/*
INSERT INTO user_profiles (source, age, gender, weight, height, fitness_goal, experience_level) VALUES
('sample', 25, 'male', 75.5, 1.80, 'build_muscle', 'intermediate'),
('sample', 30, 'female', 65.0, 1.65, 'lose_weight', 'beginner'),
('sample', 35, 'male', 80.0, 1.75, 'endurance', 'advanced');

INSERT INTO nutrition_data (food_name, food_category, calories, protein, carbs, fats, fiber) VALUES
('Chicken Breast', 'Protein', 165, 31, 0, 3.6, 0),
('Brown Rice', 'Grains', 111, 2.6, 23, 0.9, 1.8),
('Broccoli', 'Vegetables', 34, 2.8, 7, 0.4, 2.6);
*/

-- Grant permissions (adjust as needed for your user)
-- GRANT ALL PRIVILEGES ON fitness_nutrition_db.* TO 'your_username'@'localhost';

-- Add comments to document the schema (MySQL 8.0+ supports this)
ALTER TABLE user_profiles COMMENT = 'Unified user profiles from multiple data sources';
ALTER TABLE workout_sessions COMMENT = 'Individual workout session records with activity metrics';
ALTER TABLE health_metrics COMMENT = 'Time-series health and fitness metrics for users';
ALTER TABLE nutrition_data COMMENT = 'Master food database with nutritional information';
ALTER TABLE nutrition_logs COMMENT = 'User food consumption logs with portion tracking';
ALTER TABLE user_recommendations COMMENT = 'Personalized recommendations for users based on their goals';