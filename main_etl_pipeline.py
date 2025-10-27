import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
import warnings
import re

warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log', mode='w'), # Overwrite log on new run
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FitnessNutritionETL:
    """
    ETL class for fitness & nutrition pipeline 
    
    This pipeline extracts data from multiple sources, transforms it, 
    and loads it into a MySQL data warehouse for analytical querying.
    """
    
    def __init__(self, config):
        self.config = config
        self.data_sources = {} # empty dict to hold raw df's.
        self.staging_data = {}
        self.warehouse_data = {} # holds the final Dim/Fact df's
        self.engine = None
        self.user_mapping = {}
        
    def setup_database_connection(self):
        """Setup MySQL database connection using SQLAlchemy."""
        try:
            # Note: The database (e.g., 'fitness_nutrition_dw') must exist.
            # The schema will be created inside it.
            db_cfg = self.config['DATABASE_CONFIG']
            connection_string = (
                f"mysql+pymysql://{db_cfg['username']}:{db_cfg['password']}@"
                f"{db_cfg['host']}:{db_cfg['port']}/{db_cfg['database']}?charset=utf8mb4"
            )
            self.engine = create_engine(connection_string)
            logger.info("MySQL database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to MySQL database: {e}")
            raise

    # EXTRACT 
    def extract_fitbit_data(self):
        """Extract Fitbit datasets"""
        logger.info("Starting Fitbit data extraction...")
        fitbit_files = {
            'daily_activity': 'dailyActivity_merged.csv',
            'heartrate': 'heartrate_seconds_merged.csv',
            'hourly_calories': 'hourlyCalories_merged.csv',
            'weight_log': 'weightLogInfo_merged.csv',
            'sleep_minutes': 'minuteSleep_merged.csv'
        }
        fitbit_data = {}
        base_path = Path(self.config['DATA_PATHS']['fitbit_path'])
        
        for key, filename in fitbit_files.items():
            try:
                filepath = base_path / filename
                if not filepath.exists():
                    logger.warning(f"File not found: {filepath}, skipping...")
                    continue
                df = pd.read_csv(filepath)
                fitbit_data[key] = df
                logger.info(f"Extracted {key}: {len(df)} records")
            except Exception as e:
                logger.error(f"Error extracting {filename}: {e}")
        
        self.data_sources['fitbit'] = fitbit_data
        logger.info("Fitbit data extraction complete.")

    def extract_gym_members_data(self):
        """Extract gym members dataset"""
        logger.info("Starting gym members data extraction...")
        try:
            filepath = self.config['DATA_PATHS']['gym_members_file']
            if not filepath.exists():
                logger.warning(f"File not found: {filepath}, skipping...")
                return
            df = pd.read_csv(filepath)
            self.data_sources['gym_members'] = df
            logger.info(f"Extracted gym members data: {len(df)} records")
        except Exception as e:
            logger.error(f"Error processing gym members data: {e}")

    def extract_mendeley_health_data(self):
        """Extract Mendeley health dataset"""
        logger.info("Starting Mendeley health data extraction...")
        try:
            filepath = self.config['DATA_PATHS']['mendeley_file']
            if not filepath.exists():
                logger.warning(f"File not found: {filepath}, skipping...")
                return
            df = pd.read_excel(filepath) 
            self.data_sources['mendeley_health'] = df
            logger.info(f"Extracted Mendeley health data: {len(df)} records")
        except Exception as e:
            logger.error(f"Error processing Mendeley health data: {e}")

    def extract_nutrition_data(self):
        """Extract nutrition dataset"""
        logger.info("Starting nutrition data extraction...")
        try:
            filepath = self.config['DATA_PATHS']['nutrition_file']
            if not filepath.exists():
                logger.warning(f"File not found: {filepath}, skipping...")
                return
            df = pd.read_excel(filepath)
            self.data_sources['nutrition'] = df
            logger.info(f"Extracted nutrition data: {len(df)} records")
        except Exception as e:
            logger.error(f"Error processing nutrition data: {e}")

    # TRANSFORM 
    def _clean_text_list(self, text):
        """Helper to split comma-separated strings into a clean list"""
        if not isinstance(text, str):
            return []
        # Remove "and", split by comma or newline, strip whitespace, remove empty
        items = re.split(r'[,\n]| and ', text.lower())
        return [item.strip() for item in items if item.strip()]

    def transform_data(self):
        """
        Main transformation function.
        Orchestrates the transformation of raw data into a Snowflake Schema.
        """
        logger.info("Starting data transformation for Data Warehouse...")
        
        # 1. Create Staging Data (similar to your old 'transform_and_integrate_data')
        self._create_staging_data()
        
        # 2. Generate Master Date Dimension
        self._create_dim_date()
        
        # 3. Create Dimension DataFrames
        self._create_dimensions()
        
        # 4. Create Bridge Table DataFrames
        self._create_bridges()
        
        # 5. Create Fact Table DataFrames
        self._create_facts()
        
        logger.info("Data transformation into Snowflake Schema completed.")

    def _create_user_mapping(self):
        """
        Create unified user mapping using probabilistic matching
        for anonymous datasets (Mendeley, Gym) and a separate
        mapping for ID-based datasets (Fitbit).
        """
        logger.info("Creating user mapping...")
        
        self.user_mapping = {}
        self.staging_profiles = [] # A temporary list to hold master profiles
        profile_hash_map = {} # Stores {profile_hash: UserKey}
        next_user_id = 1
        
        # --- Pass 1: Anchor on Mendeley (richest profile data) ---
        if 'mendeley_health' in self.data_sources:
            df = self.data_sources['mendeley_health'].copy()
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            for idx, row in df.iterrows():
                try:
                    # Standardize data for hashing
                    age = int(row.get('age'))
                    gender = str(row.get('sex')).lower()
                    height_m = round(float(row.get('height')) / 100, 2) # Assuming cm
                    weight_kg = round(float(row.get('weight')), 1)
                    
                    profile_hash = f"{age}_{gender}_{height_m}_{weight_kg}"
                    
                    if profile_hash not in profile_hash_map:
                        # New user
                        user_key = next_user_id
                        profile_hash_map[profile_hash] = user_key
                        next_user_id += 1

                        conditions = []
                        if str(row.get('hypertension')).lower() == 'yes':
                            conditions.append('hypertension')
                        if str(row.get('diabetes')).lower() == 'yes':
                            conditions.append('diabetes')
                        health_conditions_text = ', '.join(conditions) if conditions else None
                        
                        # Create the master profile
                        profile_data = {
                            'UserKey': user_key,
                            'Source': 'mendeley',
                            'OriginalID': idx,
                            'Age': age,
                            'Gender': gender,
                            'Weight': weight_kg,
                            'Height': height_m,
                            'BMI': row.get('bmi'),
                            'HealthConditions': health_conditions_text, 
                            'FitnessGoal': self._standardize_fitness_goal(row.get('fitness_goals')),
                            'FitnessType': row.get('fitness_type'),
                            'WorkoutPreference': row.get('exercise'),
                            'DietPreference': row.get('diet'),
                            'ExperienceLevel': None,
                            'ActivityLevel': None
                        }
                        self.staging_profiles.append(profile_data)
                    else:
                        # This is a duplicate *within* the Mendeley file
                        user_key = profile_hash_map[profile_hash]
                    
                    self.user_mapping[f"mendeley_{idx}"] = user_key
                
                except Exception as e:
                    logger.warning(f"Could not parse Mendeley row {idx}: {e}")

        # --- Pass 2: Link Gym Members ---
        if 'gym_members' in self.data_sources:
            df = self.data_sources['gym_members'].copy()
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            for idx, row in df.iterrows():
                try:
                    # Standardize data for hashing
                    age = int(row.get('age'))
                    gender = str(row.get('gender')).lower()
                    height_m = round(float(row.get('height_(m)')), 2) # Check col name
                    weight_kg = round(float(row.get('weight_(kg)')), 1) # Check col name
                    
                    profile_hash = f"{age}_{gender}_{height_m}_{weight_kg}"
                    
                    if profile_hash in profile_hash_map:
                        # --- MATCH FOUND ---
                        # Link this gym row to the existing Mendeley user
                        user_key = profile_hash_map[profile_hash]
                        self.user_mapping[f"gym_{idx}"] = user_key
                        
                        # (Optional) Enrich the existing profile
                        # e.g., Find profile in self.staging_profiles and fill missing values
                        
                    else:
                        # --- NO MATCH: New User ---
                        user_key = next_user_id
                        profile_hash_map[profile_hash] = user_key
                        next_user_id += 1
                        
                        profile_data = {
                            'UserKey': user_key,
                            'Source': 'gym',
                            'OriginalID': idx,
                            'Age': age,
                            'Gender': gender,
                            'Weight': weight_kg,
                            'Height': height_m,
                            'BMI': row.get('bmi'),
                            'HealthConditions': None,
                            'FitnessGoal': self._standardize_fitness_goal(row.get('workout_type')),
                            'FitnessType': row.get('workout_type'),
                            'WorkoutPreference': row.get('workout_type'),
                            'DietPreference': None,
                            'ExperienceLevel': row.get('experience_level'),
                            'ActivityLevel': None
                        }
                        self.staging_profiles.append(profile_data)
                        
                    self.user_mapping[f"gym_{idx}"] = user_key

                except Exception as e:
                    logger.warning(f"Could not parse Gym Member row {idx}: {e}")

        # --- Pass 3: Add Fitbit Users (Unlinkable) ---
        if 'fitbit' in self.data_sources:
            fitbit_users = set()
            for dataset in self.data_sources['fitbit'].values():
                if 'Id' in dataset.columns:
                    fitbit_users.update(dataset['Id'].unique())
            
            for user_id in fitbit_users:
                user_key = next_user_id
                self.user_mapping[f"fitbit_{user_id}"] = user_key
                next_user_id += 1
                
                # Create a "shell" profile for the Fitbit user
                profile_data = {
                    'UserKey': user_key,
                    'Source': 'fitbit',
                    'OriginalID': user_id,
                    'Age': None, 'Gender': None, 'Weight': None, 'Height': None, 'BMI': None,
                    'HealthConditions': None, 'FitnessGoal': 'maintain_health', 'FitnessType': None,
                    'WorkoutPreference': None, 'DietPreference': None, 'ExperienceLevel': None, 'ActivityLevel': None
                }
                self.staging_profiles.append(profile_data)
                
        logger.info(f"Created user mapping for {len(self.staging_profiles)} unique users")

    def _standardize_fitness_goal(self, goal_text):
        """Standardize fitness goals using config mapping."""
        if not isinstance(goal_text, str):
            return 'maintain_health'
        goal_text = str(goal_text).lower()
        
        for key, keywords in self.config['FITNESS_GOALS'].items():
            if any(word in goal_text for word in keywords):
                return key
        return 'maintain_health'

    def _create_staging_data(self):
        """
        Converts the processed staging_profiles list  into the final staging DataFrame
        """
        logger.info("Creating staging data...")
        
        # create the user mapping
        self._create_user_mapping() 
        
        # convert the list of profile into a staging df
        if not self.staging_profiles:
            logger.warning("No user profiles were created. Staging will be empty.")
            self.staging_data['user_profiles'] = pd.DataFrame()
            return

        self.staging_data['user_profiles'] = pd.DataFrame(self.staging_profiles)
        
        # set the UserKey as the index 
        self.staging_data['user_profiles'] = self.staging_data['user_profiles'].set_index('UserKey')
        
        logger.info(f"Staging user profiles created: {len(self.staging_data['user_profiles'])} records")

    def _create_dim_date(self, start='2016-01-01', end='2025-12-31'):
        """Create a master date dimension table"""
        logger.info("Creating Dim_Date...")
        df = pd.DataFrame({'FullDate': pd.date_range(start, end)})
        df['DateKey'] = df['FullDate'].dt.strftime('%Y%m%d').astype(int)
        df['DayOfWeek'] = df['FullDate'].dt.dayofweek
        df['DayName'] = df['FullDate'].dt.day_name()
        df['Month'] = df['FullDate'].dt.month
        df['MonthName'] = df['FullDate'].dt.month_name()
        df['Quarter'] = df['FullDate'].dt.quarter
        df['Year'] = df['FullDate'].dt.year
        df = df.set_index('DateKey')
        self.warehouse_data['Dim_Date'] = df
        
        # Create a lookup map for faster fact creation
        self.date_lookup = df['FullDate'].to_dict()
        self.date_lookup_rev = {v.strftime('%Y-%m-%d'): k for k, v in self.date_lookup.items()}

    def _create_dimensions(self):
        """Create all dimension DataFrames from staging data."""
        logger.info("Creating Dimension tables...")
        staging_df = self.staging_data['user_profiles']

        # Dim_User
        cols = ['Source', 'OriginalID', 'Age', 'Gender', 'ExperienceLevel', 'ActivityLevel']
        self.warehouse_data['Dim_User'] = staging_df[cols]
        
        # Dim_FitnessGoal
        goals = staging_df['FitnessGoal'].dropna().unique()
        self.warehouse_data['Dim_FitnessGoal'] = pd.DataFrame(goals, columns=['GoalName'])
        self.warehouse_data['Dim_FitnessGoal'].index.name = 'GoalKey'
        self.warehouse_data['Dim_FitnessGoal'].index += 1 # Start IDs from 1
        
        # Dim_FitnessType
        types = staging_df['FitnessType'].dropna().unique()
        self.warehouse_data['Dim_FitnessType'] = pd.DataFrame(types, columns=['TypeName'])
        self.warehouse_data['Dim_FitnessType'].index.name = 'TypeKey'
        self.warehouse_data['Dim_FitnessType'].index += 1

        # Dim_HealthCondition, Dim_Exercise, Dim_Diet (from TEXT blobs)
        self.warehouse_data['Dim_HealthCondition'] = self._create_dim_from_blob('HealthConditions', 'ConditionName')
        self.warehouse_data['Dim_Exercise'] = self._create_dim_from_blob('WorkoutPreference', 'ExerciseName')
        self.warehouse_data['Dim_Diet'] = self._create_dim_from_blob('DietPreference', 'DietName')

        # Dim_FoodItem
        if 'nutrition' in self.data_sources:
            df = self.data_sources['nutrition'].copy()
            df = df.rename(columns={'name': 'FoodName', 'category': 'FoodCategory'}) # Rename first

            logger.info("Cleaning numeric nutrient columns in nutrition data...")
            numeric_nutrient_columns = [
                'calories', 'total_fat', 'saturated_fat', 'cholesterol', 'sodium',
                'choline', 'folate', 'folic_acid', 'niacin', 'pantothenic_acid',
                'riboflavin', 'thiamin', 'vitamin_a', 'vitamin_a_rae', 'carotene_alpha',
                'carotene_beta', 'cryptoxanthin_beta', 'lutein_zeaxanthin', 'lucopene',
                'vitamin_b12', 'vitamin_b6', 'vitamin_c', 'vitamin_d', 'vitamin_e',
                'tocopherol_alpha', 'vitamin_k', 'calcium', 'copper', 'iron', 'magnesium',
                'manganese', 'phosphorous', 'potassium', 'selenium', 'zink', 'protein',
                # Add amino acids if needed
                'alanine', 'arginine', 'aspartic_acid', 'cystine', 'glutamic_acid',
                'glycine', 'histidine', 'hydroxyproline', 'isoleucine', 'leucine',
                'lysine', 'methionine', 'phenylalanine', 'proline', 'serine',
                'threonine', 'tryptophan', 'tyrosine', 'valine',
                'carbohydrate', 'fiber', 'sugars', 'fructose', 'galactose', 'glucose',
                'lactose', 'maltose', 'sucrose',
                # Add fats if needed
                'saturated_fatty_acids', 'monounsaturated_fatty_acids',
                'polyunsaturated_fatty_acids', 'fatty_acids_total_trans',
                'alcohol', 'ash', 'caffeine', 'theobromin', 'water'
            ]

            # Regex to match common units at the end (case-insensitive)
            # Handles g, mg, mcg, iu, kcal (for calories maybe), etc.
            unit_regex = r'\s*(g|mg|mcg|iu|kcal)$'

            for col in numeric_nutrient_columns:
                if col in df.columns:
                    # Only process if it's currently a string column
                    if df[col].dtype == 'object':
                        # Remove units and extra whitespace
                        df[col] = df[col].astype(str).str.replace(unit_regex, '', regex=True, case=False).str.strip()
                        # Convert to numeric, errors become NaN (-> NULL)
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    elif pd.api.types.is_numeric_dtype(df[col]):
                         pass # Already numeric, do nothing
                    else:
                         # Log unexpected types and attempt conversion
                         logger.warning(f"Unexpected dtype '{df[col].dtype}' for nutrient column '{col}'. Attempting numeric conversion.")
                         df[col] = pd.to_numeric(df[col], errors='coerce')

            # Ensure 'serving_size_grams' is also numeric if you use it later
            if 'serving_size_grams' in df.columns:
                 df['serving_size_grams'] = pd.to_numeric(df['serving_size_grams'], errors='coerce')

            logger.info("Nutrient column cleaning complete.")

            cols = ['FoodName', 'FoodCategory', 'calories', 'protein', 'carbs', 'fats', 'fiber']
            available_cols = [c for c in cols if c in df.columns]
            df = df[available_cols]
            df = df[[c for c in cols if c in df.columns]].dropna(subset=['FoodName']).drop_duplicates(subset=['FoodName'])
            df = df.reset_index(drop=True)
            df.index.name = 'FoodKey'
            df.index += 1
            self.warehouse_data['Dim_FoodItem'] = df

        # Dimensions for workout/metric types (static)
        self.warehouse_data['Dim_MetricType'] = pd.DataFrame(
            {'MetricName': ['heart_rate', 'sleep', 'weight', 'bmi']},
            index=pd.Index(range(1, 5), name='MetricTypeKey')
        )
        self.warehouse_data['Dim_WorkoutType'] = pd.DataFrame(
            {'WorkoutName': staging_df['FitnessType'].dropna().unique()}, # Reuse fitness types
            index=pd.Index(range(1, len(staging_df['FitnessType'].dropna().unique()) + 1), name='WorkoutTypeKey')
        )
        self.warehouse_data['Dim_MealType'] = pd.DataFrame(
            {'MealName': ['breakfast', 'lunch', 'dinner', 'snack']},
            index=pd.Index(range(1, 5), name='MealTypeKey')
        )
        
        # Create lookup maps for bridges and facts
        self.goal_lookup = {name: key for key, name in self.warehouse_data['Dim_FitnessGoal']['GoalName'].to_dict().items()}
        self.type_lookup = {name: key for key, name in self.warehouse_data['Dim_FitnessType']['TypeName'].to_dict().items()}
        self.condition_lookup = {name: key for key, name in self.warehouse_data['Dim_HealthCondition']['ConditionName'].to_dict().items()}
        self.exercise_lookup = {name: key for key, name in self.warehouse_data['Dim_Exercise']['ExerciseName'].to_dict().items()}
        self.diet_lookup = {name: key for key, name in self.warehouse_data['Dim_Diet']['DietName'].to_dict().items()}
        self.metric_type_lookup = {name: key for key, name in self.warehouse_data['Dim_MetricType']['MetricName'].to_dict().items()}
        self.workout_type_lookup = {name: key for key, name in self.warehouse_data['Dim_WorkoutType']['WorkoutName'].to_dict().items()}

    def _create_dim_from_blob(self, column_name, dim_name):
        """Helper to parse a TEXT blob column into a unique Dimension DataFrame."""
        staging_df = self.staging_data['user_profiles']
        all_items = set()
        staging_df[column_name].dropna().apply(lambda x: all_items.update(self._clean_text_list(x)))
        df = pd.DataFrame(list(all_items), columns=[dim_name])
        df = df.reset_index()
        df = df.rename(columns={'index': f"{dim_name.replace('Name', 'Key')}"})
        df[f"{dim_name.replace('Name', 'Key')}"] += 1
        return df.set_index(f"{dim_name.replace('Name', 'Key')}")

    def _create_bridges(self):
        """Create all bridge DataFrames."""
        logger.info("Creating Bridge tables...")
        staging_df = self.staging_data['user_profiles']
        
        self.warehouse_data['Bridge_User_HealthCondition'] = self._create_bridge_from_blob(
            staging_df, 'HealthConditions', self.condition_lookup, 'ConditionKey'
        )
        self.warehouse_data['Bridge_User_WorkoutPreference'] = self._create_bridge_from_blob(
            staging_df, 'WorkoutPreference', self.exercise_lookup, 'ExerciseKey'
        )
        self.warehouse_data['Bridge_User_DietPreference'] = self._create_bridge_from_blob(
            staging_df, 'DietPreference', self.diet_lookup, 'DietKey'
        )
        
    def _create_bridge_from_blob(self, staging_df, column_name, lookup_map, key_name):
        """Helper to create a bridge table DataFrame."""
        bridge_data = []
        for user_key, row in staging_df.iterrows():
            items = self._clean_text_list(row[column_name])
            for item in items:
                item_key = lookup_map.get(item)
                if item_key:
                    bridge_data.append({
                        'UserKey': user_key,
                        key_name: item_key
                    })
        return pd.DataFrame(bridge_data).drop_duplicates()

    def _create_facts(self):
        """Create all fact DataFrames."""
        logger.info("Creating Fact tables...")
        
        # Fact_UserSnapshot
        staging_df = self.staging_data['user_profiles']
        snapshot_df = staging_df[['Height', 'Weight', 'BMI', 'FitnessGoal', 'FitnessType']].copy()
        snapshot_df['GoalKey'] = snapshot_df['FitnessGoal'].map(self.goal_lookup)
        snapshot_df['TypeKey'] = snapshot_df['FitnessType'].map(self.type_lookup)
        self.warehouse_data['Fact_UserSnapshot'] = snapshot_df[['GoalKey', 'TypeKey', 'Height', 'Weight', 'BMI']].reset_index()

        # Fact_HealthMetric & Fact_WorkoutSession
        self._create_facts_from_fitbit()

        # Fact_NutritionLog
        self._create_fact_nutrition_log()

    def _create_facts_from_fitbit(self):
        """Parse fitbit data into Fact_HealthMetric and Fact_WorkoutSession."""
        if 'fitbit' not in self.data_sources:
            return

        health_metrics = []
        workout_sessions = []
        fitbit_data = self.data_sources['fitbit']

        # Process sleep
        if 'sleep_minutes' in fitbit_data:
            df = fitbit_data['sleep_minutes'].copy()
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df = df.groupby(['Id', 'date'])['value'].sum().reset_index()
            for _, row in df.iterrows():
                user_key = self.user_mapping.get(f"fitbit_{row['Id']}")
                date_key = self.date_lookup_rev.get(row['date'])
                if user_key and date_key:
                    health_metrics.append({
                        'UserKey': user_key, 'DateKey': date_key,
                        'MetricTypeKey': self.metric_type_lookup['sleep'],
                        'Value': row['value'] / 60, # Convert to hours
                        'Unit': 'hours'
                    })
                    
        # Process heartrate
        if 'heartrate' in fitbit_data:
            df = fitbit_data['heartrate'].copy()
            df['Time'] = pd.to_datetime(df['Time'])
            df['Date'] = df['Time'].dt.strftime('%Y-%m-%d')
            df = df.groupby(['Id', 'Date'])['Value'].mean().reset_index()
            for _, row in df.iterrows():
                user_key = self.user_mapping.get(f"fitbit_{row['Id']}")
                date_key = self.date_lookup_rev.get(row['Date'])
                if user_key and date_key:
                    health_metrics.append({
                        'UserKey': user_key, 'DateKey': date_key,
                        'MetricTypeKey': self.metric_type_lookup['heart_rate'],
                        'Value': row['Value'], 'Unit': 'bpm'
                    })

        # Process weight
        if 'weight_log' in fitbit_data:
            df = fitbit_data['weight_log'].copy()
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            for _, row in df.iterrows():
                user_key = self.user_mapping.get(f"fitbit_{row['Id']}")
                date_key = self.date_lookup_rev.get(row['Date'])
                if user_key and date_key:
                    health_metrics.append({'UserKey': user_key, 'DateKey': date_key,
                                           'MetricTypeKey': self.metric_type_lookup['weight'],
                                           'Value': row['WeightKg'], 'Unit': 'kg'})
                    health_metrics.append({'UserKey': user_key, 'DateKey': date_key,
                                           'MetricTypeKey': self.metric_type_lookup['bmi'],
                                           'Value': row['BMI'], 'Unit': 'bmi'})

        # Process daily activity as workout sessions
        if 'daily_activity' in fitbit_data:
            df = fitbit_data['daily_activity'].copy()
            df['ActivityDate'] = pd.to_datetime(df['ActivityDate']).dt.strftime('%Y-%m-%d')
            for _, row in df.iterrows():
                user_key = self.user_mapping.get(f"fitbit_{row['Id']}")
                date_key = self.date_lookup_rev.get(row['ActivityDate'])
                if user_key and date_key:
                    workout_sessions.append({
                        'UserKey': user_key, 'DateKey': date_key,
                        'WorkoutTypeKey': self.workout_type_lookup.get('mixed'), # Default
                        # store both hours and minutes for easier analytics --> calculate once for difference uses.
                        'DurationHours': (row['VeryActiveMinutes'] + row['FairlyActiveMinutes']) / 60,
                        'ActiveMinutes': row['VeryActiveMinutes'] + row['FairlyActiveMinutes'], 
                        'CaloriesBurned': row['Calories'],
                        'TotalSteps': row['TotalSteps'],
                        'TotalDistance': row['TotalDistance'],
                        'FrequencyPerWeek': None # This would be calculated in analysis, not ETL (we can't know it with data from only one day)
                    })
        
        self.warehouse_data['Fact_HealthMetric'] = pd.DataFrame(health_metrics)
        self.warehouse_data['Fact_WorkoutSession'] = pd.DataFrame(workout_sessions)
        logger.info(f"Created Fact_HealthMetric: {len(self.warehouse_data['Fact_HealthMetric'])} records")
        logger.info(f"Created Fact_WorkoutSession: {len(self.warehouse_data['Fact_WorkoutSession'])} records")

    def _create_fact_nutrition_log(self):
        """Create Fact_NutritionLog from available data or generate sample logs"""
        logger.info("Creating Fact_NutritionLog...")
        
        # Check if we have food items dimension
        if 'Dim_FoodItem' not in self.warehouse_data or self.warehouse_data['Dim_FoodItem'].empty:
            logger.warning("No food items available. Skipping Fact_NutritionLog creation.")
            self.warehouse_data['Fact_NutritionLog'] = pd.DataFrame()
            return
        
        # TODO: In a production system, this would extract from actual meal log data
        # for now we will generate sample data for demonstration since we dont have actual meal logs
        
        nutrition_logs = []
        
        # Get available users and food items
        user_keys = self.staging_data['user_profiles'].index.tolist()
        food_items = self.warehouse_data['Dim_FoodItem']
        
        if len(user_keys) == 0 or len(food_items) == 0:
            logger.warning("No users or food items available for nutrition logs.")
            self.warehouse_data['Fact_NutritionLog'] = pd.DataFrame()
            return
        
        # Create meal type lookup
        meal_type_lookup = {
            'breakfast': 1,
            'lunch': 2,
            'dinner': 3,
            'snack': 4
        }
        
        # Generate sample logs for the first 10 users (or fewer if less available)
        sample_users = user_keys[:min(10, len(user_keys))]
        
        # Get some recent dates (last 30 days)
        end_date = datetime.now()
        date_range = pd.date_range(end=end_date, periods=30, freq='D')
        
        for user_key in sample_users:
            # Generate 3-5 random days of logs for this user
            num_days = np.random.randint(3, 6)
            selected_dates = np.random.choice(date_range, size=num_days, replace=False)
            
            for log_date in selected_dates:
                date_str = pd.Timestamp(log_date).strftime('%Y-%m-%d')
                date_key = self.date_lookup_rev.get(date_str)
                
                if not date_key:
                    continue
                
                # generate 3-5 meal entries per day
                num_meals = np.random.randint(3, 6)
                meal_types = np.random.choice(['breakfast', 'lunch', 'dinner', 'snack'], 
                                             size=num_meals, replace=True)
                
                for meal_type in meal_types:
                    # random food item
                    food_key = np.random.choice(food_items.index)
                    food_row = food_items.loc[food_key]
                    
                    # random serving size
                    serving_size = round(np.random.uniform(0.5, 3.0), 2)
                    
                    # calculate totals (handle missing values)
                    calories = food_row.get('calories', 0) if pd.notna(food_row.get('calories', 0)) else 0
                    protein = food_row.get('protein', 0) if pd.notna(food_row.get('protein', 0)) else 0
                    carbs = food_row.get('carbs', 0) if pd.notna(food_row.get('carbs', 0)) else 0
                    fats = food_row.get('fats', 0) if pd.notna(food_row.get('fats', 0)) else 0
                    
                    nutrition_logs.append({
                        'UserKey': user_key,
                        'DateKey': date_key,
                        'MealTypeKey': meal_type_lookup[meal_type],
                        'FoodKey': food_key,
                        'ServingSize': serving_size,
                        'TotalCalories': round(calories * serving_size, 2),
                        'TotalProtein': round(protein * serving_size, 2),
                        'TotalCarbs': round(carbs * serving_size, 2),
                        'TotalFats': round(fats * serving_size, 2)
                    })
        
        if nutrition_logs:
            self.warehouse_data['Fact_NutritionLog'] = pd.DataFrame(nutrition_logs)
            logger.info(f"Created Fact_NutritionLog: {len(nutrition_logs)} sample records")
            logger.info("NOTE: This is sample data. In production, replace with actual meal log extraction.")
        else:
            self.warehouse_data['Fact_NutritionLog'] = pd.DataFrame()
            logger.warning("No nutrition logs were generated.")

    # LOAD 
    def create_database_schema(self):
        """
        Executes the external db_schema.sql file to create the warehouse.
        """
        logger.info("Executing db_schema.sql to create Data Warehouse schema...")
        try:
            schema_path = Path('db_schema.sql')
            if not schema_path.exists():
                logger.error("db_schema.sql not found! Cannot create schema.")
                raise FileNotFoundError("db_schema.sql not found")
                
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            # Split the schema into individual statements
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            with self.engine.connect() as connection:

                # turn off foreign key check to avoid error when running DROP TABLE IF EXISTS
                connection.execute(text("SET FOREIGN_KEY_CHECKS=0;"))

                for statement in statements:
                    if statement:
                        # Skip comments
                        if statement.startswith('--'):
                            continue
                        connection.execute(text(statement))

                connection.execute(text("SET FOREIGN_KEY_CHECKS=1;")) # turn FK check back on

                connection.commit()

            logger.info("Data Warehouse schema created successfully")
        except Exception as e:
            logger.error(f"Error creating MySQL database schema: {e}")
            raise
    
    def load_data_to_database(self):
        """Load all transformed DataFrames into the Data Warehouse."""
        logger.info("Loading data to Data Warehouse...")
        
        # Define the correct loading order (Dims -> Bridges -> Facts)
        load_order = [
            'Dim_Date', 'Dim_User', 'Dim_FitnessGoal', 'Dim_FitnessType', 
            'Dim_HealthCondition', 'Dim_Exercise', 'Dim_Diet', 'Dim_FoodItem',
            'Dim_MetricType', 'Dim_WorkoutType', 'Dim_MealType',
            'Bridge_User_HealthCondition', 'Bridge_User_WorkoutPreference', 
            'Bridge_User_DietPreference',
            'Fact_UserSnapshot', 'Fact_WorkoutSession', 'Fact_HealthMetric',
            'Fact_NutritionLog']
        
        try:
            with self.engine.connect() as connection:
                for table_name in load_order:
                    if table_name in self.warehouse_data:
                        df = self.warehouse_data[table_name]
                        if not df.empty:
                            logger.info(f"Loading {table_name} ({len(df)} records)...")
                            # Get table name from DataFrame name (e.g., Dim_User -> dim_user)
                            sql_table_name = table_name.lower()
                            
                            # 'index=True' is needed for Dims where the key is the index
                            use_index = table_name.startswith('Dim_')
                            
                            df.to_sql(
                                sql_table_name, 
                                connection, 
                                if_exists='append', 
                                index=use_index
                            )
                        else:
                            logger.info(f"Skipping {table_name} (empty DataFrame)")
                    else:
                        logger.warning(f"Table {table_name} not found in processed data. Skipping.")
            
            logger.info("Data Warehouse loading complete.")
            
        except Exception as e:
            logger.error(f"Error loading data to MySQL database: {e}")
            raise

    # VALIDATE & RUN
    def validate_data_quality(self):
        """Perform data quality validation on the new data warehouse"""
        logger.info("Performing data quality validation...")
        validation_results = {}
        
        try:
            with self.engine.connect() as connection:
                # Check record counts
                tables = ['Dim_User', 'Fact_UserSnapshot', 'Fact_WorkoutSession', 'Fact_HealthMetric', 'Fact_NutritionLog']
                for table in tables:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table.lower()}"))
                    count = result.fetchone()[0]
                    validation_results[f"{table}_count"] = count
                    logger.info(f"{table}: {count} records")
                
                # Check that bridges were populated
                result = connection.execute(text("SELECT COUNT(*) FROM bridge_user_healthcondition"))
                bridge_count = result.fetchone()[0]
                validation_results['bridge_healthcondition_count'] = bridge_count
                logger.info(f"Bridge_User_HealthCondition: {bridge_count} records")

        except Exception as e:
            logger.error(f"Error during data validation: {e}")
        
        return validation_results

    def generate_summary_report(self, validation_results):
        """Generate summary report for the ETL process"""
        logger.info("Generating summary report...")
        report = {
            'etl_timestamp': datetime.now().isoformat(),
            'data_sources_processed': list(self.data_sources.keys()),
            'total_users_mapped': len(self.user_mapping),
            'validation_results': validation_results
        }
        
        report_path = self.config['DATA_PATHS']['output_path'] / f"etl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Summary report saved to {report_path}")

    def run_full_etl_pipeline(self):
        """Execute the complete ETL pipeline."""
        logger.info("Starting full Data Warehouse ETL pipeline...")
        
        try:
            # 1. Setup db connection
            self.setup_database_connection()
            
            # 2. Extract
            self.extract_fitbit_data()
            self.extract_gym_members_data()
            self.extract_mendeley_health_data()
            self.extract_nutrition_data()
            
            # 3. Transform
            self.transform_data()
            
            # 4. Load (Schema -> data)
            self.create_database_schema()
            self.load_data_to_database()
            
            # 5. Validate & Report
            validation_results = self.validate_data_quality()
            self.generate_summary_report(validation_results)
            
            logger.info("Data Warehouse ETL pipeline completed successfully.")
            
        except Exception as e:
            logger.error(f"ETL pipeline FAILED: {e}", exc_info=True)
            raise


# MAIN EXECUTION
def main():
    """Main func to run the ETL pipeline"""
    
    # use the config from confic.py
    try:
        import config as cfg
        
        config = {
            'DATABASE_CONFIG': cfg.DATABASE_CONFIG,
            'DATA_PATHS': cfg.DATA_PATHS,
            'ETL_CONFIG': cfg.ETL_CONFIG,
            'FITNESS_GOALS': cfg.FITNESS_GOALS,
            'QUALITY_THRESHOLDS': cfg.QUALITY_THRESHOLDS
        }
        
        # create output directory
        Path(config['DATA_PATHS']['output_path']).mkdir(parents=True, exist_ok=True)
        
        etl = FitnessNutritionETL(config)
        etl.run_full_etl_pipeline()
        
    except ImportError:
        logger.error("config.py not found. Please create it with your credentials.")
    except Exception as e:
        logger.error(f"An error occurred during pipeline execution: {e}")

if __name__ == "__main__":
    main()