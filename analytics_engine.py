import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import config as cfg

class RecommendationEngine:
    def __init__(self):
        try:
            # Connect to the Data Warehouse
            db_cfg = cfg.DATABASE_CONFIG
            conn_str = f"mysql+pymysql://{db_cfg['username']}:{db_cfg['password']}@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['database']}?charset=utf8mb4"
            self.engine = create_engine(conn_str)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to the database: {e}")

    def get_user_profile(self, user_key):
        """Fetch basic user info and their specific Goal."""
        sql = """
        SELECT u.UserKey, s.Weight, s.Height, g.GoalName, u.Age, u.Gender
        FROM Dim_User u
        JOIN Fact_UserSnapshot s ON u.UserKey = s.UserKey
        JOIN Dim_FitnessGoal g ON s.GoalKey = g.GoalKey
        WHERE u.UserKey = :user_key
        LIMIT 1
        """
        try:
            with self.engine.connect() as conn:
                user_profile = pd.read_sql(text(sql), conn, params={"user_key": user_key})
                if user_profile.empty:
                    raise ValueError(f"No profile found for UserKey {user_key}.")
                return user_profile.iloc[0]
        except SQLAlchemyError as e:
            raise RuntimeError(f"SQL error occurred while fetching user profile: {e}")

    def run_recommendations(self, user_key):

        try:
            user = self.get_user_profile(user_key)
            goal = user['GoalName'].lower()

            print(f"Generating Recommendations for User {user_key} ({goal})")
            recs = []

            # UNIVERSAL RULE-BASED LOGIC 
            # Applies to ALL users 
            recs.extend(self._run_universal_checks(user))

            # GOAL-SPECIFIC LOGIC
            if 'lose' in goal:
                recs.extend(self._logic_weight_loss(user))
            elif 'muscle' in goal or 'build' in goal:
                recs.extend(self._logic_muscle_gain(user))
            elif 'endurance' in goal:
                recs.extend(self._logic_endurance(user))
            elif 'maintain' in goal:
                recs.extend(self._logic_maintenance(user))
            else:
                recs.append(f"Unrecognized goal: {goal}. Defaulting to maintenance logic.")
                recs.extend(self._logic_maintenance(user))

            return recs
        except Exception as e:
            print(f"Error generating recommendations: {e}")
            return []
    
    # UNIVERSAL GATES 
    def _run_universal_checks(self, user):
        recs = []
        try:
            sql = """
            SELECT 
                (SELECT AVG(Value) FROM Fact_HealthMetric h 
                 JOIN Dim_MetricType m ON h.MetricTypeKey = m.MetricTypeKey 
                 WHERE h.UserKey = :uk AND m.MetricName = 'sleep' AND h.DateKey >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), '%Y%m%d')) as LastSleep,
                 
                (SELECT AVG(Value) FROM Fact_HealthMetric h 
                 JOIN Dim_MetricType m ON h.MetricTypeKey = m.MetricTypeKey 
                 WHERE h.UserKey = :uk AND m.MetricName = 'heart_rate' AND h.DateKey >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 3 DAY), '%Y%m%d')) as RecentHR,
                 
                (SELECT DATEDIFF(CURDATE(), MAX(d.FullDate)) FROM Fact_WorkoutSession w 
                 JOIN Dim_Date d ON w.DateKey = d.DateKey WHERE w.UserKey = :uk) as DaysSinceWorkout,
                 
                (
                  (SELECT COALESCE(SUM(TotalCalories), 0) FROM Fact_NutritionLog WHERE UserKey = :uk AND DateKey >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 2 DAY), '%Y%m%d'))
                  -
                  (SELECT COALESCE(SUM(CaloriesBurned), 0) FROM Fact_WorkoutSession WHERE UserKey = :uk AND DateKey >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 2 DAY), '%Y%m%d'))
                ) / 2 as AvgNetCalories
            """
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn, params={"uk": user["UserKey"]})
                if df.empty: return []
                data = df.iloc[0]

            # 1. The Recovery Gate
            sleep = data['LastSleep'] if pd.notna(data['LastSleep']) else 7
            hr = data['RecentHR'] if pd.notna(data['RecentHR']) else 60
            
            if sleep < 6 or hr > 75: 
                recs.append("🛑 RECOVERY GATE: High physiological stress detected (Low Sleep or High HR). Recommended Action: Zone 2 Cardio or Rest.")
            
            # 2. The Consistency Gate
            gap = data['DaysSinceWorkout'] if pd.notna(data['DaysSinceWorkout']) else 0
            if gap > 3:
                recs.append(f"⚠️ CONSISTENCY: Momentum Alert. You haven't logged a session in {int(gap)} days. Suggestion: 15-minute activation workout.")

            # 3. The Caloric Balance Gate
            bmr = 10 * user['Weight'] + 6.25 * (user['Height'] * 100) - 5 * user['Age']
            bmr += 5 if user['Gender'].lower() == 'male' else -161
            
            avg_net = data['AvgNetCalories'] if pd.notna(data['AvgNetCalories']) else bmr
            if avg_net < (bmr * 0.8): 
                recs.append("🍽️ CALORIC GATE: Undereating detected. Performance decline imminent. Increase carbohydrate intake.")

        except Exception as e:
            print(f"Warning: Universal checks failed: {e}")
        
        return recs

    # GOAL-SPECIFIC LOGIC
    def _logic_weight_loss(self, user):
        recs = []
        try:
            sql = """
            SELECT 
                COALESCE(SUM(n.TotalCalories), 0) as CaloriesIn,
                (SELECT COALESCE(SUM(CaloriesBurned), 0) FROM Fact_WorkoutSession w 
                 JOIN Dim_Date d ON w.DateKey = d.DateKey WHERE w.UserKey = :uk AND d.FullDate = CURDATE()) as CaloriesOut
            FROM Fact_NutritionLog n
            JOIN Dim_Date d ON n.DateKey = d.DateKey
            WHERE n.UserKey = :uk AND d.FullDate = CURDATE()
            """
            with self.engine.connect() as conn:
                data = pd.read_sql(text(sql), conn, params={"uk": user["UserKey"]}).iloc[0]

            # Caloric Deficit Strategy
            cal_in = data['CaloriesIn']
            cal_out = data['CaloriesOut']
            
            bmr = 10 * user['Weight'] + 6.25 * (user['Height'] * 100) - 5 * user['Age']
            if user['Gender'].lower() == 'male': bmr += 5
            else: bmr -= 161
            
            target_net = bmr + cal_out - 500 # 500 kcal deficit
            
            if cal_in > target_net + 200:
                recs.append(f"📉 WEIGHT LOSS: You are {cal_in - target_net:.0f} kcals over your deficit target.")
                recs.append("💡 TIP: Swap your next snack for an Apple (52 kcal) from our database.")
            
            # Active Minutes proxy 
            if cal_out < 200:
                 recs.append("🏃 MOVEMENT: You haven't burned enough active calories today. Try a 30-min brisk walk.")
        except Exception as e:
            recs.append(f"Error in Weight Loss logic: {e}")
        return recs

    def _logic_muscle_gain(self, user):
        recs = []
        try:
            sql = """
            SELECT COALESCE(SUM(n.TotalProtein), 0) as ProteinGrams
            FROM Fact_NutritionLog n
            JOIN Dim_Date d ON n.DateKey = d.DateKey
            WHERE n.UserKey = :uk AND d.FullDate = CURDATE()
            """
            with self.engine.connect() as conn:
                data = pd.read_sql(text(sql), conn, params={"uk": user["UserKey"]}).iloc[0]
            
            # Hypertrophy Strategy
            target_protein = user['Weight'] * 2.0
            if data['ProteinGrams'] < target_protein:
                recs.append(f"🥩 MUSCLE GAIN: Protein intake is {data['ProteinGrams']}g, but target is {target_protein:.0f}g. Add Chicken Breast to dinner.")
        except Exception as e:
            recs.append(f"Error in Muscle logic: {e}")
        return recs

    def _logic_endurance(self, user):
        recs = []
        try:
            sql = """
            SELECT AVG(Value) as CurrentHR
            FROM Fact_HealthMetric h
            JOIN Dim_MetricType m ON h.MetricTypeKey = m.MetricTypeKey
            JOIN Dim_Date d ON h.DateKey = d.DateKey
            WHERE h.UserKey = :uk AND m.MetricName = 'heart_rate'
              AND d.FullDate >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
            """
            with self.engine.connect() as conn:
                data = pd.read_sql(text(sql), conn, params={"uk": user["UserKey"]}).iloc[0]
            
            # Performance Strategy
            current_hr = data['CurrentHR'] if pd.notna(data['CurrentHR']) else 60
            if current_hr > 65: # Simplified baseline + 10%
                recs.append(f"❤️ ENDURANCE: Resting HR elevated ({current_hr:.0f} bpm). Switch tomorrow's Long Run to Yoga.")
        except Exception as e:
            recs.append(f"Error in Endurance logic: {e}")
        return recs

    def _logic_maintenance(self, user):
        recs = []
        try:
            sql = """
            SELECT DATEDIFF(CURDATE(), MAX(d.FullDate)) as DaysSinceWorkout
            FROM Fact_WorkoutSession w
            JOIN Dim_Date d ON w.DateKey = d.DateKey
            WHERE w.UserKey = :user_key
            """
            with self.engine.connect() as conn:
                result = pd.read_sql(text(sql), conn, params={"user_key": user["UserKey"]})
                
                if not result.empty and pd.notna(result.iloc[0]['DaysSinceWorkout']):
                    days_gap = int(result.iloc[0]['DaysSinceWorkout'])
                else:
                    days_gap = 10 # Default large gap if no workouts found

            if days_gap > 3:
                recs.append(f"🗓️ CONSISTENCY: It's been {days_gap} days since your last activity. Just walk 15 mins today.")
            else:
                recs.append("✅ MAINTENANCE: Consistency is good. Keep hitting your daily step targets.")
                
        except SQLAlchemyError as e:
            recs.append(f"SQL Error for Maintenance Logic: {e}")
        return recs

if __name__ == "__main__":
    engine = RecommendationEngine()
    try:
        # Test User 1
        recommendations = engine.run_recommendations(user_key=1)
        if not recommendations: print("✅ No alerts today.")
        else: 
            for r in recommendations: print(f" - {r}")
    except Exception as e:
        print(f"Error: {e}")