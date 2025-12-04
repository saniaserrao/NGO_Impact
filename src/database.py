import pandas as pd
import sqlite3
from pathlib import Path
from typing import Dict, Optional


class DatabaseManager:
    
    def __init__(self, db_path = 'outputs/nonprofit_grants.db'):
        self.db_path = db_path
        self.conn = None
        
    def create_database(self, csv_folder = 'data') -> sqlite3.Connection:
  
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.conn = sqlite3.connect(self.db_path)
        print(f"Creating database at: {self.db_path}\n")
        
        csv_files = {
            'grants': 'grants.csv',
            'grants_final': 'grants_final.csv',
            'non_profits': 'non-profits.csv',
            'non_profits_final': 'non-profits_final.csv',
            'nonprofit_anomalies': 'nonprofit_anomalies.csv',
            'nonprofit_quality': 'nonprofit_quality.csv'
        }
        
        for table_name, filename in csv_files.items():
            filepath = Path(csv_folder) / filename
            
            if filepath.exists():
                print(f"Loading {filename}...")
                
                df = pd.read_csv(filepath, low_memory=False)
                df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                
                print(f"\nTable '{table_name}': {len(df):,} rows, {len(df.columns)} columns")
            else:
                print(f"\nWarning: {filename} not found in {csv_folder}/")
        
        print(f"\nDatabase created successfully!\n")
        
        return self.conn
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        
        return pd.read_sql_query(query, self.conn)
    
    def get_table_info(self, table_name: str) -> Dict:
        """Get metadata about a specific table"""
        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        row_count = self.execute_query(count_query).iloc[0]['count']
        
        # Get column info
        col_query = f"PRAGMA table_info({table_name})"
        columns = self.execute_query(col_query)
        
        # Get sample data
        sample_query = f"SELECT * FROM {table_name} LIMIT 5"
        sample_data = self.execute_query(sample_query)
        
        return {
            'row_count': row_count,
            'columns': columns,
            'sample_data': sample_data
        }
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed")


# ============================================================================
# SQL QUERY TEMPLATES - Pre-built queries for analysis
# ============================================================================

class SQLQueries:
    """Collection of SQL queries for nonprofit grant analysis"""
    
    @staticmethod
    def top_grants_by_funding(limit: int = 10) -> str:
        """Get grants with highest funding amounts"""
        return f"""
        SELECT 
            opportunity_title,
            agency_name,
            award_ceiling,
            award_floor,
            estimated_total_program_funding,
            close_date
        FROM grants
        WHERE award_ceiling IS NOT NULL
        ORDER BY award_ceiling DESC
        LIMIT {limit}
        """
    
    @staticmethod
    def nonprofits_by_state() -> str:
        """Analyze nonprofit distribution across states"""
        return """
        SELECT 
            STATE,
            COUNT(*) as org_count,
            AVG(INCOME_AMT) as avg_income,
            AVG(ASSET_AMT) as avg_assets,
            AVG(REVENUE_AMT) as avg_revenue,
            SUM(INCOME_AMT) as total_income
        FROM non_profits
        WHERE STATE IS NOT NULL AND STATE != ''
        GROUP BY STATE
        ORDER BY org_count DESC
        LIMIT 20
        """
    
    @staticmethod
    def impact_by_classification() -> str:
        """Analyze impact scores by nonprofit classification"""
        return """
        SELECT 
            CLASSIFICATION,
            COUNT(*) as org_count,
            AVG(impact_score_numeric) as avg_impact,
            MIN(impact_score_numeric) as min_impact,
            MAX(impact_score_numeric) as max_impact,
            AVG(financial_metric) as avg_financial,
            AVG(impact_efficiency) as avg_efficiency
        FROM non_profits_final
        WHERE impact_score_numeric IS NOT NULL
        GROUP BY CLASSIFICATION
        HAVING org_count >= 10
        ORDER BY avg_impact DESC
        """
    
    @staticmethod
    def anomaly_summary() -> str:
        """Summarize detected anomalies"""
        return """
        SELECT 
            anomaly_type,
            risk_level,
            COUNT(*) as anomaly_count,
            AVG(anomaly_score) as avg_anomaly_score,
            MIN(anomaly_score) as min_score,
            MAX(anomaly_score) as max_score
        FROM nonprofit_anomalies
        WHERE is_anomalous = 1
        GROUP BY anomaly_type, risk_level
        ORDER BY anomaly_count DESC
        """
    
    @staticmethod
    def data_quality_overview() -> str:
        """Analyze data quality metrics"""
        return """
        SELECT 
            data_quality,
            COUNT(*) as org_count,
            AVG(confidence_score) as avg_confidence,
            SUM(CASE WHEN has_mission = 1 THEN 1 ELSE 0 END) as orgs_with_mission,
            SUM(CASE WHEN has_financial = 1 THEN 1 ELSE 0 END) as orgs_with_financial,
            SUM(CASE WHEN has_impact = 1 THEN 1 ELSE 0 END) as orgs_with_impact,
            ROUND(AVG(CASE WHEN has_mission = 1 THEN 1 ELSE 0 END) * 100, 2) as mission_pct
        FROM nonprofit_quality
        GROUP BY data_quality
        ORDER BY 
            CASE data_quality
                WHEN 'high' THEN 1
                WHEN 'medium' THEN 2
                WHEN 'low' THEN 3
                ELSE 4
            END
        """
    
    @staticmethod
    def top_performers(limit: int = 25) -> str:
        """Get top performing nonprofits by impact score"""
        return f"""
        SELECT 
            nf.NAME,
            nf.STATE,
            nf.CITY,
            nf.CLASSIFICATION,
            nf.impact_score_numeric,
            nf.financial_metric,
            nf.impact_efficiency,
            nf.INCOME_AMT,
            nf.ASSET_AMT,
            nq.confidence_score
        FROM non_profits_final nf
        LEFT JOIN nonprofit_quality nq ON nf.EIN = nq.EIN
        WHERE nf.impact_score_numeric IS NOT NULL
        ORDER BY nf.impact_score_numeric DESC
        LIMIT {limit}
        """
    
    @staticmethod
    def funding_by_category() -> str:
        """Analyze grants by funding category"""
        return """
        SELECT 
            opportunity_category,
            COUNT(*) as grant_count,
            AVG(award_ceiling) as avg_award,
            MAX(award_ceiling) as max_award,
            SUM(estimated_total_program_funding) as total_funding_available,
            COUNT(CASE WHEN close_date >= date('now') THEN 1 END) as active_grants
        FROM grants
        WHERE opportunity_category IS NOT NULL
        GROUP BY opportunity_category
        ORDER BY grant_count DESC
        LIMIT 15
        """
    
    @staticmethod
    def high_risk_organizations() -> str:
        """Identify high-risk organizations"""
        return """
        SELECT 
            nf.NAME,
            nf.STATE,
            nf.CLASSIFICATION,
            nf.impact_score_numeric,
            na.anomaly_type,
            na.risk_level,
            na.anomaly_score,
            nf.INCOME_AMT,
            nf.ASSET_AMT
        FROM non_profits_final nf
        INNER JOIN nonprofit_anomalies na ON nf.EIN = na.EIN
        WHERE na.is_anomalous = 1 AND na.risk_level = 'high'
        ORDER BY na.anomaly_score DESC
        LIMIT 50
        """
    
    @staticmethod
    def grant_nonprofit_matching() -> str:
        """
        Match nonprofits to relevant grants based on classification
        This is a simplified matching - in production you'd use more sophisticated logic
        """
        return """
        SELECT 
            g.opportunity_title,
            g.agency_name,
            g.opportunity_category,
            g.award_ceiling,
            COUNT(DISTINCT nf.EIN) as eligible_nonprofits,
            AVG(nf.impact_score_numeric) as avg_impact_of_eligible
        FROM grants g
        CROSS JOIN non_profits_final nf
        WHERE g.award_ceiling IS NOT NULL
          AND nf.impact_score_numeric IS NOT NULL
          AND g.close_date >= date('now')
        GROUP BY g.opportunity_id, g.opportunity_title, g.agency_name, 
                 g.opportunity_category, g.award_ceiling
        HAVING eligible_nonprofits > 0
        ORDER BY g.award_ceiling DESC
        LIMIT 20
        """


# ============================================================================
# TESTING FUNCTIONS
# ============================================================================

def database_creation():
    """main function for database creation and queries"""
    
    
  
    db = DatabaseManager()
    db.create_database()
    
    # Test each table
    tables = ['grants', 'grants_final', 'non_profits', 'non_profits_final',
              'nonprofit_anomalies', 'nonprofit_quality']
    
    print("\nTable Metadata:")
    print("-" * 70)
    
    for table in tables:
        try:
            info = db.get_table_info(table)
            print(f"\n{table}:")
            print(f"  Rows: {info['row_count']:,}")
            print(f"  Columns: {len(info['columns'])}")
            print(f"  Sample columns: {', '.join(info['sample_data'].columns[:5].tolist())}...")
        except Exception as e:
            print(f"\n{table}: Error - {str(e)}")
    
   
    print("-" * 70)
    query = SQLQueries.nonprofits_by_state()
    result = db.execute_query(query)
    print(f"\nQuery returned {len(result)} rows")
    print("\nTop 5 states by nonprofit count:")
    print(result.head())
    
    db.close()


if __name__ == "__main__":
    database_creation()