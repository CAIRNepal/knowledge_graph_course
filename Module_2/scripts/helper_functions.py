"""
Helper functions for KG querying demos
"""

from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
from tabulate import tabulate

class QueryHelper:
    """Utilities for executing and displaying queries"""
    
    @staticmethod
    def run_neo4j_query(driver, query, parameters=None):
        """Execute Cypher query and return results as DataFrame"""
        with driver.session() as session:
            result = session.run(query, parameters or {})
            records = [dict(record) for record in result]
            return pd.DataFrame(records) if records else pd.DataFrame()
    
    @staticmethod
    def run_sparql_query(endpoint, query):
        """Execute SPARQL query and return results as DataFrame"""
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = sparql.query().convert()
            
            # Parse results
            bindings = results["results"]["bindings"]
            if not bindings:
                return pd.DataFrame()
            
            # Convert to DataFrame
            data = []
            for binding in bindings:
                row = {k: v.get("value", "") for k, v in binding.items()}
                data.append(row)
            
            return pd.DataFrame(data)
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def display_results(df, title="Query Results", max_rows=10):
        """Pretty print DataFrame results"""
        print("\n" + "=" * 80)
        print(f"📊 {title}")
        print("=" * 80)
        
        if df.empty:
            print("No results found.")
            return
        
        # Truncate long strings
        display_df = df.head(max_rows).copy()
        for col in display_df.columns:
            if display_df[col].dtype == 'object':
                display_df[col] = display_df[col].apply(
                    lambda x: str(x)[:50] + "..." if len(str(x)) > 50 else str(x)
                )
        
        print(tabulate(display_df, headers='keys', tablefmt='psql', showindex=False))
        
        if len(df) > max_rows:
            print(f"\n... and {len(df) - max_rows} more rows")
        
        print(f"\nTotal rows: {len(df)}")
    
    @staticmethod
    def format_query(query):
        """Format query for display"""
        print("\n" + "─" * 80)
        print("🔍 Executing Query:")
        print("─" * 80)
        print(query.strip())
        print("─" * 80)

def load_neo4j_credentials():
    """Load Neo4j credentials from .env file"""
    credentials = {}
    try:
        with open('.env', 'r') as f:
            for line in f:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    credentials[key] = value
        return credentials
    except FileNotFoundError:
        print("❌ .env file not found. Run setup_neo4j.py first!")
        return None