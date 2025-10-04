"""
Neo4j Connection Setup and Verification
"""

from neo4j import GraphDatabase
import sys

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()
        
    def verify_connectivity(self):
        """Test if we can connect to Neo4j"""
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 as test")
                record = result.single()
                if record["test"] == 1:
                    print("✅ Successfully connected to Neo4j!")
                    return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def get_database_info(self):
        """Get basic stats about the database"""
        with self.driver.session() as session:
            # Count nodes
            node_count = session.run("MATCH (n) RETURN count(n) as count").single()["count"]
            
            # Count relationships
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()["count"]
            
            # Get node labels
            labels = session.run("CALL db.labels()").values()
            
            # Get relationship types
            rel_types = session.run("CALL db.relationshipTypes()").values()
            
            print(f"\n📊 Database Statistics:")
            print(f"   Nodes: {node_count}")
            print(f"   Relationships: {rel_count}")
            print(f"   Node Labels: {[label[0] for label in labels]}")
            print(f"   Relationship Types: {[rel[0] for rel in rel_types]}")

def main():
    """Main setup function"""
    print("=" * 60)
    print("Neo4j Connection Setup")
    print("=" * 60)
    
    # Get credentials from user
    print("\nEnter your Neo4j Aura credentials:")
    print("(Find these in your Neo4j Aura dashboard)")
    
    uri = input("URI (e.g., neo4j+s://xxxxx.databases.neo4j.io): ").strip()
    user = input("Username (usually 'neo4j'): ").strip() or "neo4j"
    password = input("Password: ").strip()
    
    # Test connection
    conn = Neo4jConnection(uri, user, password)
    
    if conn.verify_connectivity():
        conn.get_database_info()
        
        # Save credentials for notebooks
        with open('.env', 'w') as f:
            f.write(f"NEO4J_URI={uri}\n")
            f.write(f"NEO4J_USER={user}\n")
            f.write(f"NEO4J_PASSWORD={password}\n")
        print("\n✅ Credentials saved to .env file")
        print("You can now use the Jupyter notebooks!")
    else:
        print("\n❌ Setup failed. Please check your credentials and try again.")
        sys.exit(1)
    
    conn.close()

if __name__ == "__main__":
    main()