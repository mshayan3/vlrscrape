"""
SQL Query Engine for RAG Chatbot.
Handles natural language to SQL translation, query execution, and result formatting.
"""
import sqlite3
import re
from typing import Dict, List, Any, Optional, Tuple

class QueryEngine:
    """Executes SQL queries against the Valorant database with safety checks."""
    
    def __init__(self, db_path: str = "valorant_champions_2025.db"):
        """Initialize query engine with database path."""
        self.db_path = db_path
        self.allowed_operations = ['SELECT']  # Only allow SELECT queries for safety
        
    def validate_query(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Validate SQL query for safety.
        Returns (is_valid, error_message)
        """
        sql_upper = sql.upper().strip()
        
        # Check for dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return False, f"Query contains forbidden operation: {keyword}"
        
        # Must start with SELECT
        if not sql_upper.startswith('SELECT'):
            return False, "Only SELECT queries are allowed"
        
        # Check for semicolon injection (multiple statements)
        if sql.count(';') > 1:
            return False, "Multiple statements not allowed"
        
        return True, None
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query and return results.
        Returns dict with 'success', 'data', 'columns', 'error' keys.
        """
        # Validate query first
        is_valid, error = self.validate_query(sql)
        if not is_valid:
            return {
                'success': False,
                'error': error,
                'data': [],
                'columns': []
            }
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            cursor = conn.cursor()
            
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            # Get column names
            columns = [description[0] for description in cursor.description] if cursor.description else []
            
            # Convert rows to list of dicts
            data = [dict(row) for row in rows]
            
            conn.close()
            
            return {
                'success': True,
                'data': data,
                'columns': columns,
                'row_count': len(data),
                'error': None
            }
            
        except sqlite3.Error as e:
            return {
                'success': False,
                'error': f"Database error: {str(e)}",
                'data': [],
                'columns': []
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}",
                'data': [],
                'columns': []
            }
    
    def format_results(self, results: Dict[str, Any], max_rows: int = 50) -> str:
        """
        Format query results as a human-readable string.
        """
        if not results['success']:
            return f"❌ Error: {results['error']}"
        
        data = results['data']
        columns = results['columns']
        
        if not data:
            return "No results found."
        
        # Limit rows
        if len(data) > max_rows:
            data = data[:max_rows]
            truncated = True
        else:
            truncated = False
        
        # Build table
        output = []
        
        # Header
        header = " | ".join(columns)
        separator = "-" * len(header)
        output.append(header)
        output.append(separator)
        
        # Rows
        for row in data:
            row_str = " | ".join(str(row.get(col, '')) for col in columns)
            output.append(row_str)
        
        # Footer
        if truncated:
            output.append(f"\n(Showing first {max_rows} of {results['row_count']} results)")
        else:
            output.append(f"\n({results['row_count']} results)")
        
        return "\n".join(output)
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a specific table."""
        sql = f"PRAGMA table_info({table_name});"
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = cursor.fetchall()
            conn.close()
            
            return {
                'success': True,
                'table': table_name,
                'columns': [
                    {
                        'name': col[1],
                        'type': col[2],
                        'not_null': bool(col[3]),
                        'primary_key': bool(col[5])
                    }
                    for col in columns
                ]
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        result = self.execute_query(sql)
        
        if result['success']:
            return [row['name'] for row in result['data']]
        return []


def clean_sql_from_llm_response(response: str) -> str:
    """
    Extract SQL query from LLM response.
    Handles markdown code blocks and extra text.
    """
    # Remove markdown code blocks
    sql = re.sub(r'```sql\s*', '', response)
    sql = re.sub(r'```\s*', '', sql)
    
    # Remove common prefixes
    sql = re.sub(r'^(Here\'s the SQL query:|SQL:|Query:)\s*', '', sql, flags=re.IGNORECASE)
    
    # Get first complete SQL statement
    sql = sql.strip()
    
    # If multiple lines, try to find SELECT ... ; pattern
    if '\n' in sql:
        lines = sql.split('\n')
        sql_lines = []
        in_query = False
        
        for line in lines:
            line = line.strip()
            if line.upper().startswith('SELECT'):
                in_query = True
            if in_query:
                sql_lines.append(line)
            if ';' in line:
                break
        
        sql = ' '.join(sql_lines)
    
    # Ensure ends with semicolon
    if not sql.endswith(';'):
        sql += ';'
    
    return sql
