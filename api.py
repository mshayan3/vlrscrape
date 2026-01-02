"""
Flask API for VALORANT Champions 2025 Chatbot
Provides REST API endpoints for the web frontend
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from dotenv import load_dotenv

from schema_context import get_schema_context
from query_engine import QueryEngine, clean_sql_from_llm_response

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for GitHub Pages

# Initialize components
query_engine = QueryEngine()

# Initialize LLM client
provider = os.getenv('LLM_PROVIDER', 'openai').lower()
if provider == 'openai':
    from openai import OpenAI
    llm_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
elif provider == 'anthropic':
    from anthropic import Anthropic
    llm_client = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
    model = os.getenv('ANTHROPIC_MODEL', 'claude-3-5-sonnet-20241022')

# System prompt
system_prompt = f"""You are a helpful AI assistant for querying VALORANT Champions 2025 data.

{get_schema_context()}

Generate ONLY the SQL query to answer the user's question. Return it in a ```sql``` code block.
Use JOINs for human-readable names. Only SELECT queries allowed.
"""


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'message': 'API is running'}), 200


@app.route('/api/chat', methods=['POST'])
def chat():
    """
    Main chat endpoint
    Expects: { "query": "user question" }
    Returns: { "success": bool, "sql": str, "results": list, "columns": list, "error": str }
    """
    try:
        data = request.get_json()
        
        if not data or 'query' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing query parameter'
            }), 400
        
        user_query = data['query']
        
        # Generate SQL using LLM
        if provider == 'openai':
            response = llm_client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_query}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            llm_response = response.choices[0].message.content
        
        elif provider == 'anthropic':
            response = llm_client.messages.create(
                model=model,
                max_tokens=1000,
                temperature=0.1,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_query}
                ]
            )
            llm_response = response.content[0].text
        
        # Extract SQL
        sql = clean_sql_from_llm_response(llm_response)
        
        # Execute query
        result = query_engine.execute_query(sql)
        
        if result['success']:
            return jsonify({
                'success': True,
                'sql': sql,
                'results': result['data'],
                'columns': result['columns'],
                'row_count': result['row_count']
            })
        else:
            return jsonify({
                'success': False,
                'error': result['error'],
                'sql': sql
            }), 400
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/tables', methods=['GET'])
def list_tables():
    """List all tables in the database"""
    try:
        tables = query_engine.list_tables()
        return jsonify({
            'success': True,
            'tables': tables
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    print("=" * 60)
    print("🎮 VALORANT Champions 2025 API Server")
    print("=" * 60)
    print(f"\nLLM Provider: {provider}")
    print(f"Model: {model}")
    print("\nAPI Endpoints:")
    print("  • GET  /api/health  - Health check")
    print("  • POST /api/chat    - Chat query")
    print("  • GET  /api/tables  - List tables")
    print("\nStarting server on http://localhost:5000")
    print("=" * 60)
    print()
    
    app.run(host='0.0.0.0', port=5000, debug=True)
