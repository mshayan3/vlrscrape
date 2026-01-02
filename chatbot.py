"""
RAG-based AI Chatbot for Valorant Champions 2025 Database.
Translates natural language questions into SQL queries and provides conversational responses.
"""
import os
import sys
from typing import Optional, List, Dict
from dotenv import load_dotenv

from schema_context import get_schema_context, get_sample_queries
from query_engine import QueryEngine, clean_sql_from_llm_response

# Load environment variables
load_dotenv()

class ValorantChatbot:
    """RAG-based chatbot for querying Valorant Champions 2025 data."""
    
    def __init__(self):
        """Initialize chatbot with LLM client and query engine."""
        self.provider = os.getenv('LLM_PROVIDER', 'openai').lower()
        self.db_path = os.getenv('DB_PATH', 'valorant_champions_2025.db')
        self.max_results = int(os.getenv('MAX_RESULTS', '50'))
        
        # Initialize query engine
        self.query_engine = QueryEngine(self.db_path)
        
        # Initialize LLM client
        self._init_llm()
        
        # Conversation history
        self.conversation_history: List[Dict[str, str]] = []
        
        # System prompt
        self.system_prompt = self._build_system_prompt()
    
    def _init_llm(self):
        """Initialize LLM client based on provider."""
        if self.provider == 'openai':
            try:
                from openai import OpenAI
                api_key = os.getenv('OPENAI_API_KEY')
                if not api_key or api_key == 'your_api_key_here':
                    raise ValueError("OPENAI_API_KEY not set in .env file")
                self.client = OpenAI(api_key=api_key)
                self.model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
            except ImportError:
                print("❌ OpenAI library not installed. Run: pip install openai")
                sys.exit(1)
        
        elif self.provider == 'anthropic':
            try:
                from anthropic import Anthropic
                api_key = os.getenv('ANTHROPIC_API_KEY')
                if not api_key or api_key == 'your_api_key_here':
                    raise ValueError("ANTHROPIC_API_KEY not set in .env file")
                self.client = Anthropic(api_key=api_key)
                self.model = os.getenv('ANTHROPIC_MODEL', 'claude-3-5-sonnet-20241022')
            except ImportError:
                print("❌ Anthropic library not installed. Run: pip install anthropic")
                sys.exit(1)
        else:
            raise ValueError(f"Unknown LLM provider: {self.provider}")
    
    def _build_system_prompt(self) -> str:
        """Build system prompt with schema context."""
        schema = get_schema_context()
        samples = get_sample_queries()
        
        sample_text = "\n\n## Example Queries\n\n"
        for key, example in samples.items():
            sample_text += f"**Question:** {example['question']}\n"
            sample_text += f"**SQL:**\n```sql\n{example['sql'].strip()}\n```\n\n"
        
        return f"""You are a helpful AI assistant that helps users query a Valorant Champions 2025 esports database.

{schema}

{sample_text}

## Your Task
1. When the user asks a question, generate a SQL query to answer it
2. Return ONLY the SQL query, wrapped in ```sql``` code blocks
3. Use JOINs to get human-readable names (team names, player names, etc.)
4. Only use SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
5. Be precise with table and column names as shown in the schema
6. Use LIMIT to prevent returning too many rows (default: 50)
7. For percentage columns (kast, hs_pct), values are stored as decimals (e.g., 75.5 not 0.755)

## Important
- All IDs are TEXT (slugified), not integers
- Use LIKE with wildcards for partial name matches (e.g., WHERE team_name LIKE '%Heretics%')
- When calculating averages for players, consider using HAVING to filter by minimum maps played
- Always include relevant context in results (team names, player names, not just IDs)
"""
    
    def _call_llm(self, user_message: str) -> str:
        """Call LLM with user message and return response."""
        if self.provider == 'openai':
            messages = [
                {"role": "system", "content": self.system_prompt}
            ]
            
            # Add conversation history
            for msg in self.conversation_history[-6:]:  # Last 3 exchanges
                messages.append(msg)
            
            messages.append({"role": "user", "content": user_message})
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.1,  # Low temperature for more deterministic SQL
                max_tokens=1000
            )
            
            return response.choices[0].message.content
        
        elif self.provider == 'anthropic':
            messages = []
            
            # Add conversation history
            for msg in self.conversation_history[-6:]:
                messages.append({
                    "role": msg["role"],
                    "content": msg["content"]
                })
            
            messages.append({"role": "user", "content": user_message})
            
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                temperature=0.1,
                system=self.system_prompt,
                messages=messages
            )
            
            return response.content[0].text
    
    def ask(self, question: str) -> str:
        """
        Process user question and return answer.
        """
        print(f"\n🤔 Thinking...")
        
        # Get SQL query from LLM
        llm_response = self._call_llm(question)
        
        # Extract SQL from response
        sql = clean_sql_from_llm_response(llm_response)
        
        print(f"\n📝 Generated SQL:\n{sql}\n")
        
        # Execute query
        results = self.query_engine.execute_query(sql)
        
        if not results['success']:
            error_msg = f"❌ Query failed: {results['error']}"
            print(error_msg)
            return error_msg
        
        # Format results
        formatted_results = self.query_engine.format_results(results, self.max_results)
        
        # Update conversation history
        self.conversation_history.append({"role": "user", "content": question})
        self.conversation_history.append({"role": "assistant", "content": llm_response})
        
        return formatted_results
    
    def reset_conversation(self):
        """Clear conversation history."""
        self.conversation_history = []
        print("🔄 Conversation history cleared.")


def main():
    """Main CLI interface for the chatbot."""
    print("=" * 60)
    print("🎮 Valorant Champions 2025 AI Chatbot")
    print("=" * 60)
    print("\nAsk questions about Valorant Champions 2025 data!")
    print("Type 'exit' or 'quit' to end the conversation.")
    print("Type 'reset' to clear conversation history.")
    print("Type 'help' for example questions.\n")
    
    try:
        chatbot = ValorantChatbot()
    except Exception as e:
        print(f"\n❌ Failed to initialize chatbot: {e}")
        print("\nMake sure you:")
        print("1. Copy .env.template to .env")
        print("2. Add your API key to .env")
        print("3. Install dependencies: pip install -r requirements.txt")
        sys.exit(1)
    
    while True:
        try:
            question = input("\n💬 You: ").strip()
            
            if not question:
                continue
            
            if question.lower() in ['exit', 'quit', 'q']:
                print("\n👋 Goodbye!")
                break
            
            if question.lower() == 'reset':
                chatbot.reset_conversation()
                continue
            
            if question.lower() == 'help':
                print("\n📚 Example questions:")
                print("  • Who are the top 10 players by ACS?")
                print("  • Show me all matches for Team Heretics")
                print("  • What agents does aspas play most?")
                print("  • Which team won the most rounds on Ascent?")
                print("  • What is the average headshot percentage for Jett players?")
                print("  • Show me the map veto for G2 vs FNATIC matches")
                continue
            
            # Get answer
            answer = chatbot.ask(question)
            print(f"\n🤖 Answer:\n{answer}")
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()
