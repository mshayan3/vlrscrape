# Chatbot Setup Guide

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure API Key
```bash
# Copy the template
cp .env.template .env

# Edit .env and add your API key
# For OpenAI: Get key from https://platform.openai.com/api-keys
# For Anthropic: Get key from https://console.anthropic.com/
```

### 3. Run the Chatbot
```bash
python chatbot.py
```

## Example Usage

```
🎮 Valorant Champions 2025 AI Chatbot
========================================

💬 You: Who are the top 5 players by ACS?

🤔 Thinking...

📝 Generated SQL:
SELECT p.player_name, t.team_name, AVG(pms.acs) as avg_acs, COUNT(*) as maps_played
FROM player_map_stats pms
JOIN players p ON pms.player_id = p.player_id
JOIN teams t ON pms.team_id = t.team_id
GROUP BY p.player_id
HAVING maps_played >= 3
ORDER BY avg_acs DESC
LIMIT 5;

🤖 Answer:
player_name | team_name | avg_acs | maps_played
------------------------------------------------
aspas | MIBR | 261.6 | 15
ZmjjKK | EDG | 237.4 | 5
SpiritZ1 | DRG | 237.0 | 4
jawgemo | G2 Esports | 230.4 | 12
zekken | SEN | 229.3 | 6

(5 results)
```

## Commands

- `help` - Show example questions
- `reset` - Clear conversation history
- `exit` or `quit` - Exit the chatbot

## Example Questions

- "Who are the top 10 players by ACS?"
- "Show me all matches for Team Heretics"
- "What agents does aspas play most?"
- "Which team won the most rounds on Ascent?"
- "What is the average headshot percentage for Jett players?"
- "Show me the map veto for G2 vs FNATIC matches"
- "Which players have the best K/D ratio?"
- "What's the most picked agent in the tournament?"

## Configuration

Edit `.env` to customize:

```bash
# Choose LLM provider (openai or anthropic)
LLM_PROVIDER=openai

# Model selection
OPENAI_MODEL=gpt-4o-mini
ANTHROPIC_MODEL=claude-3-5-sonnet-20241022

# Query settings
MAX_RESULTS=50
```

## Troubleshooting

### "OPENAI_API_KEY not set"
- Make sure you copied `.env.template` to `.env`
- Add your actual API key (not "your_api_key_here")

### "OpenAI library not installed"
```bash
pip install openai
```

### "Query failed: Database error"
- Ensure `valorant_champions_2025.db` exists in the same directory
- Check that the database is not corrupted

## How It Works

1. **User asks a question** in natural language
2. **LLM generates SQL** using schema context and examples
3. **Query engine validates** the SQL (only SELECT allowed)
4. **Database executes** the query
5. **Results are formatted** and displayed
6. **Conversation history** is maintained for context
