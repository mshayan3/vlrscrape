# VALORANT Champions 2025 - Web Frontend

## Quick Start

### 1. Install Dependencies
```bash
pip install flask flask-cors
```

### 2. Start the API Server
```bash
python api.py
```

The API will start on `http://localhost:5000`

### 3. Open the Web Interface
Simply open `web/index.html` in your browser, or use a local server:

```bash
# Option 1: Python HTTP server
cd web
python -m http.server 8000
# Open http://localhost:8000

# Option 2: Just open the file
open web/index.html  # macOS
start web/index.html  # Windows
```

## GitHub Pages Deployment

### Deploy Frontend to GitHub Pages

1. **Push to GitHub:**
```bash
git add web/
git commit -m "Add Valorant-themed web frontend"
git push
```

2. **Enable GitHub Pages:**
   - Go to your repository settings
   - Navigate to "Pages" section
   - Source: Deploy from a branch
   - Branch: `main` → `/web` folder
   - Save

3. **Access your site:**
   - Your site will be available at: `https://YOUR_USERNAME.github.io/vlrscrape/`

### Deploy Backend API

The backend needs to run on a server. Options:

#### Option 1: Run Locally
```bash
python api.py
```
Update `web/script.js` line 7:
```javascript
API_URL: 'http://localhost:5000/api/chat'
```

#### Option 2: Deploy to Render (Free)
1. Create account at https://render.com
2. New Web Service → Connect your GitHub repo
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `python api.py`
5. Add environment variables (OPENAI_API_KEY, etc.)
6. Deploy

Update `web/script.js` with your Render URL:
```javascript
API_URL: 'https://your-app.onrender.com/api/chat'
```

#### Option 3: Deploy to Railway (Free tier)
1. Create account at https://railway.app
2. New Project → Deploy from GitHub
3. Add environment variables
4. Deploy

## Configuration

### API URL
Edit `web/script.js` line 7 to point to your API:
```javascript
const CONFIG = {
    API_URL: 'YOUR_API_URL_HERE/api/chat',
    // ...
};
```

### Environment Variables
Make sure your `.env` file has:
```bash
OPENAI_API_KEY=your_key_here
LLM_PROVIDER=openai
OPENAI_MODEL=gpt-4o-mini
DB_PATH=valorant_champions_2025.db
```

## Features

- 🎮 **Valorant-Themed Design** - Red/black color scheme with glowing effects
- 💬 **Real-time Chat** - Interactive chat interface with typing indicators
- 📊 **Table Results** - Beautiful formatted tables for query results
- 📱 **Responsive** - Works on desktop and mobile
- ⚡ **Fast** - Optimized for performance
- 🔒 **Secure** - SQL injection protection

## Troubleshooting

### "Disconnected" Status
- Make sure the API server is running (`python api.py`)
- Check that the API_URL in `script.js` is correct
- Check browser console for CORS errors

### CORS Errors
- Make sure `flask-cors` is installed
- API must be running with CORS enabled (already configured in `api.py`)

### API Quota Exceeded
- Add credits to your OpenAI account
- Or switch to Anthropic in `.env`

## Example Queries

- "Who are the top 10 players by ACS?"
- "Show me all matches for Team Heretics"
- "What agents does aspas play most?"
- "Which team won the most rounds on Ascent?"
- "What is the average headshot percentage for Jett players?"
