// ============================================
// VALORANT Champions 2025 - Chat Interface
// ============================================

// Configuration
const CONFIG = {
    API_URL: 'http://localhost:5000/api/chat', // Change this to your deployed API URL
    MAX_RETRIES: 3,
    RETRY_DELAY: 1000
};

// DOM Elements
const messagesContainer = document.getElementById('messages');
const userInput = document.getElementById('userInput');
const sendButton = document.getElementById('sendButton');
const loadingOverlay = document.getElementById('loadingOverlay');
const statusDot = document.getElementById('statusDot');
const statusText = document.getElementById('statusText');
const hintText = document.getElementById('hintText');

// State
let isProcessing = false;
let messageHistory = [];

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    checkAPIStatus();
    setupEventListeners();
    loadExampleHints();
});

// Event Listeners
function setupEventListeners() {
    sendButton.addEventListener('click', handleSendMessage);

    userInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSendMessage();
        }
    });

    userInput.addEventListener('input', () => {
        sendButton.disabled = !userInput.value.trim();
    });
}

// Check API Status
async function checkAPIStatus() {
    try {
        const response = await fetch(CONFIG.API_URL.replace('/chat', '/health'), {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        if (response.ok) {
            setStatus('connected', 'Connected');
        } else {
            setStatus('error', 'API Error');
        }
    } catch (error) {
        setStatus('error', 'Disconnected');
        console.error('API health check failed:', error);
    }
}

// Set Status Indicator
function setStatus(status, text) {
    statusDot.className = `status-dot ${status}`;
    statusText.textContent = text;
}

// Handle Send Message
async function handleSendMessage() {
    const message = userInput.value.trim();

    if (!message || isProcessing) return;

    // Add user message to chat
    addMessage(message, 'user');

    // Clear input
    userInput.value = '';
    sendButton.disabled = true;

    // Show typing indicator
    const typingId = showTypingIndicator();

    // Send to API
    isProcessing = true;
    try {
        const response = await sendToAPI(message);
        removeTypingIndicator(typingId);

        if (response.success) {
            addBotResponse(response);
        } else {
            addErrorMessage(response.error || 'Failed to process query');
        }
    } catch (error) {
        removeTypingIndicator(typingId);
        addErrorMessage('Connection error. Please check if the API server is running.');
        console.error('Error:', error);
    } finally {
        isProcessing = false;
    }
}

// Send to API
async function sendToAPI(message, retries = 0) {
    try {
        const response = await fetch(CONFIG.API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query: message })
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        if (retries < CONFIG.MAX_RETRIES) {
            await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY));
            return sendToAPI(message, retries + 1);
        }
        throw error;
    }
}

// Add Message to Chat
function addMessage(text, type) {
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${type}-message`;

    const avatar = document.createElement('div');
    avatar.className = `message-avatar ${type}-avatar`;
    avatar.textContent = type === 'user' ? 'YOU' : 'AI';

    const content = document.createElement('div');
    content.className = 'message-content';

    const messageText = document.createElement('div');
    messageText.className = 'message-text';
    messageText.innerHTML = `<p>${escapeHtml(text)}</p>`;

    content.appendChild(messageText);
    messageDiv.appendChild(avatar);
    messageDiv.appendChild(content);

    messagesContainer.appendChild(messageDiv);
    scrollToBottom();

    messageHistory.push({ type, text });
}

// Add Bot Response
function addBotResponse(response) {
    const messageDiv = document.createElement('div');
    messageDiv.className = 'message bot-message';

    const avatar = document.createElement('div');
    avatar.className = 'message-avatar bot-avatar';
    avatar.textContent = 'AI';

    const content = document.createElement('div');
    content.className = 'message-content';

    const messageText = document.createElement('div');
    messageText.className = 'message-text';

    // Add SQL query if present
    if (response.sql) {
        const sqlDiv = document.createElement('div');
        sqlDiv.className = 'sql-query';
        sqlDiv.textContent = response.sql;
        messageText.appendChild(sqlDiv);
    }

    // Add results
    if (response.results && response.results.length > 0) {
        const resultsDiv = document.createElement('div');
        resultsDiv.className = 'results-table';
        resultsDiv.appendChild(createTable(response.results, response.columns));
        messageText.appendChild(resultsDiv);

        const countP = document.createElement('p');
        countP.innerHTML = `<em>${response.results.length} result(s)</em>`;
        messageText.appendChild(countP);
    } else if (response.message) {
        const p = document.createElement('p');
        p.textContent = response.message;
        messageText.appendChild(p);
    } else {
        const p = document.createElement('p');
        p.textContent = 'No results found.';
        messageText.appendChild(p);
    }

    content.appendChild(messageText);
    messageDiv.appendChild(avatar);
    messageDiv.appendChild(content);

    messagesContainer.appendChild(messageDiv);
    scrollToBottom();
}

// Create Table from Results
function createTable(data, columns) {
    const table = document.createElement('table');

    // Header
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Body
    const tbody = document.createElement('tbody');
    data.forEach(row => {
        const tr = document.createElement('tr');
        columns.forEach(col => {
            const td = document.createElement('td');
            const value = row[col];
            td.textContent = value !== null && value !== undefined ? value : '';
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    return table;
}

// Add Error Message
function addErrorMessage(error) {
    const messageDiv = document.createElement('div');
    messageDiv.className = 'message bot-message';

    const avatar = document.createElement('div');
    avatar.className = 'message-avatar bot-avatar';
    avatar.textContent = 'AI';

    const content = document.createElement('div');
    content.className = 'message-content';

    const messageText = document.createElement('div');
    messageText.className = 'message-text';
    messageText.innerHTML = `<p><strong>❌ Error:</strong> ${escapeHtml(error)}</p>`;

    content.appendChild(messageText);
    messageDiv.appendChild(avatar);
    messageDiv.appendChild(content);

    messagesContainer.appendChild(messageDiv);
    scrollToBottom();
}

// Show Typing Indicator
function showTypingIndicator() {
    const typingDiv = document.createElement('div');
    typingDiv.className = 'message bot-message';
    typingDiv.id = 'typing-indicator';

    const avatar = document.createElement('div');
    avatar.className = 'message-avatar bot-avatar';
    avatar.textContent = 'AI';

    const content = document.createElement('div');
    content.className = 'message-content';

    const indicator = document.createElement('div');
    indicator.className = 'typing-indicator';
    indicator.innerHTML = '<div class="typing-dot"></div><div class="typing-dot"></div><div class="typing-dot"></div>';

    content.appendChild(indicator);
    typingDiv.appendChild(avatar);
    typingDiv.appendChild(content);

    messagesContainer.appendChild(typingDiv);
    scrollToBottom();

    return 'typing-indicator';
}

// Remove Typing Indicator
function removeTypingIndicator(id) {
    const indicator = document.getElementById(id);
    if (indicator) {
        indicator.remove();
    }
}

// Scroll to Bottom
function scrollToBottom() {
    messagesContainer.scrollTop = messagesContainer.scrollHeight;
}

// Escape HTML
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Load Example Hints
function loadExampleHints() {
    const hints = [
        'Try: "Who are the top 5 players by ACS?"',
        'Try: "What agents does aspas play most?"',
        'Try: "Show me all matches for Team Heretics"',
        'Try: "Which team won the most rounds?"',
        'Try: "What is the average headshot percentage?"'
    ];

    let currentHint = 0;

    setInterval(() => {
        currentHint = (currentHint + 1) % hints.length;
        hintText.textContent = hints[currentHint];
    }, 5000);
}

// Periodic API health check
setInterval(checkAPIStatus, 30000);
