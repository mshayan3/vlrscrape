// app.js

document.addEventListener('DOMContentLoaded', () => {
    loadVetoData();
    loadScoreboardData();
    loadEconomyData();
});

function loadVetoData() {
    Papa.parse('data/map_veto.csv', {
        download: true,
        header: true,
        complete: function (results) {
            const data = results.data;
            const container = document.getElementById('veto-container');
            container.innerHTML = '';

            data.forEach(row => {
                if (!row.map) return;

                const card = document.createElement('div');
                card.className = 'veto-card';

                let content = `<strong>${row.map}</strong><br>`;

                if (row.pick) {
                    card.classList.add('pick');
                    content += `Pick: ${row.pick}`;
                } else if (row.ban) {
                    card.classList.add('ban');
                    content += `Ban: ${row.ban}`;
                } else if (row.map) {
                    content += `Decider`;
                }

                card.innerHTML = content;
                container.appendChild(card);
            });
        }
    });
}

function loadScoreboardData() {
    Papa.parse('data/player_stats.csv', {
        download: true,
        header: true,
        complete: function (results) {
            const data = results.data;
            const tableHead = document.querySelector('#scoreboard-table thead');
            const tableBody = document.querySelector('#scoreboard-table tbody');

            // Setup Headers (Simplified)
            const headers = ['Player', 'Team', 'Agent', 'ACS', 'K', 'D', 'A', 'K/D'];
            let headerRow = '<tr>';
            headers.forEach(h => headerRow += `<th>${h}</th>`);
            headerRow += '</tr>';
            tableHead.innerHTML = headerRow;

            // Setup Rows
            tableBody.innerHTML = '';
            data.forEach(row => {
                if (!row.Player || row.Map !== "All_Maps" || row.Side !== "All") return;

                const tr = document.createElement('tr');
                const kdClass = parseFloat(row['K/D']) >= 0 ? 'positive-kd' : 'negative-kd';
                const kdText = parseFloat(row['K/D']) > 0 ? `+${row['K/D']}` : row['K/D'];

                // Clean up agents string
                const agents = row.Agents ? row.Agents.split(',')[0] : 'N/A'; // Just show first agent for MVP

                tr.innerHTML = `
                    <td>${row.Player}</td>
                    <td>${row.Team}</td>
                    <td>${agents}</td>
                    <td>${row.ACS}</td>
                    <td>${row.K}</td>
                    <td>${row.D}</td>
                    <td>${row.A}</td>
                    <td class="${kdClass}">${kdText}</td>
                `;
                tableBody.appendChild(tr);
            });
        }
    });
}

function loadEconomyData() {
    Papa.parse('data/economy.csv', {
        download: true,
        header: false, // Economy CSV has a weird header, let's parse manually-ish or skip
        complete: function (results) {
            // The economy file format is complex: "(BANK) Team1 Team2 (BANK)", "R1 1.0k 2.0k", ...
            // We need to parse this specifically based on the VLR format we saw

            // Sample row 0: (BANK) NRG FNC (BANK),1 0.3k 0.4k,2 8.5k $$ 6.1k,...
            const rows = results.data;
            if (rows.length < 2) return;

            const rounds = [];
            const team1Bank = [];
            const team2Bank = [];

            // Iterate through the columns of the first row (skipping first cell)
            const headerRow = rows[0];

            // VLR Economy CSV is one row per team?? No, checking view_file output:
            // Row 1: (BANK) NRG FNC (BANK), 1 0.3k 0.4k, ...
            // It seems all data for a map is in one row? Or maybe multiple rows for halves?

            // Let's try to parse Row 0 (First half?)
            // Cell format: "1 0.3k 0.4k" -> Round Num, Team1 Bank, Team2 Bank

            headerRow.slice(1).forEach(cell => {
                if (!cell) return;
                const parts = cell.trim().split(' ');
                if (parts.length >= 3) {
                    rounds.push(parts[0]);
                    team1Bank.push(parseBank(parts[1]));
                    team2Bank.push(parseBank(parts.slice(2).join(' '))); // Handle weird spacing if any
                }
            });

            renderChart(rounds, team1Bank, team2Bank);
        }
    });
}

function parseBank(val) {
    // Value could be "0.3k", "$$", "$", or "8.5k"
    // $ = loss bonus? VLR uses $ symbols to denote loss bonus sometimes?
    // Wait, typical VLR economy: "3.5k", or "$$$"
    if (!val) return 0;
    if (val.includes('k')) {
        return parseFloat(val.replace('k', '')) * 1000;
    }
    // Fallback for non-numeric representation if needed, or just 0
    return 0;
}

function renderChart(labels, team1Data, team2Data) {
    const ctx = document.getElementById('economyChart').getContext('2d');

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'NRG Bank',
                    data: team1Data,
                    borderColor: '#ff4655', // Valorant Red
                    tension: 0.1
                },
                {
                    label: 'FNC Bank',
                    data: team2Data,
                    borderColor: '#ffce56', // Yellow/Gold
                    tension: 0.1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Credits'
                    }
                }
            }
        }
    });
}
