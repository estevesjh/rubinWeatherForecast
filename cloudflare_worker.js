export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Handle CSV upload (store in KV)
    if (request.method === 'POST' && url.pathname === '/api/update') {
      const contentType = request.headers.get('Content-Type');
      if (!contentType || !contentType.includes('text/csv')) {
        return new Response('Invalid content type. Expected text/csv.', { status: 400 });
      }
      const csvData = await request.text();
      await env.TEMP_KV.put('latest_csv', csvData);
      const uploadTime =
        request.headers.get('X-Upload-Timestamp') ||
        new Date().toISOString();
      await env.TEMP_KV.put('latest_csv_time', uploadTime);
      return new Response('CSV uploaded successfully with timestamp stored.', { status: 200 });
    }

    // Serve index.html
    if (url.pathname === '/' || url.pathname === '/index.html') {
      return new Response(
        `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta http-equiv="Content-Security-Policy" content="default-src 'self'; script-src 'self' https://code.highcharts.com 'unsafe-eval' 'unsafe-inline'; style-src 'self' 'unsafe-inline';">
  <title>Rubin Summit Temperature Forecast</title>
  <script src="https://code.highcharts.com/highcharts.js"></script>
  <script src="https://code.highcharts.com/highcharts-more.js"></script>
  <script src="https://code.highcharts.com/modules/exporting.js"></script>
  <style>
.twilight-flex {
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: flex-start;
  gap: 2.4rem;
  margin: 2em auto 0 auto;
  width: 94vw;
  max-width: 1250px;
}
.twilight-box {
  flex: 1 1 0;
  max-width: 420px;
  min-width: 190px;
  background: #b22222;      /* firebrick */
  border-radius: 18px;
  box-shadow: 0 6px 32px 0 #0002;
  padding: 1.25em 0.5em 1.1em 0.5em;
  text-align: center;
  color: #fff;
  margin-top: 0.5em;
  margin-bottom: 0.5em;
}
.twilight-timebox {
  max-width: 420px;
  min-width: 190px;
  background: #232b36cc;
  border-radius: 18px;
  box-shadow: 0 2px 16px 0 #0002;
  padding: 1.05em 0.5em 1.0em 0.5em;
  text-align: center;
  color: #fff;
  margin-top: 1.1em;
  margin-bottom: 0.5em;
}
.twilight-label {
  color: #ccc;
  font-size: 1.19rem;
  margin-bottom: 0.19em;
  letter-spacing: 0.01em;
}
.twilight-temp {
  font-size: 3.3rem;
  font-family: 'SF Mono', 'Menlo', 'Consolas', 'monospace';
  font-weight: 600;
  color: #fff;
  line-height: 1.08;
  margin-bottom: 0.18em;
}
.twilight-time {
  font-size: 2.2rem;
  font-family: 'SF Mono', 'Menlo', 'Consolas', 'monospace';
  font-weight: 600;
  color: #b9eaff;
  line-height: 1.18;
  margin-bottom: 0.12em;
}
.twilight-remaining {
  font-size: 1.31rem;
  color: #f0f0f0;
  margin-top: 0.22em;
  opacity: 0.95;
  letter-spacing: 0.02em;
}
.deg {
  font-size: 1.2rem;
  vertical-align: super;
  margin-left: 2px;
  color: #ccc;
}
.twilight-meta {
  font-size: 1.07rem;
  color: #f0f0f0;
  margin-top: 0.22em;
  opacity: 0.95;
}
#container {
  flex: 2 1 0;
  min-width: 350px;
  min-height: 470px;
  height: 510px;
  margin: 0;
  background: #18181a;
  border-radius: 16px;
  box-shadow: 0 3px 18px 0 #0002;
}
@media (max-width: 1000px) {
  .twilight-flex { flex-direction: column; gap: 1.3rem; width: 99vw;}
  #container { width: 99vw; min-width: 200px; height: 410px;}
}
  </style>
</head>
<body>
  <h1 style="text-align:center;">Rubin Summit Temperature Forecast</h1>
  <h2 style="text-align:center;">Forecast of the Day</h2>
  <div id="lastUpdate" style="text-align:center;font-size:0.9rem;color:#555;margin-top:0.25rem;"></div>

  <div class="twilight-flex">
    <div>
      <div class="twilight-box">
        <div class="twilight-label">Forecast at Twilight</div>
        <div class="twilight-temp" id="twilight-forecast">--<span class="deg">°C</span></div>
        <div class="twilight-meta" id="twilight-actual">Actual: -- °C (Weather Tower)</div>
      </div>
      <div class="twilight-timebox">
        <div class="twilight-label">Twilight Time</div>
        <div class="twilight-time" id="twilight-time">--:-- CLT</div>
        <div class="twilight-remaining" id="twilight-remaining">--h --min</div>
      </div>
    </div>
    <div id="container"></div>
  </div>

  <script>
      const statusDiv = document.createElement('div');   // spot for user messages
      statusDiv.style.cssText = 'text-align:center;color:red;margin-top:1rem;';
      document.body.prepend(statusDiv);
      const lastUpdateDiv = document.getElementById('lastUpdate');

      // ---------- live‑update helpers ----------
      function updateBadge(latestTs) {
        function render() {
          // Show delay in whole minutes, always non‑negative
          const mins = Math.floor(Math.abs(Date.now() - latestTs) / 60000);
          lastUpdateDiv.textContent = 'Last update: ' + mins + ' min ago';
          lastUpdateDiv.style.color = mins > 30 ? 'red' : '#555';
        }
        render();
        if (updateBadge.timer) clearInterval(updateBadge.timer);
        updateBadge.timer = setInterval(render, 60 * 1000);
      }

      function fetchAndRedraw() {
        fetch('https://rubin-weather-forecast.jesteves.workers.dev/api/forecast')
          .then(r => {
            if (!r.ok) throw new Error('Server replied ' + r.status + ' ' + r.statusText);
            return r.text();
          })
          .then(csv => {
            if (!csv.trim()) throw new Error('Empty CSV returned');

            // ---- parse CSV (original logic retained) ----
            const lines  = csv.trim().split('\\n');
            const header = lines.shift().split(',');
            const idx = n => header.indexOf(n);
            const safe = v => { const n = parseFloat(v); return isNaN(n) ? null : +n.toFixed(1); };

            let latestPastTs = null;

            const tmin = [], tmean = [], tmax = [];
            const tpmin = [], tprophet = [], tpmax = [];
            const trend = [];
            const sunset = [];

            lines.forEach(l => {
              if (!l.trim()) return;
              const c = l.split(',');
              const ts = new Date(c[idx('timestamp')]).getTime();
              if (ts <= Date.now()) latestPastTs = ts;
              if (c[idx('sunset')].toLowerCase() === 'true') sunset.push(ts);

              tmean.push([ts, safe(c[idx('tmean')])]);
              tprophet.push([ts, safe(c[idx('tprophet')])]);
              tmin.push([ts, safe(c[idx('tmin')])]);
              tmax.push([ts, safe(c[idx('tmax')])]);
              tpmin.push([ts, safe(c[idx('tpmin')])]);
              tpmax.push([ts, safe(c[idx('tpmax')])]);
              trend.push([ts, safe(c[idx('trend-weekly')])]);
            });

            // badge
            const badgeTs = latestPastTs ?? tprophet.at(0)[0];
            updateBadge(badgeTs);

            const obsBand  = tmin.map((d, i) => [d[0], d[1], tmax[i][1]]);
            const predBand = tpmin.map((d, i) => [d[0], d[1], tpmax[i][1]]);

            Highcharts.setOptions({ time: { timezone: 'America/Santiago' } });

            if (window.chart) window.chart.destroy();

            window.chart = Highcharts.chart('container', {
              chart: { type: 'spline', zoomType: 'x',
                       resetZoomButton: { position: { align: 'right', verticalAlign: 'top', x: 0, y: 0 } } },
              title: { text: null },
              xAxis: {
                type: 'datetime',
                title: { text: 'Time (CLT)' },
                plotLines: sunset.map(ts => ({
                  value: ts, color: 'gray', width: 2, dashStyle: 'Dash',
                  label: { text: 'Sunset', rotation: 90, textAlign: 'left', style: { color: 'gray' } },
                  zIndex: 5
                }))
              },
              yAxis: { title: { text: 'Temperature (°C)' } },
              tooltip: {
                shared: true,
                xDateFormat: '%Y-%m-%d %H:%M',
                formatter() {
                  let s = '<b>' + Highcharts.dateFormat('%Y-%m-%d %H:%M', this.x) + '</b><br/>';
                  this.points.forEach(p => {
                    const n = p.series.name, col = p.color;
                    if (n === 'Observed Range' || n === 'Forecast Range') {
                      s += '<span style="color:' + col +
                           '">●</span> (max‑min): <b>' + (p.point.high - p.point.low).toFixed(2) + '°C</b><br/>';
                    } else {
                      s += '<span style="color:' + col + '">●</span> ' + n +
                           ': <b>' + p.y.toFixed(2) + '°C</b><br/>';
                    }
                  });
                  return s;
                }
              },
              series: [
                { name: 'Observed Range', type: 'arearange', data: obsBand,
                  color: 'rgba(128,128,128,0.3)', lineWidth: 0, marker: { enabled: false }, zIndex: 0 },
                { name: 'Weather Tower', data: tmean, color: 'black', zIndex: 1, connectNulls: false },
                { name: 'Forecast Range', type: 'arearange', data: predBand,
                  color: 'rgba(178,34,34,0.25)', lineWidth: 0, marker: { enabled: false }, zIndex: 0 },
                { name: 'Prophet Forecast', data: tprophet, color: 'firebrick', zIndex: 1, connectNulls: false },
                { name: 'Trend + Weekly', data: trend, color: 'gray', zIndex: 1,
                  connectNulls: false, marker: { enabled: false } }
              ],
              credits: { enabled: false }
            });

            // ----- Update Twilight Box (forecast and actual at 18:00 CLT) -----
            try {
              // Helper: get UTC timestamp for a given Chile (America/Santiago) local wall time
              function santiagoLocalTimeToUTC(year, month, day, hour, minute = 0) {
                // month: 0-11
                // Returns UTC timestamp (ms since epoch) for this wall time
                const dtf = new Intl.DateTimeFormat('en-US', {
                  timeZone: 'America/Santiago',
                  year: 'numeric', month: '2-digit', day: '2-digit',
                  hour: '2-digit', minute: '2-digit', second: '2-digit',
                  hour12: false
                });
                // Brute force search within +/-16h for a matching local hour/day
                for (let guess = -16; guess <= 16; guess++) {
                  const testUTC = Date.UTC(year, month, day, hour - guess, minute);
                  const parts = dtf.formatToParts(new Date(testUTC));
                  const lh = +parts.find(x => x.type === 'hour').value;
                  const lm = +parts.find(x => x.type === 'minute').value;
                  const ld = +parts.find(x => x.type === 'day').value;
                  if (lh === hour && lm === minute && ld === day) return testUTC;
                }
                // fallback: now
                return Date.now();
              }

              // Get today's date in Chile local time
              const now = new Date();
              const dtfCL = new Intl.DateTimeFormat('en-US', { timeZone: 'America/Santiago' });
              const parts = dtfCL.formatToParts(now);
              const todayY = +parts.find(x => x.type === 'year').value;
              const todayM = +parts.find(x => x.type === 'month').value - 1; // JS months
              const todayD = +parts.find(x => x.type === 'day').value;

              // Get UTC timestamp for today at 18:00 CLT
              const twilightUTC = santiagoLocalTimeToUTC(todayY, todayM, todayD, 18, 0);

              // Find closest index in tprophet to twilightUTC
              function closestIdx(arr, target) {
                return arr.reduce((bestIdx, [ts], i, a) =>
                  Math.abs(ts - target) < Math.abs(a[bestIdx][0] - target) ? i : bestIdx, 0);
              }
              const idxTwilight = closestIdx(tprophet, twilightUTC);

              // Get forecast and actual values
              const forecastTwilight = tprophet[idxTwilight][1];
              const actualTwilight = tmean[idxTwilight][1];

              document.getElementById('twilight-forecast').innerHTML =
                `${forecastTwilight !== null ? forecastTwilight.toFixed(1) : '--'}<span class="deg">°C</span>`;
              document.getElementById('twilight-actual').textContent =
                `Actual: ${actualTwilight !== null ? actualTwilight.toFixed(1) : '--'} °C (Weather Tower)`;

              // --- Update Twilight Time and Time to Twilight Box ---
              try {
                // Format twilight time in Chile local time
                const dtTwilight = new Date(twilightUTC);
                const twilightCLTime = dtTwilight.toLocaleTimeString('en-US', {
                  timeZone: 'America/Santiago',
                  hour: '2-digit',
                  minute: '2-digit',
                  hour12: false
                });
                document.getElementById('twilight-time').textContent = `${twilightCLTime} CLT`;

                // Compute hours/minutes to twilight
                const nowUTC = Date.now();
                let diffMs = twilightUTC - nowUTC;
                let sign = '';
                if (diffMs < 0) { diffMs = -diffMs; sign = '-'; }
                const diffTotalMin = Math.floor(diffMs / 60000);
                const hours = Math.floor(diffTotalMin / 60);
                const mins = diffTotalMin % 60;
                let remainingText = '';
                if (sign === '-') {
                  remainingText = 'Passed';
                } else if (hours > 0) {
                  remainingText = `${hours}h ${mins}min`;
                } else {
                  remainingText = `${mins}min`;
                }
                document.getElementById('twilight-remaining').textContent = `In ${remainingText}`;
              } catch (err) {
                document.getElementById('twilight-time').textContent = '--:-- CLT';
                document.getElementById('twilight-remaining').textContent = '--h --min';
              }
            } catch (err) {
              // fallback, leave '--' if not available
              document.getElementById('twilight-forecast').innerHTML = '--<span class="deg">°C</span>';
              document.getElementById('twilight-actual').textContent = 'Actual: -- °C (Weather Tower)';
            }
          })
          .catch(err => {
            console.error(err);
            statusDiv.textContent = '⚠️ Forecast file not found or could not be loaded.';
          });
      }

      // initial draw + auto‑reload every 5 minutes
      fetchAndRedraw();
      setInterval(fetchAndRedraw, 5 * 60 * 1000);
  </script>
</body>
</html>
`,
        { headers: { 'Content-Type': 'text/html' } }
      );
    }

    // Serve latest CSV from KV at /api/forecast
    if (url.pathname === '/api/forecast') {
      const latestCSV = await env.TEMP_KV.get('latest_csv');
      if (latestCSV) {
        return new Response(latestCSV, {
          headers: { 'Content-Type': 'text/csv' }
        });
      } else {
        return new Response('No forecast uploaded yet.', { status: 404 });
      }
    }

    if (url.pathname === '/api/forecast_time') {
      const ts = await env.TEMP_KV.get('latest_csv_time');
      return new Response(JSON.stringify({ timestamp: ts }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response('Not found', { status: 404 });
  }
};
