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
      return new Response('CSV uploaded successfully.', { status: 200 });
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
</head>
<body>
  <h1 style="text-align:center;">Rubin Summit Temperature Forecast</h1>
  <h2 style="text-align:center;">Forecast of the Day</h2>
  <div id="lastUpdate" style="text-align:center;font-size:0.9rem;color:#555;margin-top:0.25rem;"></div>

  <div id="container" style="width: 90%; height: 500px; margin: auto;"></div>

  <script>
      const statusDiv = document.createElement('div');   // spot for user messages
      statusDiv.style.cssText = 'text-align:center;color:red;margin-top:1rem;';
      document.body.prepend(statusDiv);
      const lastUpdateDiv = document.getElementById('lastUpdate');

      // ---------- live‑update helpers ----------
      function updateBadge(latestTs) {
        function render() {
          const mins = Math.round((Date.now() - latestTs) / 60000);
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

            const tmin = [], tmean = [], tmax = [];
            const tpmin = [], tprophet = [], tpmax = [];
            const trend = [];
            const sunset = [];

            lines.forEach(l => {
              if (!l.trim()) return;
              const c = l.split(',');
              const ts = new Date(c[idx('timestamp')]).getTime();
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
            updateBadge(tprophet.at(-1)[0]);

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

    return new Response('Not found', { status: 404 });
  }
};
