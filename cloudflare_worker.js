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

  <div id="container" style="width: 90%; height: 500px; margin: auto;"></div>

  <script>
      const statusDiv = document.createElement('div');   // spot for user messages
      statusDiv.style.cssText = 'text-align:center;color:red;margin-top:1rem;';
      document.body.prepend(statusDiv);
      fetch('https://rubin-weather-forecast.jesteves.workers.dev/api/forecast')
        .then(response => {
          if (!response.ok) {
            throw new Error('Server replied ' + response.status + ' ' + response.statusText);
          }
          return response.text();
        })
        .then(csv => {
          if (!csv.trim()) throw new Error('Empty CSV returned');

          // -------- parse CSV --------
          const lines  = csv.trim().split('\\n');
          console.log('CSV length:', csv.length);
          console.log('First 120 chars:', csv.slice(0, 120));
          const header = lines.shift().split(',');
          console.log('Header:', header);
          console.log('Total lines parsed:', lines.length);
          const idx = name => header.indexOf(name);

          function safeParse(val) {
            const n = parseFloat(val);
            return isNaN(n) ? null : +n.toFixed(1);
          }

          const tminData = [], tmeanData = [], tmaxData = [];
          const tpminData = [], tprophetData = [], tpmaxData = [];
          const ttrendWeeklyData = [];
          const sunsetLines = [];

          lines.forEach((line, i) => {
            if (!line.trim()) return;
            const cols = line.split(',');
            if (cols.length !== header.length) {
              console.warn('Line', i, 'has unexpected column count:', cols);
              return;
            }
            const ts = new Date(cols[idx('timestamp')]).getTime();
            if (cols[idx('sunset')].toLowerCase() === 'true') sunsetLines.push(ts);

            const tmin       = safeParse(cols[idx('tmin')]);
            const tmean      = safeParse(cols[idx('tmean')]);
            const tmax       = safeParse(cols[idx('tmax')]);
            const tpminVal   = safeParse(cols[idx('tpmin')]);
            const tprophet   = safeParse(cols[idx('tprophet')]);
            const tpmaxVal   = safeParse(cols[idx('tpmax')]);
            const ttrendWeekly = safeParse(cols[idx('trend-weekly')]);

            tmeanData.push([ts, tmean]);
            tprophetData.push([ts, tprophet]);
            tminData.push([ts, tmin]);
            tmaxData.push([ts, tmax]);
            tpminData.push([ts, tpminVal]);
            tpmaxData.push([ts, tpmaxVal]);
            ttrendWeeklyData.push([ts, ttrendWeekly]);
          });
          console.log('tmeanData points:', tmeanData.length);
          console.log('tpminData points:', tpminData.length);
          console.log('Sunset markers:', sunsetLines.length);

          Highcharts.setOptions({
            time: {
              timezone: 'America/Santiago'
            }
          });

          const obsBand  = tminData.map((d, i) => [d[0], d[1], tmaxData[i][1]]);
          const predBand = tpminData.map((d, i) => [d[0], d[1], tpmaxData[i][1]]);

          Highcharts.chart('container', {
            chart: {
              type: 'spline',
              zoomType: 'x',
              resetZoomButton: {
                position: { align: 'right', verticalAlign: 'top', x: 0, y: 0 }
              }
            },
            title: { text: null },                       // No extra title
            xAxis: {
              type: 'datetime',
              title: { text: 'Time (CLT)' },
              plotLines: sunsetLines.map(ts => ({
                value: ts,
                color: 'gray',
                width: 2,
                dashStyle: 'Dash',
                label: {
                  text: 'Sunset',
                  rotation: 90,
                  textAlign: 'left',
                  style: { color: 'gray' }
                },
                zIndex: 5
              }))
            },
            yAxis: {
              title: { text: 'Temperature (°C)' }
            },
            tooltip: {
              shared: true,
              xDateFormat: '%Y-%m-%d %H:%M',
              formatter: function () {
                let s = '<b>' + Highcharts.dateFormat('%Y-%m-%d %H:%M', this.x) + '</b><br/>';
                this.points.forEach(pt => {
                  const name  = pt.series.name;
                  const color = pt.color;
                  if (name === 'Observed Range' || name === 'Forecast Range') {
                    const diff = (pt.point.high - pt.point.low).toFixed(2);
                    s += '<span style="color:' + color + '">●</span> (max - min): <b>' + diff + '°C</b><br/>';
                  } else {
                    s += '<span style="color:' + color + '">●</span> ' + name + ': <b>' + pt.y.toFixed(2) + '°C</b><br/>';
                  }
                });
                return s;
              }
            },
            series: [
              {
                name: 'Observed Range',
                type: 'arearange',
                data: obsBand,
                color: 'rgba(128,128,128,0.3)',
                lineWidth: 0,
                marker: { enabled: false },
                zIndex: 0
              },
              {
                name: 'Weather Tower',
                data: tmeanData,
                color: 'black',
                zIndex: 1,
                connectNulls: false
              },
              {
                name: 'Forecast Range',
                type: 'arearange',
                data: predBand,
                color: 'rgba(178,34,34,0.25)',
                lineWidth: 0,
                marker: { enabled: false },
                zIndex: 0
              },
              {
                name: 'Prophet Forecast',
                data: tprophetData,
                color: 'firebrick',
                zIndex: 1,
                connectNulls: false
              },
              {
                name: 'Trend + Weekly',
                data: ttrendWeeklyData,
                color: 'gray',
                zIndex: 1,
                connectNulls: false,
                marker: { enabled: false }
              }
            ],
            credits: { enabled: false }
          });
        })
        .catch(err => {
          console.error(err);
          statusDiv.textContent = '⚠️ Forecast file not found or could not be loaded.';
        });
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
