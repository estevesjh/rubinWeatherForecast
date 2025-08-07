export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // In-memory storage for latest CSV (demo only, not persistent)
    if (!globalThis.latestCSV) {
      globalThis.latestCSV = null;
    }

    // Handle CSV upload
    if (request.method === 'POST' && url.pathname === '/api/update') {
      const contentType = request.headers.get('Content-Type');
      if (!contentType || !contentType.includes('text/csv')) {
        return new Response('Invalid content type. Expected text/csv.', { status: 400 });
      }
      const csvData = await request.text();
      globalThis.latestCSV = csvData;
      return new Response('CSV uploaded successfully.', { status: 200 });
    }

    // Serve index.html
    if (url.pathname === '/' || url.pathname === '/index.html') {
      return new Response(
        `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
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
    fetch('temp_forecast_20250805.csv')
      .then(response => response.text())
      .then(csv => {
        const lines = csv.trim().split('\n');
        const header = lines.shift().split(',');

        const idx = name => header.indexOf(name);
        const tminData = [], tmeanData = [], tmaxData = [];
        const tpminData = [], tprophetData = [], tpmaxData = [];
        const sunsetLines = [];

        lines.forEach(line => {
          const cols = line.split(',');
          const ts = new Date(cols[idx('timestamp')]).getTime();

          if (cols[idx('sunset')].toLowerCase() === 'true') {
            sunsetLines.push(ts);
          }

          const tmin = parseFloat(parseFloat(cols[idx('tmin')]).toFixed(1));
          const tmean = parseFloat(parseFloat(cols[idx('tmean')]).toFixed(1));
          const tmax = parseFloat(parseFloat(cols[idx('tmax')]).toFixed(1));

          const tpminVal = parseFloat(parseFloat(cols[idx('tpmin')]).toFixed(1));
          const tprophet = parseFloat(parseFloat(cols[idx('tprophet')]).toFixed(1));
          const tpmaxVal = parseFloat(parseFloat(cols[idx('tpmax')]).toFixed(1));

          tmeanData.push([ts, tmean]);
          tprophetData.push([ts, tprophet]);

          tminData.push([ts, tmin]);
          tmaxData.push([ts, tmax]);
          tpminData.push([ts, tpminVal]);
          tpmaxData.push([ts, tpmaxVal]);
        });

        const obsBand = tminData.map((d, i) => [d[0], d[1], tmaxData[i][1]]);
        const predBand = tpminData.map((d, i) => [d[0], d[1], tpmaxData[i][1]]);

        Highcharts.chart('container', {
          chart: {
            type: 'spline',
            zoomType: 'x',
            resetZoomButton: {
              position: {
                align: 'right',
                verticalAlign: 'top',
                x: 0,
                y: 0
              },
              theme: {
                fill: '#f7f7f7',
                stroke: '#ccc',
                r: 2,
                style: {
                  color: '#333'
                }
              }
            }
          },
          title: { text: 'Temperature Forecast' },
          xAxis: {
            type: 'datetime',
            title: { text: 'Time (local)' },
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
              this.points.forEach(point => {
                const name = point.series.name;
                const color = point.color;
                if (name === "Observed Range" || name === "Forecast Range") {
                  const range = (point.point.high - point.point.low).toFixed(2);
                  s += '<span style="color:' + color + '">\u25CF</span> (max - min): <b>' + range + '&#176;C</b><br/>';
                } else {
                  s += '<span style="color:' + color + '">\u25CF</span> ' + name + ': <b>' + point.y.toFixed(2) + '&#176;C</b><br/>';
                }
              });
              return s;
            }
          },
          legend: { enabled: true },
          series: [
            {
              name: 'Observed Range',
              type: 'arearange',
              data: obsBand,
              lineWidth: 0,
              color: 'rgba(128,128,128,0.3)',
              fillOpacity: 0.3,
              zIndex: 0,
              marker: { enabled: false }
            },
            {
              name: 'Weather Tower',
              data: tmeanData,
              color: 'black',
              zIndex: 1
            },
            {
              name: 'Forecast Range',
              type: 'arearange',
              data: predBand,
              lineWidth: 0,
              color: 'rgba(255,64,78,0.3)',
              fillOpacity: 0.3,
              zIndex: 0,
              marker: { enabled: false }
            },
            {
              name: 'Prophet Forecast',
              data: tprophetData,
              color: 'firebrick',
              zIndex: 1
            }
          ]
        });
      });
  </script>
</body>
</html>
`,
        { headers: { 'Content-Type': 'text/html' } }
      );
    }

    // Serve latest CSV at /api/forecast
    if (url.pathname === '/api/forecast') {
      if (globalThis.latestCSV) {
        return new Response(globalThis.latestCSV, {
          headers: { 'Content-Type': 'text/csv' }
        });
      } else {
        return new Response('No forecast uploaded yet.', { status: 404 });
      }
    }

    return new Response('Not found', { status: 404 });
  }
};
