export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === 'POST' && url.pathname === '/upload') {
      // Handle CSV upload
      const contentType = request.headers.get('Content-Type');
      if (!contentType || !contentType.includes('text/csv')) {
        return new Response('Invalid content type. Expected text/csv.', { status: 400 });
      }

      const csvData = await request.text();
      const timestamp = new Date().toISOString();

      try {
        // Store CSV data in KV with a timestamp key
        await env.TEMP_KV.put(`forecast_${timestamp}`, csvData);
        return new Response('CSV data uploaded successfully.', { status: 200 });
      } catch (error) {
        return new Response(`Error storing data: ${error.message}`, { status: 500 });
      }
    } else if (request.method === 'GET' && url.pathname === '/data') {
      // Retrieve all keys and their data from KV
      try {
        const keys = await env.TEMP_KV.list();
        const data = await Promise.all(
          keys.keys.map(async (key) => {
            const value = await env.TEMP_KV.get(key.name);
            return { key: key.name, value };
          })
        );
        return new Response(JSON.stringify(data), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (error) {
        return new Response(`Error retrieving data: ${error.message}`, { status: 500 });
      }
    } else {
      return new Response('Not Found', { status: 404 });
    }
  },
};
