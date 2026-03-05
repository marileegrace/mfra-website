export default async (request) => {
  const url = new URL(request.url);
  const path = url.pathname;

  let yahooUrl;
  if (path.startsWith('/api/chart/')) {
    const ticker = path.replace('/api/chart/', '');
    yahooUrl = `https://query2.finance.yahoo.com/v8/finance/chart/${ticker}${url.search}`;
  } else if (path.startsWith('/api/quote/')) {
    const ticker = path.replace('/api/quote/', '');
    yahooUrl = `https://query2.finance.yahoo.com/v10/finance/quoteSummary/${ticker}${url.search}`;
  } else {
    return;
  }

  try {
    const resp = await fetch(yahooUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com/',
        'Origin': 'https://finance.yahoo.com',
      },
    });

    const body = await resp.text();
    return new Response(body, {
      status: resp.status,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};

export const config = {
  path: ['/api/chart/*', '/api/quote/*'],
};
