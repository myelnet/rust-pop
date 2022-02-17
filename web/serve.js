const http = require("http");

require("esbuild")
  .serve(
    {
      servedir: "public",
    },
    {
      entryPoints: ["src/index.tsx", "src/sw.ts"],
      outdir: "public",
      bundle: true,
    }
  )
  .then((result) => {
    const { host, port } = result;

    // Then start a proxy server on port 8008
    http
      .createServer((req, res) => {
        const options = {
          hostname: host,
          port: port,
          path: req.url,
          method: req.method,
          headers: req.headers,
        };

        // Forward each incoming request to esbuild
        const proxyReq = http.request(options, (proxyRes) => {
          // Otherwise, forward the response from esbuild to the client
          res.writeHead(proxyRes.statusCode, {
            ...proxyRes.headers,
            // add cross origin isolation headers
            "Cross-Origin-Opener-Policy": "same-origin",
            "Cross-Origin-Embedder-Policy": "require-corp",
          });
          proxyRes.pipe(res, { end: true });
        });

        // Forward the body of the request to esbuild
        req.pipe(proxyReq, { end: true });
      })
      .listen(8008);
  });
