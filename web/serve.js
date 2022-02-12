require("esbuild").serve(
  {
    servedir: "public",
  },
  {
    entryPoints: ["src/index.tsx", "src/sw.ts"],
    outdir: "public",
    bundle: true,
  }
);
