const esbuild = require('esbuild');
const glob = require('tiny-glob');

const commonConfig = {
    bundle: true,
    platform: 'node',
    target: 'es2020',
    sourcemap: false,
    // minify: true, makes messy debug/error output
    treeShaking: true,
    legalComments: 'none',
    metafile: true,
    format: 'cjs',
    external: [
        'bigint-buffer',
        '@triton-one/yellowstone-grpc',
        'helius-laserstream',
    ]
};

(async () => {
    let entryPoints = await glob("./src/**/*.ts");
    await esbuild.build({
        ...commonConfig,
        entryPoints,
        outdir: 'lib',
    });
})().catch(() => process.exit(1));