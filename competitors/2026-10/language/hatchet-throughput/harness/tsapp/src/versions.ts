/** Record the runtime actually in the worker image. */
const pkg = require('/app/node_modules/@hatchet-dev/typescript-sdk/package.json');
process.stdout.write(
  JSON.stringify({
    node: process.version,
    v8: process.versions.v8,
    sdk: pkg.version,
    sdk_name: pkg.name,
  }) + '\n'
);
