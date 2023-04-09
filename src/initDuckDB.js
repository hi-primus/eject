import * as duckdb from "@duckdb/duckdb-wasm";

async function makeDB() {
    // const duckdb = await import(
    //     'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.17.0/+esm'
    //     );
    // const arrow = await import(
    //     'https://cdn.jsdelivr.net/npm/apache-arrow@10.0.1/+esm'
    //     );
    // const knex = await import(
    //   'https://cdn.jsdelivr.net/npm/knex@2.4.2/knex.min.js'
    // );
    // const reflect_metadata = await import(
    //   'https://cdn.jsdelivr.net/npm/reflect-metadata@0.1.13/Reflect.min.js'
    // );

    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);
    const logger = new duckdb.ConsoleLogger();
    const worker = await duckdb.createWorker(bundle.mainWorker);
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule);
    return db;
}

export { makeDB };
