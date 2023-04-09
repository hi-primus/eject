import { DuckDBClient, getType } from "./duckDBClass";

const c = new DuckDBClient();
const tableName = "meteors";

function lineSpace(startValue: number, stopValue: number, cardinality: number) {
  let arr = [];
  const step = (stopValue - startValue) / (cardinality - 1);
  for (let i = 0; i < cardinality; i++) {
    arr.push(startValue + step * i);
  }
  return arr;
}

const names = async () => {
  const columns = (await execute(`DESCRIBE ${tableName}`)).toArray();
  return columns.map(({ column_name, column_type }) => {
    return {
      name: column_name,
      type: getType(column_type),
      databaseType: column_type,
    };
  });
};

/**
 * Returns an array of column names based on the input criteria.
 *
 * @param {string[]|number[]|string|number|RegExp} columns - The columns to select.
 * It can be an array of strings (column names), an array of numbers (column indices), a string (column name),
 * a number (column index), or a regular expression (pattern to match against column names).
 * @param {boolean} [exclude=false] - If true, excludes the selected columns from the output array.
 * Default is false.
 * @param {boolean} [isRegex=false] - If true, treats the `columns` parameter as a regular expression. Default is false.
 * @param {string|string[]|null} [filterByDataTypes=null] - Filters the selected columns by data type.
 * It can be a string or an array of strings representing the data types to include.
 * The data types must be one of the following: 'number', 'string', 'boolean', or 'date'.
 * If null, no data type filtering is applied. Default is null.
 * @returns {Promise<string[]>} An array of strings representing the selected columns.
 */
async function prepareColumns(
  columns: (string | number | RegExp)[] | string | number | RegExp,
  exclude: boolean = false,
  isRegex: boolean = false,
  filterByDataTypes: string | string[] | null = null
): Promise<string[]> {
  let result: string[];

  // Get the column names using the names() function
  const columnNames = await names();

  // If the columns parameter is '*', use the names() function to get all column names
  if (columns === "*") {
    result = columnNames;
  }
  // If the columns parameter is a number, return the name of the column at that position
  else if (typeof columns === "number") {
    if (columns < 0 || columns >= columnNames.length) {
      throw new Error("Invalid column index");
    }
    result = [columnNames[columns]];
  }
  // If the columns parameter is an array of numbers, return the column names according to their position
  else if (
    Array.isArray(columns) &&
    columns.every((col) => typeof col === "number")
  ) {
    const maxIndex = columnNames.length - 1;
    if (columns.some((col) => col < 0 || col > maxIndex)) {
      throw new Error("Invalid column index");
    }
    result = columns.map((col) => columnNames[col]);
  }
  // If the columns parameter is a regular expression, return the column names that match the regular expression
  else if (isRegex && columns instanceof RegExp) {
    result = columnNames.filter((col) => columns.test(col));
  }
  // If the columns parameter is an array of strings or a single string, return it as an array
  else if (Array.isArray(columns)) {
    result = columns
      .map((col) => {
        if (typeof col === "number") {
          if (col < 0 || col >= columnNames.length) {
            throw new Error("Invalid column index");
          }
          return columnNames[col];
        } else if (col instanceof RegExp) {
          return columnNames.filter((name) => col.test(name));
        } else if (typeof col === "string") {
          return col;
        } else {
          throw new Error("Invalid input type");
        }
      })
      .flat();
  } else if (typeof columns === "string") {
    result = [columns];
  }
  // If the columns parameter is not a valid input type, throw an error
  else {
    throw new Error("Invalid input type");
  }

  // If filterByDataTypes is set, filter the result to include only columns with matching data types
  if (filterByDataTypes) {
    const dataTypesDict = datatypes();
    const filterDataTypes = Array.isArray(filterByDataTypes)
      ? filterByDataTypes
      : [filterByDataTypes];
    result = result.filter((name) =>
      filterDataTypes.includes(dataTypesDict[name])
    );
  }

  // If invert is true, return all columns that are not included in the input columns parameter
  if (exclude) {
    result = columnNames.filter((name) => !result.includes(name));
  }

  return result;
}

const load = async (file) => {
  return await c.insertCSVURL(tableName, file);
};

const execute = async (
  query: string,
  params: (string | number)[] | null | undefined = null
) => {
  return await c.query(query, params);
};
const select = async (limit: number = 10) => {
  return await execute(`SELECT * FROM ${tableName} LIMIT $1`, [limit]);
};

// Concatenate multiple columns
const nest = (cols: string[]) => {
  return execute(`SELECT concat(${cols.join(",")})`);
};

// Uppercase multiple columns
// input upper case first name and last name
// result = await c.query(`SELECT UPPER(firstName), UPPER(lastName) FROM meteors`);
const upper = async (cols: string[]) => {
  const colsOperation = cols.map((column) => `UPPER(${column})`).join(", ");
  const query = `SELECT ${colsOperation} FROM ${tableName}`;
  console.log("00000000000000", query);
  let r = await c.query(query);
  console.table(r.toArray());
  // return await execute(query);
};

// Lowercase multiple columns
const lower = (cols: string[]) => {
  return execute(`SELECT LOWER(${cols.join(",")}}) FROM ${tableName}`);
};

const sample = (limit: number) => {
  return execute(`SELECT * FROM ${tableName} USING SAMPLE 0.01% (BERNOULLI)`);
};

const print = (data) => {
  const json = JSON.stringify(
    data.toArray(),
    (key, value) => (typeof value === "bigint" ? value.toString() : value) // return everything else unchanged
  );

  console.table(JSON.parse(json));
  // console.log(JSON.parse(JSON.stringify(data.toArray(), (key, value) =>
  //     typeof value === 'bigint'
  //         ? value.toString()
  //         : value // return everything else unchanged
  // )))
  // console.table(data.toArray())
};
const tables = async () => {
  let r = await execute(`DESCRIBE`);
  console.table(r.toArray());
};

const histogram = (cols: string) => {
  const ln = lineSpace(0, 1000, 11);
  let result = [];
  let r;
  ln.map((value, index, element) => {
    if (index < ln.length - 1) {
      result.push([element[index], element[index + 1]]);
    }
  });

  let sqlBins = []; // console.log(result);
  result.map((value, index, element) => {
    sqlBins.push(`(${index},${value[0]}, ${value[1]})`);
  });
  const tableName = "meteors";
  const columnName = c.escape("mass (g)");

  console.log(sqlBins.join(","));

  const minmax = `
       SELECT min(${columnName}) AS min, max(${columnName}) AS max FROM ${tableName}
      `;
  r = execute(minmax);
  console.log("-----", r.toArray()[0]);

  const hist = `
    -- some random data

      -- create 10 equally spaced bins for data based on min/max
      CREATE TABLE dummy_bins (bin_id INTEGER, bin_min INTEGER, bin_max INTEGER);
      INSERT INTO dummy_bins VALUES ${sqlBins};
      `;
  // Histogram floor implementation
  // https://www.wagonhq.com/sql-tutorial/creating-a-histogram-sql/
  //   const hist = `
  //   CREATE TEMPORARY TABLE dummy_bins AS
  // SELECT range bin_id,
  // 	minv + (maxv - minv)*(range/n::double) as bin_min,
  // 	minv + (maxv - minv)*((range+1)/n::double) bin_max
  // FROM (
  // 	SELECT *, max(range) over() + 1 n
  // 	FROM range(0, 9),
  // 		(SELECT min(${columnName}) minv, max(${columnName}) maxv FROM ${tableName}));
  //   `;
  // Histogram
  // console.log("----",tableName)
  execute(hist);
  r = execute(
    `SELECT bin_id, bin_min, bin_max, COUNT(*) AS count FROM ${tableName} JOIN dummy_bins ON (${columnName} >= bin_min AND ${columnName} < bin_max) GROUP BY bin_id, bin_min,bin_max;`
  );
  console.table(r.toArray());

  const ls = `SELECT bin_min, bin_max FROM dummy_bins`;
  r = execute(ls);
  console.log(JSON.parse(JSON.stringify(r.toArray())));
};
const frequency = (cols: string) => {
  return execute(
    `SELECT count(cols) AS count_reclass FROM  ${tableName} GROUP BY (cols) ORDER BY count DESC LIMIT 10`
  );
};

const sum = (cols: string) => {
  return execute(`SELECT sum(cols) FROM ${tableName}`);
};
const min = (cols: string) => {
  return execute(`SELECT min(cols) FROM ${tableName}`);
};

const unnest = (cols: string) => {
  //  r = await c.query(`SELECT str_split(CAST(reclat AS VARCHAR),' ') FROM meteors LIMIT 10`);
  return execute(
    `SELECT str_split(CAST(${tableName} AS VARCHAR),' ') FROM meteors LIMIT 10`
  );
};

(async () => {
  try {
    // await c.insertCSVURL(
    //     'meteors',
    //     'https://raw.githubusercontent.com/hi-primus/optimus/develop/examples/data/Meteorite_Landings.csv',
    //     { delimiter: ',', header: true }
    // );
    // // Limit
    // let r = await c.query(`SELECT * FROM meteors LIMIT $1`,[5]);
    // print(r)

    await load(
      "https://raw.githubusercontent.com/hi-primus/optimus/develop/examples/data/foo.csv"
    );
    let r = await select(10);

    print(r);
    console.table(await names());

    // await tables()
    // let r
    // r = await upper(['firstName', 'lastName'])
    // // console.log("r",r)
    // print(r)
    // const arrow = await import(
    //   'https://cdn.jsdelivr.net/npm/apache-arrow@10.0.0/+esm'
    // );

    // console.log(c)
    // await c.query(`CREATE TABLE dt(u STRING, x INTEGER, y FLOAT)`);

    // await c.query(
    //   `INSERT INTO dt VALUES ('a', 1, 5), ('b', 2, 6), ('c', 3, 7), ('d', 4, 8);`
    // );
    // console.log(await c.describe())
    // console.log(await c.describeTables())
    // console.log(await c.describeColumns({table:'dt'}))

    // let r = await c.query<{ i: arrow.Map_<arrow.Int32, arrow.Uint64> }>(
    //   `SELECT histogram("mass (g)") AS count FROM  meteors LIMIT 10`
    // );
    // r.toArray().map(i => {
    //   console.dir(arrow.Uint64);
    // });

    // r = await c.query(`SELECT regexp_matches(meteors.name, "a") FROM meteors LIMIT 10`)
    // console.table(r.toArray())

    // r = await c.query(`SELECT id like '1' FROM meteors LIMIT 10`)
    // console.table(r.toArray())

    // r = await c.query(`SELECT regexp_matches(name,'Aachen') FROM meteors LIMIT 10`);
    // console.table(r.toArray())

    // Update
    // r = await c.query(`UPDATE meteors SET name = lower(name)`)

    // r = await c.query(`ALTER TABLE meteors ADD COLUMN newName VARCHAR`)
    // r = await c.query(`ALTER TABLE meteors ALTER newName SET DATA TYPE VARCHAR USING upper(name)`)

    // Rename columns
    // ALTER TABLE integers RENAME i TO j;

    // Drop column
    // ALTER TABLE integers DROP k;
  } catch (e) {
    console.log(e);
  }
})();
