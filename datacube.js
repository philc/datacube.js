/*
 * DataCube.js is a library that makes it simple and efficient to slice and dice multi-dimensional
 * data sets that fit in RAM. It provides a single data structure, called a DataCube, and many
 * functions to construct and manipulate these cubes.

 * A DataCube, akin to an OLAP data cube, is a collection of cells, each one uniquely identified by
 * set of dimension values and containing the corresponding metrics. For example, a data cube
 * encapsulating a book store's data could have as dimensions [genre, title, author, day] and metrics
 * [sales, revenue, views]. A sample cell in this the data cube would be:
 *     [{ genre: "Fiction", title: "Pride and Prejudice", author: "Jane Austen", day: "2023-01-01" },
 *      { sales: 100, revenue: 2130.05, views: 5000 }]
*/

/*
 * An array-like type that allocates its backing arrays in large pages.
 * This prevents GC as the array grows.
 */
class PagedArray {
  // Constructor can take either a pageSize (in bytes), or an ArrayBuffer.
  // - arrayType: A typed array type, e.g. UInt32Array.
  // - arg: optional; an ArrayBuffer, or a pageSize.
  //   When arg is an ArrayBuffer, pageSize will be equal to its length.
  constructor(arrayType, arg) {
    this.pages = [];
    this.arrayType = arrayType;
    this._length = 0;
    // TODO(philc): Throw an error if `arg` is an unexpected type.
    let pageSize = 100 * 1024;
    if (arg instanceof ArrayBuffer) {
      const bytes = arg.byteLength;
      if (bytes % this.BYTES_PER_ELEMENT != 0) {
        throw `ArrayBuffer's length should be a multiple of ${this.BYTES_PER_ELEMENT}.`;
      }
      pageSize = arg.byteLength;
      this.pages[0] = new this.arrayType(arg);
      this._length = bytes / this.BYTES_PER_ELEMENT;
    } else if (arg instanceof Number) {
      pageSize = arg;
    }
    this.pageSize = pageSize;
  }

  copy(offset, destPagedArray, destOffset, length) {
    for (let i = 0; i < length; i++) {
      destPagedArray.set(destOffset + i, this.get(offset + i));
    }
  }

  get(i) {
    // Check if the caller is indexing beyond the size of this array.
    if (i >= this.length) {
      return null;
    }
    const page = Math.floor(i / this.pageSize);
    const indexInPage = i % this.pageSize;
    return this.pages[page][indexInPage];
  }

  set(i, v) {
    if (i >= this._length) {
      this._length = i + 1;
    }
    const page = Math.floor(i / this.pageSize);
    const indexInPage = i % this.pageSize;
    if (this.pages.length <= page) {
      for (let i = 0; i <= page; i++) {
        if (this.pages[i] == null) {
          this.pages[i] = new this.arrayType(this.pageSize);
        }
      }
    }
    this.pages[page][indexInPage] = v;
  }

  get length() {
    return this._length;
  }

  get BYTES_PER_ELEMENT() {
    return this.arrayType.BYTES_PER_ELEMENT;
  }

  slice(from, to) {
    if (to == null) {
      to == this.length;
    } else if (to > this.length) {
      to = this.length;
    }
    const result = [];
    for (let i = from; i < to; i++) {
      result.push(this.get(i));
    }
    return result;
  }

  clone() {
    const dest = new PagedArray(this.arrayType, this.pageSize);
    for (const page of this.pages) {
      dest.pages.push(page.slice(0));
    }
    dest._length = this._length;
    return dest;
  }

  // Useful during development, for debugging.
  _print() {
    let printed = 0;
    for (let pIndex = 0; pIndex < this.pages.length; pIndex++) {
      console.log("Page", pIndex);
      const page = this.pages[pIndex];
      for (let i = 0; i < this.pageSize; i++) {
        console.log(page[i]);
        printed++;
        // The size of the page is likely greater than the number of elements that have been set.
        if (printed >= this.length) {
          return;
        }
      }
    }
  }
}

const ARRAY_TYPES = {
  dimenKeyToIndices: Uint32Array,
  metrics: Float32Array,
};

// Returns a DataCube from `rows` with the given dimens and metrics.
// - rows: a list of objects which have each dimen in `dimens` and each metric in `metrics` as
//   properties.
const fromRows = (dimens, metrics, rows) => {
  const dc = new DataCube(dimens, metrics);
  for (const row of rows) {
    dc.addRow(row);
  }
  return dc;
};

const readFromFile = (pathPrefix) => {
  // Make filePath into an absolute path, which is required by the fetch API.
  const isRelative = !pathPrefix.startsWith("/");
  if (isRelative) {
    pathPrefix = Deno.cwd() + "/" + pathPrefix;
  }
  return readFromUrl("file://" + pathPrefix);
};

/*
 * A datacube is serialized as 3 files and can be fetched from the network (using fetch) or a file.
 * - urlPrefix: the prefix of the path. E.g. if the datacube is in tmp/dc.json, the pathPrefix is
     tmp/dc
 *
 * NOTE(philc): This API could take a set of readable streams rather than just a path. We would need
 * to expose a variable which indicates the schema of how the files of a datacube are named.
 */
const readFromUrl = async (urlPrefix) => {
  const response = await fetch(`${urlPrefix}.json`);
  const manifest = await response.json();
  const dc = new DataCube(manifest.dimens, manifest.metrics);
  dc.dimenIndexToValue = manifest.dimenIndexToValue;

  // TODO(philc): This could be done more efficiently by copying/adopting the byte ranges directly.
  let bytes;
  bytes = await (await fetch(`${urlPrefix}.dimens.bin`)).arrayBuffer();
  dc.dimenKeyToIndices = new PagedArray(ARRAY_TYPES.dimenKeyToIndices, bytes);

  bytes = await (await fetch(`${urlPrefix}.metrics.bin`)).arrayBuffer();
  dc.metricsData = new PagedArray(ARRAY_TYPES.metrics, bytes);

  return dc;
};

class DataCube {
  constructor(dimens, metrics) {
    this.separator = ",";

    const pageSize = 100 * 1024;

    // Map of dimension value => dimension index.
    this.dimenValueToIndex = new Map();

    // Array of dimension index => dimension value;
    this.dimenIndexToValue = [];

    this.dimenKeyToIndices = new PagedArray(
      ARRAY_TYPES.dimenKeyToIndices,
      pageSize,
    );

    this.dimens = dimens;
    this.metrics = metrics;

    // An array indexed by "dimen key" (an integer index into dimenKeyToIndices)
    this.metricsData = new PagedArray(ARRAY_TYPES.metrics, pageSize);

    // Once the dimens and metrics in this dataview are known, we can generate code for a more
    // efficient implementation of getRows.
    this.getRows = this.genGetRows();

    this._getDimenIndices = (row) => this.dimens.map((d) => this.getDimenIndex(row[d]));
    this._stringKeyToIndex = new Map();
    this._getKey = this.dimens.length == 0
      ? () => null
      : (dimenIndices) => dimenIndices.join(this.separator);
  }

  clone() {
    const dc = new DataCube(this.dimens, this.metrics);
    dc.metricsData = this.metricsData.clone();
    dc.dimenKeyToIndices = this.dimenKeyToIndices.clone();
    dc.dimenIndexToValue = this.dimenIndexToValue.slice(0);
    dc.dimenValueToIndex = new Map(this.dimenValueToIndex);
    dc.getRows = this.getRows;
    return dc;
  }

  getWritableStream() {
    let rowCount = 0;
    return new WritableStream({
      write: (chunk) => {
        this.addRow(chunk);
        rowCount++;
        // Progress output
        if (rowCount % 1_000_000 == 0) {
          console.log(rowCount.toLocaleString("en-US"));
        }
      },
      close: () => {
      },
      abort: (err) => {
        console.log("Datacube writable stream error:", err);
      },
    });
  }

  addRow(row) {
    const indices = this._getDimenIndices(row);
    const key = this._getKey(indices);
    let rowIndex = this._stringKeyToIndex.get(key);

    if (rowIndex == null) {
      rowIndex = this.count();
      const dimenKeyToIndicesOffset = this.dimenKeyToIndices.length;
      for (let i = 0; i < indices.length; i++) {
        this.dimenKeyToIndices.set(dimenKeyToIndicesOffset + i, indices[i]);
      }
      this._stringKeyToIndex.set(key, rowIndex);
    }

    const metricsDataOffset = rowIndex * this.metrics.length;
    for (let m = 0; m < this.metrics.length; m++) {
      const metricName = this.metrics[m];
      const metricValue = this.metricsData.get(metricsDataOffset + m) || 0;
      this.metricsData.set(
        metricsDataOffset + m,
        metricValue + row[metricName],
      );
    }
  }

  assertValidDimensions(dimens) {
    const incorrectDimens = dimens.filter((d) => !this.dimens.includes(d));
    if (incorrectDimens.length > 0) {
      throw `These dimens are not part of the datacube: [${incorrectDimens}]. ` +
        `The datacube has: [${this.dimens}].`;
    }
  }

  // Returns a new copy of this DataCube containing only the dimensions in `dimens`.
  // - dimens: a list of dimensions which are present in this DataCube.
  select(dimens) {
    this.assertValidDimensions(dimens);
    const destDc = new DataCube(dimens, this.metrics);
    // TODO(philc): Build up a new dictionary for the affected dimension.
    destDc.dimenValueToIndex = new Map(this.dimenValueToIndex);
    destDc.dimenIndexToValue = this.dimenIndexToValue.slice(0);

    // I could get rid of the dimenKey here, if I stored just row data, and a separate map of key =>
    // rowIndex.
    const getKey = dimens.length == 0
      ? () => null
      : (dimenIndices) => dimenIndices.join(this.separator);

    const destDimenIndexToSrcIndex = dimens.map((d) => {
      return this.dimens.indexOf(d);
    });

    const stringKeyToIndex = new Map();

    const destIndices = new Array(dimens.length);

    for (let rowIndex = 0; rowIndex < this.count(); rowIndex++) {
      const metricsDataOffset = rowIndex * this.metrics.length;
      const dimenDataOffset = rowIndex * this.dimens.length;
      for (let i = 0; i < dimens.length; i++) {
        const srcDimenIndex = this.dimenKeyToIndices.get(
          dimenDataOffset + destDimenIndexToSrcIndex[i],
        );
        destIndices[i] = srcDimenIndex;
      }
      const newKey = getKey(destIndices);
      let index = stringKeyToIndex.get(newKey);

      if (index == null) {
        index = destDc.count();
        const dimenKeyToIndicesOffset = destDc.dimenKeyToIndices.length;
        for (let i = 0; i < dimens.length; i++) {
          destDc.dimenKeyToIndices.set(
            dimenKeyToIndicesOffset + i,
            destIndices[i],
          );
        }
        stringKeyToIndex.set(newKey, index);
      }

      for (let m = 0; m < destDc.metrics.length; m++) {
        const destMetricsDataOffset = index * destDc.metrics.length;
        const srcMetricValue = this.metricsData.get(metricsDataOffset + m);
        const destMetricValue = destDc.metricsData.get(destMetricsDataOffset + m) || 0;
        destDc.metricsData.set(
          destMetricsDataOffset + m,
          destMetricValue + srcMetricValue,
        );
      }
    }
    destDc.getRows = destDc.genGetRows();
    return destDc;
  }

  // Returns the index into dimenValueToIndex for `dimenValue`, inserting `dimenValue` if it's not
  // already present.
  getDimenIndex(dimenValue) {
    const i = this.dimenValueToIndex.get(dimenValue);
    if (i != null) return i;
    this.dimenIndexToValue.push(dimenValue);
    const insertedIndex = this.dimenIndexToValue.length - 1;
    this.dimenValueToIndex.set(dimenValue, insertedIndex);
    return insertedIndex;
  }

  // Returns an array which has the set of all values for the given dimension.
  getDimensionValues(dimen) {
    this.assertValidDimensions([dimen]);
    const dimenOffset = this.dimens.indexOf(dimen);
    const uniqueDimenIndices = new Set();
    for (let i = 0; i < this.count(); i++) {
      const dimenIndexOfRow = this.dimenKeyToIndices.get(
        i * this.dimens.length + dimenOffset,
      );
      uniqueDimenIndices.add(dimenIndexOfRow);
    }
    const dimenValues = Array.from(uniqueDimenIndices).map((i) => this.dimenIndexToValue[i]);
    return dimenValues;
  }

  // Returns an array of objects representing the rows in this DataCube. Each returned object has a
  // property for every dimension and every metric in this DataCube.
  getRows() {
    // The code for this function is generated at runtime by genGetRows, based on the number and
    // names of the dimens and metrics in this DataCube. This provides a substantial speedup.
  }

  // Generates the code for the `getRows` function.
  genGetRows() {
    // Generate an assignment statement of the form:
    // let row = { "dimen-name1": this.dimenIndexToValue[dimenIndicies[0], "dimen-name2": ... }
    // NOTE(philc): This is 2x faster than building up an object by iterating over this.dimens and
    // this.metrics. I think the main improvement comes from creating the object once, rather than
    // incrementally, so that its shape is fixed.
    const assignDimens = this.dimens.map(
      (d, i) => `"${d}": this.dimenIndexToValue[dimenIndices[${i}]]`,
    );
    const assignMetrics = this.metrics.map(
      (m, i) => `"${m}": this.metricsData.get(metricsDataOffset + ${i})`,
    );
    const assignStatement = "let row = { " +
      assignDimens.concat(assignMetrics).join(",\n") + " }";

    const template = function () {
      const count = this.count();
      const metricsCount = this.metrics.length;

      const rows = [];
      for (let rowIndex = 0; rowIndex < count; rowIndex++) {
        const metricsDataOffset = rowIndex * metricsCount;
        const dimenKeyToIndicesOffset = rowIndex * this.dimens.length;
        const dimenIndices = this.dimens.length == 0 ? [] : this.dimenKeyToIndices.slice(
          dimenKeyToIndicesOffset,
          dimenKeyToIndicesOffset + this.dimens.length,
        );
        ASSIGN_STATEMENT;
        rows.push(row);
      }
      return rows;
    };

    const code = template
      .toString()
      .replace("ASSIGN_STATEMENT", assignStatement);
    return new Function(`return ${code}`)();
  }

  // Returns true if `dimenKey` is included by `dimenFilters`.
  // - dimenKey: an array of integers representing a tuple of dimension values. The integers are
  //   indices into dimenIndexToValue.
  // - dimenFilters: a map of dimenName => filter. Filter can be either a primitive value, or a
  //   function.
  includeRow(dimenKey, dimenFilters) {
    const dimenIndices = this.dimenKeyToIndices.slice(
      dimenKey * this.dimens.length,
      (dimenKey + 1) * this.dimens.length,
    );
    for (let i = 0; i < this.dimens.length; i++) {
      const dimen = this.dimens[i];
      const filter = dimenFilters[dimen];
      if (filter == null) continue;
      const dimenValue = this.dimenIndexToValue[dimenIndices[i]];
      if (Array.isArray(filter)) {
        if (!filter.includes(dimenValue)) return false;
      } else if (typeof filter == "function") {
        if (!filter(dimenValue)) return false;
      } else if (dimenValue != filter) {
        return false;
      }
    }
    return true;
  }

  // Returns a new dataview which excludes rows which do not match `dimenFilters`.
  // - dimenFilters: a map of dimenName => filter. Filter can be either a primitive value, or a
  //   function.
  // TODO(philc): This should remove dimens from the dimen dictionary which were filtered out.
  where(dimenFilters) {
    if (Object.keys(dimenFilters).length == 0) return this;

    const dest = this.clone();
    const destMetricsData = new PagedArray(dest.metricsData.arrayType);
    const destDimenKeyToIndices = new PagedArray(dest.dimenKeyToIndices.arrayType);
    for (let rowIndex = 0; rowIndex < this.count(); rowIndex++) {
      if (this.includeRow(rowIndex, dimenFilters)) {
        this.dimenKeyToIndices.copy(
          rowIndex * this.dimens.length,
          destDimenKeyToIndices,
          destDimenKeyToIndices.length,
          this.dimens.length,
        );

        const srcMetricsDataOffset = rowIndex * this.metrics.length;
        const destMetricsDataOffset = destMetricsData.length;
        for (let m = 0; m < this.metrics.length; m++) {
          const metricValue = this.metricsData.get(srcMetricsDataOffset + m);
          destMetricsData.set(destMetricsDataOffset + m, metricValue);
        }
      }
    }
    dest.metricsData = destMetricsData;
    dest.dimenKeyToIndices = destDimenKeyToIndices;
    return dest;
  }

  count() {
    return this.metricsData.length / this.metrics.length;
  }

  getMetricsDataOffset(rowIndex) {
    return rowIndex * this.metrics.length;
  }

  // Returns the metric map containing the sums of all metrics over all cells.
  totals() {
    const totals = this.metrics.map(() => 0);
    for (let rowIndex = 0; rowIndex < this.count(); rowIndex++) {
      const metricsDataOffset = this.getMetricsDataOffset(rowIndex);
      for (let m = 0; m < this.metrics.length; m++) {
        totals[m] += this.metricsData.get(metricsDataOffset + m);
      }
    }

    const resultMap = {};
    for (let i = 0; i < totals.length; i++) {
      resultMap[this.metrics[i]] = totals[i];
    }
    return resultMap;
  }

  // Creates a new DataCube where `dimen` is removed from the DataCube's dimensions, and instead,
  // its values are added as separate columns.
  // E.g. Let dc be a DataCube with dimensions [country, customer] and metrics [spend].
  // dc.explodeDimenIntoColumns(country, null) produces a DataCube with dimens [customer] and
  // metrics [us-spend, jp-spend, ...].
  // This is commonly useful when analyzing AB tests, where you want each row to include metrics for
  // both the control and the experiment group of the AB test.
  // - keyNameFn: a function which tags `dimenValue` and `metric` as arguments, and returns the name
  //   of the new metric. When null, the new metric names will be "dimenValue-metric".
  explodeDimenIntoColumns(dimen, keyNameFn) {
    this.assertValidDimensions([dimen]);
    const rows = this.getRows();
    const metricNames = [];
    for (const row of rows) {
      const dimenValue = row[dimen];
      for (const metric of this.metrics) {
        const newMetric = keyNameFn ? keyNameFn(dimenValue, metric) : `${dimenValue}-${metric}`;
        if (metricNames.indexOf(newMetric) == -1) metricNames.push(newMetric);
        row[newMetric] = row[metric];
        // We don't delete `metric` (now unused) from the row, because doing so is costly and
        // unnecessary.
      }
    }
    const reducedDimens = Array.from(this.dimens);
    reducedDimens.splice(this.dimens.indexOf(dimen), 1);
    return fromRows(reducedDimens, Array.from(metricNames), rows);
  }

  async writeToFile(pathPrefix) {
    const jsonStruct = {
      dimens: this.dimens,
      metrics: this.metrics,
      count: this.count(),
      dimenIndexToValue: this.dimenIndexToValue,
    };
    await Deno.writeTextFile(
      `${pathPrefix}.json`,
      JSON.stringify(jsonStruct, null, 2),
    );

    const writePagedArray = async (fileName, pagedArray) => {
      const file = await Deno.create(fileName);
      const writer = file.writable.getWriter();
      for (const [pIndex, page] of pagedArray.pages.entries()) {
        let chunk;
        if ((pIndex + 1) * page.length < pagedArray.length) {
          chunk = page;
        } else {
          chunk = page.slice(0, pagedArray.length - (pIndex * page.length));
        }
        // NOTE(philc): I believe Deno.write should work with a Uint32Array and other typed arrays,
        // but it doesn't at the time of writing. It fails with "expected typed ArrayBufferView". So
        // first we convert our typed array to a Uint8Array.
        await writer.write(new Uint8Array(chunk.buffer));
      }
      writer.close();
    };

    await writePagedArray(`${pathPrefix}.dimens.bin`, this.dimenKeyToIndices);
    await writePagedArray(`${pathPrefix}.metrics.bin`, this.metricsData);
  }

  // Collapses all rows where the value of `dimen` is not one of the top `n`, as determined by
  // sorting using the given `compare-fn`. The dimension value for the collapsed rows will be
  // replaced with `placeholder-value`.
  aggregateTailValues(dimen, compareFn, n, placeholderValue) {
    const sortedRows = this.select([dimen]).getRows().sort(compareFn).slice(
      0,
      n,
    );
    const set = new Set(sortedRows.map((row) => row[dimen]));
    // This could be made more efficient by operating on the dataview's internal data structures, so
    // we avoid materializing the row maps.
    const destDv = new DataCube(this.dimens, this.metrics);
    for (const row of this.getRows()) {
      if (!set.has(row[dimen])) {
        row[dimen] = placeholderValue;
      }
      destDv.addRow(row);
    }
    return destDv;
  }
}

export { DataCube, fromRows, readFromFile, readFromUrl };
