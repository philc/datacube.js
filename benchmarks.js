/*
 * This is used occasionally during development to measure performance optimizations.
 */
import * as DataCube from "./datacube.js";
import * as DataCube2 from "./datacube2.js";

const Bench = {
  labelToTime: new Map(),
  reset() {
    this.labelToTime.clear();
  },

  async timeMultiple(label, count, fn) {
    const start = performance.now();
    for (let i = 0; i < count; i++) {
      await fn();
    }
    const end = performance.now();
    const sum = this.labelToTime.get(label) || 0;
    const avgDuration = (end - start) / count;
    this.labelToTime.set(label, sum + avgDuration);
  },

  async time(label, fn) {
    const start = performance.now();
    const returnVal = await fn();
    const end = performance.now();
    const sum = this.labelToTime.get(label) || 0;
    this.labelToTime.set(label, sum + (end - start));
    return returnVal;
  },

  print() {
    for (const [label, time] of this.labelToTime) {
      console.log(`${label}: ${time.toFixed(2)}ms`);
    }
  },
};

const inputDataset = [];
const rowCount = 500_000;
for (let i = 0; i < rowCount; i++) {
  inputDataset.push({ "d1": i, "d2": i, "d3": i, "m1": 1, "m2": 2, "m3": 3 });
}

const setup = () => {
  const dc = new DataCube.DataCube(["d1", "d2", "d3"], ["m1", "m2", "m3"]);
  for (let i = 0; i < inputDataset.length; i++) {
    dc.addRow(inputDataset[i]);
  }
  return dc;
};

const runCount = 6;

const runBench1 = async () => {
  let dc1;
  let dc2;

  await Bench.timeMultiple("setup dc - addRows", 1, () => dc1 = setup());

  await Bench.timeMultiple(
    "setup dc2 - fromRows",
    1,
    () =>
      dc2 = DataCube2.fromRows(
        ["d1", "d2", "d3"],
        ["m1", "m2", "m3"],
        inputDataset,
      ),
  );

  Bench.timeMultiple("getRows dc1", runCount, () => dc1.getRows());
  Bench.timeMultiple("getRows dc2", runCount, () => dc2.getRows());

  Bench.print();

  // Sanity checks to ensure functionality doesn't break as we're benchmarking.
  // console.assert(dc1.count() == dc2.count(), "Count mismatch");
  // console.assert(
  //   JSON.stringify(dc1.getRows()[0]) == JSON.stringify(dc2.getRows()[0]),
  //   "Equality mismatch",
  // );

};
