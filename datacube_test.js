import * as shoulda from "https://deno.land/x/shoulda@v2.0/shoulda.js";
const { assert, context, setup, should } = shoulda;

import * as DataCube from "./datacube.js";

context("datacube", () => {
  const rows = [
    { d1: "a", m1: 2 },
    { d1: "b", m1: 3 },
  ];

  const rows2 = [
    { d1: "a", d2: "b", m1: 2 },
    { d1: "a", d2: "c", m1: 3 },
  ];

  let dc;
  let dc2;

  setup(() => {
    dc = DataCube.fromRows(["d1"], ["m1"], rows);
    dc2 = DataCube.fromRows(["d1", "d2"], ["m1"], rows2);
  });

  should("calculate totals", () => {
    assert.equal(2, dc.count());
    assert.equal({ m1: 5 }, dc.totals());
  });

  should("createRows", () => {
    assert.equal(rows, dc.getRows());
  });

  should("dimens", () => {
    assert.equal(["d1"], dc.dimens);
  });

  should("metrics", () => {
    assert.equal(["m1"], dc.metrics);
  });

  should("select", () => {
    // Select the first dimension.
    assert.equal([{ d1: "a", m1: 5 }], dc2.select(["d1"]).getRows());

    // Select the second dimension.
    assert.equal(
      [
        { d2: "b", m1: 2 },
        { d2: "c", m1: 3 },
      ],
      dc2.select(["d2"]).getRows(),
    );
  });

  should("select with zero dimensions", () => {
    const selected = dc.select([]);
    assert.equal([{ m1: 5 }], selected.getRows());
  });

  should("where", () => {
    assert.equal(rows, dc.where({}).getRows());
    assert.equal([rows[0]], dc.where({ d1: "a" }).getRows());
    assert.equal([rows[0]], dc.where({ d1: ["a"] }).getRows());
    assert.equal(rows, dc.where({ d1: ["a", "b"] }).getRows());
    assert.equal([rows[1]], dc.where({ d1: (d1) => d1 == "b" }).getRows());
  });

  should("reduce dimensions when creating a datacube", () => {
    // DataCube is only using the d1 dimension, not d2.
    const dc = DataCube.fromRows(["d1"], ["m1"], rows2);
    assert.equal([{ d1: "a", m1: 5 }], dc.getRows());
  });

  should("explodeDimenIntoColumns", () => {
    const dc = dc2.explodeDimenIntoColumns("d1");
    assert.equal(
      [
        { d2: "b", "a-m1": 2 },
        { d2: "c", "a-m1": 3 },
      ],
      dc.getRows(),
    );
  });

  should("writeToFile", async () => {
    const path = "/tmp/test_datacube";
    await dc.writeToFile(path);
    const dcFromFile = await DataCube.readFromFile(path);

    // Clean up the files that were written
    await Deno.remove(path + ".json");
    await Deno.remove(path + ".dimens.bin");
    await Deno.remove(path + ".metrics.bin");

    assert.equal(dc.getRows(), dcFromFile.getRows());
  });

  should("aggregateTailValues", () => {
    const resultDc = dc.aggregateTailValues(
      "d1",
      (a, b) => b["m1"] - a["m1"],
      1,
      "tail",
    );
    assert.equal(
      [
        { "d1": "tail", m1: 2 },
        { "d1": "b", m1: 3 },
      ],
      resultDc.getRows(),
    );
  });

  should("getDimensionValues", () => {
    assert.equal(["a"], dc2.getDimensionValues("d1"));
    assert.equal(["b", "c"], dc2.getDimensionValues("d2"));
  });
});
