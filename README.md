# Datacube.js - High-performance data cubes in JavaScript

Datacube.js is a library for efficiently filtering and manipulating OLAP data cubes in JavaScript.

An OLAP data cube is a set of rows, each containing one or more metrics which are grouped by a set
of dimensions.

A common use case is to use this as a foundation for building dashboards of very large analytics
data sets, with interactive real-time filtering.

## Example usage for an e-commerce site

```javascript
const dimens = ["date", "product", "category", "country"];
const metrics = ["views", "purchases", "revenue"];
const dc = new DataCube(dimens, metrics);

// Assume there are millions of rows, usually fetched from a SQL database. Example row:
// { "date": "2023-01-05", "product": "Hamlet", "category": "fiction", "country": "USA",
//   "views": "84,123", "purchases": 125, "revenue": 1561.25 }
for (const row of rows) {
  dc.addRow(row);
}

// Get the summed metrics, grouped by date.
const byDate = dc.select(["date"]).getRows();

// Get the summed metrics, grouped by product and country.
const byProductAndCountry = dc.select(["product", "country"]);

// Get the summed metrics for the fiction category.
const totalsForFiction = dc.where({ category: (c) => c == "fiction" }).totals();
```

## License

Licensed under the [MIT license](LICENSE.txt).
