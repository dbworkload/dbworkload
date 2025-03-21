# merge_csvs

## Merge multiple dbworkload statistic CSV files

There are situations where you need to run `dbworkload` from multiple servers, for example,
to simulate a distributed app targeting different nodes in your database technology.

You can use the `--save|-s` flag in your `dbworkload run` command to save metrics to a CSV files,
but eventually you want a unified view of your test.

This function allows you to merge multiple such statistics CSV files into a single CSV file.

### Example

`merge` accepts a directory as input and will output a new statistic CSV file.

```bash
dbworkload util merge_csvs -i stats/
```

## See also

- [`dbworkload util merge_csvs`](../cli.md#dbworkload-util-merge_csvs)
