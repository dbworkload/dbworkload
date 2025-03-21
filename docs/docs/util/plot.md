# plot

## Plot terminal charts from the statistics CSV file

`dbworkload` can plot terminal charts so you can inspect the workload directly from the instance
you are running the workload.

### Example

Run the `plot` command, passing in your CSV as the input.

```bash
dbworkload util plot -i Bank.20241021_145825.csv 
```

The output is displayed right on the terminal

![plot](../getting_started/media//plot.png)

## See also

- [`dbworkload util plot`](../cli.md#dbworkload-util-plot)
- [Plot the metrics](../getting_started/7.md)
