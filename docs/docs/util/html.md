# html

## Save charts to HTML from the dbworkload statistics CSV file

`dbworkload` can create an interactive chart in a self contained HTML file.

### Example

1. Run the `html` command, passing in your CSV as the input.

    ```bash
    dbworkload util html -i Bank.20241021_145825.csv 
    ```

2. Open resulting file `Bank.20241021_145825.html` in your browser

    ![html](../getting_started/media//html.png)

## See also

- [`dbworkload util html`](../cli.md#dbworkload-util-html)
- [Plot the metrics](../getting_started/7.md)
