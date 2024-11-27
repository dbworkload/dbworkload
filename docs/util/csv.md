# csv

## Generating CSV files

You can seed a database quickly by letting `dbworkload` generate pseudo-random data and import it.

`dbworkload` takes the DDL as an input and [creates an intermediate YAML file](yaml.md), with the definition of what data you want to create (a string, a number, a date, a bool..) based on the column data type.

You then refine the YAML file to suit your needs, for example, the size of the string, a range for a date, the precision for a decimal, a choice among a discrete list of values..

You can also specify what is the percentage of NULL for any column, or how many elements in an ARRAY type.
You then specify the total row count, how many rows per file, and in what order, if any, to sort by.

Then `dbworkload` will generate the data into CSV or TSV files, compress them if so requested.

You can then optionally [merge-sort the files](merge_sort.md).

### Example

Here is a sample input YAML file, `bank.yaml`.

```yaml
ref_data:
- count: 1000
  sort-by: 
    - acc_no
  columns:
    acc_no:
      type: sequence
    external_ref_id:
      type: uuid
    created_time:
      type: timestamp
    acc_details:
      type: string
```

Now let's create a CSV dataset, using only 1 processor.

```bash
dbworkload util csv -i bank.yaml -x 1
```

The CSV files will be located inside a `bank` directory.

Inspect it

```bash
$ head -n5 bank/ref_data.0_0_0.tsv 
0       3a2edc9d-a96b-4541-99ae-0098527545f7    2008-03-19 06:20:27.209214      CWUh0FWashpmWCx4LF3kb1
1       829de6d6-103c-4707-9668-c4359ef5373c    2014-02-13 22:04:20.168239      QGspICZBHYpRLnHNcg
2       5dd183af-d728-4e12-8b11-2900b6f6880a    2019-04-01 16:14:40.388236      sEUukccOePdnIbiQyVUSi0HS7rL
3       21f00778-5fca-4302-8380-56fa461adfc8    2003-05-21 19:21:21.598455      OQTNwxoZIAdNmcA6fJM5eGDvMJgKJ
4       035dac61-b4a3-40a4-9e4d-0deb50fef3ae    2011-08-15 06:15:40.405698      RvToVnn20BEXoxFzw9QFpCt
```

## See also

- [`dbworkload util csv`](../cli.md#dbworkload-util-csv)

- [Generating intermediate data definition YAML file](yaml.md)

- <a href="https://dev.to/cockroachlabs/generate-multiple-large-sorted-csv-files-with-pseudo-random-data-1jo4" target="_blank">Blog: Generate multiple large sorted CSV files with pseudo-random data</a>
