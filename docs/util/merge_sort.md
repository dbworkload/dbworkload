# merge_sort

## Merge Sort multiple sorted CSV files into 1+ files

Sorted CSV files as those created using the [csv](csv.md) function are sorted on a file by file basis, but not across the files.

`dbworkload` can assist you with re-writing all rows into new CSV files where the rows are sorted across all files.

This is known as a **merge-sort** operation.

### Example

Create 2 files such as these.
Notice the rows are sorted alphabetically on a file by file basis, but not across all files.

```bash
$ cat bank/t.0.csv 
aaa
ddd
fff
kkk
rrr
zzz

$ cat bank/t.1.csv 
bbb
ggg
lll
ooo
ttt
yyy
```

Run the `merge_sort` command

```bash
dbworkload util merge_sort -i bank/
```

You will notice a new directory has been created. Inspect the file

```bash
$ cat bank.merged/out_000.csv 
aaa
bbb
ddd
fff
ggg
kkk
lll
ooo
rrr
ttt
yyy
zzz
```

Rows are now correctly sorted. Use option `--csv-max-rows` to split the output in many, smaller files.

## See also

- [`dbworkload util merge_sort`](../cli.md#dbworkload-util-merge_sort)
- <a href="https://dev.to/cockroachlabs/generate-multiple-large-sorted-csv-files-with-pseudo-random-data-1jo4" target="_blank">Blog: Generate multiple large sorted CSV files with pseudo-random data</a>
