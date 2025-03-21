# Pipe

You can modify the connection count live during a workload run via a **pipe**.

When dbworkload runs, it creates a pipe in your current directory called `dbworkload.pipe`:

```bash
$ ls -l dbworkload.*
prw-r--r--  1 fabio  staff  0 Mar 21 09:31 dbworkload.pipe
```

You can write into this pipe a positive or negative number to add or remove connections.

## Example

```bash
# add 10 new connections
echo 10 > dbworkload.pipe

# remove 5 connections
echo -5 > dbworkload.pipe
```
