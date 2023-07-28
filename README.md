# pqdump
A simple program to dump Parquet files.

## Usage
```
Usage: pqdump [OPTIONS] <INPUT>

Arguments:
  <INPUT>  

Options:
  -n, --length             Print the number of rows and exit
  -A, --only-types         Print the datatypes only
      --no-types           Suppress printing the datatypes
      --head <HEAD>        Print the first rows
      --tail <TAIL>        Print the last rows
      --columns <COLUMNS>  Print the specified columns
      --exclude <EXCLUDE>  Suppress the specified columns
  -h, --help               Print help
  -V, --version            Print version
```
