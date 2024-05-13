
## Usage

```
cargo run --release -- --input-db /path/to/db/dir --query-file query.txt
```

## Options

- ``--input-db`` : path to the directory containing the database csv files. 
Table names are automatically computed using the csv filename. For example, ``my-table.csv``
will generate a table named ``my-table``. The cli does not check the name validity.

- ``--query-file`` : path to a text file containing the a single line sql select query. Example of a 'query.txt' file:

```bash
SELECT CustomerID,PostalCode,Country FROM Customers WHERE Country='Germany'
```
- ``--mode`` : Optional. Default value is ``check-encrypt``
    - ``check-encrypt``: the query is executed using rust native types (bool and u8) and also using FHE encrypted data, outputs the two results in csv format and performs the comparison
    - ``encrypt``: the query is executed using FHE encrypted data (very slow), outputs the result in csv format
    - ``trivial``: the query is executed using trivialy encrypted data (fast), outputs the result in csv format
    - ``clear``: the query is executed using rust native types (bool and u8) (usefull for quick testing), outputs the result in csv format

## Example

Enter the ``tfhesql-cli`` root directory
```
$ cd /path/to/tfhesql-cli
```

Type one of the following commands:

- ``check-encrypt`` mode (default, slow)
```bash
cargo run --release -- --input-db ../tfhesql/test/data/tiny --query-file ../tfhesql/test/queries/query-eq.txt 
```

- ``clear`` mode (fast)
```bash
cargo run --release -- --input-db ../tfhesql/test/data/tiny --query-file ../tfhesql/test/queries/query-eq.txt --mode clear 
```

