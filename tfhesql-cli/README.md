
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
- ``--mode`` : Optional. Default value is ``encrypted``
    - ``encrypted``: the query is executed using FHE encrypted data (very slow)
    - ``trivial``: the query is executed using trivialy encrypted data (fast)
    - ``clear``: the query is executed using rust native types (bool and u8) (usefull for quick testing)

## Output

The output 'tries' to strictly follow the instructions given in the bounty. The bounty instructions were very vague. I did my best to understand the requirements...

- First group: ``Runtime: <duration in seconds>``
- Second group: ``Clear DB query result:`` followed by a multiline result in csv format
- Third group: ``Encrypted DB query result:`` followed by the encrypted result in JSON string format

Note: ``Results match: YES`` is ignored by lack of documentation.


