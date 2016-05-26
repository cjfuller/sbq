# sbq
Low-dependency utility for running bigquery queries programatically.

# Installation

`pip install sbq`
You'll need the `gcloud` tool on your path, and should have run `gcloud auth login <account>` at some point in history.

# Example usage

At the top of your script file, set up the account and project you're using to query:

```python
from sbq import sbq

sbq.account('colin@khanacademy.org')
sbq.project('khanacademy.org:deductive-jet-827')
```

Then set up your query and run it, and any parameters will be interpolated using string formatting, 
and output will be saved to your specified table.

```python
sbq.params({'select_items': 'some_field'})

@sbq.query('destination_table', 'destination_dataset')
def my_query_fn():
    return """SELECT {select_items} FROM [dataset.my_awesome_table]"""
    
my_query_fn()
```

Want to run a quick query and just print the output without saving it?
Just don't supply a table or dataset, and the results will be printed as 
JSON (though there's a limit to how many rows we can grab via the API).

```python
@sbq.query()
def my_query_fn():
    return """SELECT {select_items} FROM [dataset.my_awesome_table]"""
    
result = my_query_fn()
print(result)
```

There's also a pretty printer, which you can use via `@sbq.query(print_result=True)`.
