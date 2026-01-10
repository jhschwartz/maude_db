# Possible Changes & Future Enhancements


- **Confirm utility of different tables** 
  - What do they each have? are the master and patient tables even necessary? would be nice to not use them. it's possible they each contain minimal data already present elsewhere for convinience.


- **noticed in notebooks**
  - need to confirm each notebook runs, and commit the notebook after all output is shown
  - seems like cacheing is not working for master table and it may be reloading years unnecessarily 
  - notebooks 1/2/3/4/6 are passing, saved with proper output
  - notebooks 5 has a large bug 
  - notebook 7 needs more data loading, not yet run, just run when I can and confirm ran properly 


--> should integration tests spot check actual data from fda?? 



## Future Enhancements

- **Deduplication logic** - Prevent duplicate records when re-adding years
- **Incremental updates** - Support for monthly FDA update files (`*add.zip`, `*change.zip`)
- **Parallel table processing** - Download/process multiple tables simultaneously
- **Progress bars** - Better visual feedback for long-running operations
- **Query builder** - Higher-level API for complex queries without SQL
- **Data validation** - Verify row counts and data integrity after import
