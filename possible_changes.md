# Possible Changes & Future Enhancements


- **Confirm utility of different tables** 
  - What do they each have? are the master and patient tables even necessary? would be nice to not use them. it's possible they each contain minimal data already present elsewhere for convinience.


- **noticed in notebooks**
  - seems like cacheing is not working for master table and it may be reloading years unnecessarily -- specifically, if one year is added, then all years existing get deleted and reinitialized
  - notebooks 1-6 ok, failure in nb 7
  - notebooks will likely be redone with upcoming claude updates upon usage refresh


--> should integration tests spot check actual data from fda?? 

--> must update init_db, notebooks, and archive_tools
--> must go back through docs and confirm no references to old naming 
--> must get rid of the hierarchial search code and references to it 
--> must go through code and delete functionality that won't be used
--> must use the widget (and other stuff) on real data to make sure it works 


--> need to do AI categorization implementation and analysis -- regular LLM or specific model? 


--> must review final codebase, tests, and docs in depth and update as necessary 


## Future Enhancements

- **Deduplication logic** - Prevent duplicate records when re-adding years
- **Incremental updates** - Support for monthly FDA update files (`*add.zip`, `*change.zip`)
- **Parallel table processing** - Download/process multiple tables simultaneously
- **Progress bars** - Better visual feedback for long-running operations
- **Query builder** - Higher-level API for complex queries without SQL
- **Data validation** - Verify row counts and data integrity after import
