# Possible Changes & Future Enhancements


- **Confirm utility of different tables** 
  - What do they each have? are the master and patient tables even necessary? would be nice to not use them. it's possible they each contain minimal data already present elsewhere for convinience.


- **why so few rows in master for some years**
  - check again...?


- **cache zip**
  - right now cached files used are zip
  - should it cache txt? if not, then delete the txt after use.


## Future Enhancements

- **Deduplication logic** - Prevent duplicate records when re-adding years
- **Incremental updates** - Support for monthly FDA update files (`*add.zip`, `*change.zip`)
- **Parallel table processing** - Download/process multiple tables simultaneously
- **Progress bars** - Better visual feedback for long-running operations
- **Query builder** - Higher-level API for complex queries without SQL
- **Data validation** - Verify row counts and data integrity after import
