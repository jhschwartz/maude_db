# Possible Changes & Future Enhancements


- **Confirm utility of different tables** 
  - What do they each have? are the master and patient tables even necessary? would be nice to not use them. it's possible they each contain minimal data already present elsewhere for convinience.


- **noticed in notebooks**
  - why are deaths, malfunctions, etc. always zero? 
  - notebook 03 --> is it plotting cumulatively by year? because it goes up each year in prelim analysis
  - need to confirm each notebook runs, and commit the notebook after all output is shown


--> should integration tests spot check actual data from fda?? 



## Future Enhancements

- **Deduplication logic** - Prevent duplicate records when re-adding years
- **Incremental updates** - Support for monthly FDA update files (`*add.zip`, `*change.zip`)
- **Parallel table processing** - Download/process multiple tables simultaneously
- **Progress bars** - Better visual feedback for long-running operations
- **Query builder** - Higher-level API for complex queries without SQL
- **Data validation** - Verify row counts and data integrity after import
