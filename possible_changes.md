# Possible Changes & Future Enhancements

## Completed ✅

- ✅ **Fixed slow master data processing** - Implemented batch processing optimization (~29x speedup)
  - Previously: Read mdrfoithru2024.zip file 29 times (once per year)
  - Now: Read file once and filter for all requested years simultaneously
  - See `OPTIMIZATION_SUMMARY.md` for technical details

## Known Issues

- **Duplicate year handling** - Adding a year already in database will duplicate data
  - Currently unclear if it duplicates or overwrites
  - Need better "update only" mechanism for incremental updates
  - Workaround: Check `db.info()` or `db._get_years_in_db()` before adding years

- **Test suite needs updates** - Some integration tests failing (10 failures)
  - Interactive mode tests fail in pytest (stdin prompts)
  - Network-dependent tests may fail without internet
  - Tests need `interactive=False` parameter added

## Future Enhancements

- **Deduplication logic** - Prevent duplicate records when re-adding years
- **Incremental updates** - Support for monthly FDA update files (`*add.zip`, `*change.zip`)
- **Parallel table processing** - Download/process multiple tables simultaneously
- **Progress bars** - Better visual feedback for long-running operations
- **Query builder** - Higher-level API for complex queries without SQL
- **Data validation** - Verify row counts and data integrity after import
