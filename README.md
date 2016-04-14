[![Build Status](https://travis-ci.org/danielecook/rdatastore.svg?branch=master)](https://travis-ci.org/danielecook/rdatastore)

# rdatastore
R package for accessing google datastore


### To Do

### Functions

* [x] `lookup` - Lookup single entities. Returns a data frame.
* [x] `commit` - Update a single entity.
* [ ] `commit_df` - Save a dataframe to the google datastore with update/upsert/insert/delete (consistant data).
* [ ] `commit_list` - Save a list (heterogeneous data).
* [ ] `runQuery` - Function for querying. Possibly using dplyr style.

### Features

* [ ] __keep_existing__ When updating - keep existing values, add new ones, and overwrite where specified.
