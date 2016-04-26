[![Build Status](https://travis-ci.org/danielecook/rdatastore.svg?branch=master)](https://travis-ci.org/danielecook/rdatastore)

# rdatastore
R package for accessing google datastore


### To Do

* [x] Add tests for autoallocation of ids when committing.
* [x] Add tests for __keep_existing__ T/F.
* [x] Add test for datetime consistancy.
* [x] When committing, return data and transaction id.
* [ ] Add further Blob support.
* [ ] Prohibit list, vector, and matrix storage; allow for object storage.

### Functions

* [x] `lookup` - Lookup single entities. Returns a data frame.
* [x] `commit` - Update a single entity.
* [ ] `commit_df` - Save a dataframe to the google datastore with update/upsert/insert/delete (consistant data).
* [ ] `commit_list` - Save a list (heterogeneous data).
* [x] `runQuery` - Function for querying. Possibly using dplyr paradigms.
* [ ] `datasets` - A centralized function for tracking/accessing datasets. Read only store.
* [/] `gql` - Add ability to return all results (batches).

### Features

* [x] __keep_existing__ When updating - keep existing values, add new ones, and overwrite where specified.
* [x] Blob Store - limited.

#### `commit_df()`

User specifies a key (unique value) for a given dataframe

__Planned Arguments__:

* use row numbers/names - Use row names for 'names' column and/or numbers; Retreived data will be slightly modified.
* keep_existing - retain existing data; only update values/insert new ones.

#### `datasets()`

The aim of the datasets function is to centralize storage of datasets and make them easily searchable. 

#### `gql()`

Used for running queries.
