# WIP Disclaimer
This template is currently work-in-progress. Feel free to play around with it and give us feedback. Note also that this template depends on a development version of DuckDB. Follow https://duckdb.org/news for more information on official launch.

# DuckDB Extension Template
The main goal of this template is to allow users to easily develop, test and distribute their own DuckDB extension.

## Getting started
First step to getting started is to create your own repo from this template by clicking `Use this template`. Then clone your new repository using 
```sh
git clone --recurse-submodules https://github.com/<you>/<your-new-extension-repo>.git
```
Note that `--recurse-submodules` will ensure the correct version of duckdb is pulled allowing you to get started right away.

## Building
To build the extension:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/<extension_name>/<extension_name>.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded. 
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `<extension_name>.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `quack()` that takes a string arguments and returns a string:
```
D select quack('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Quack Jane 🐥 │
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

## Getting started with your own extension
After creating a repository from this template, the first step is to name your extension. To rename the extension, run:
```
python3 ./scripts/set_extension_name.py <extension_name_you_want>
```
Feel free to delete the script after this step.

Now you're good to go! After a (re)build, you should now be able to use your duckdb extension:
```
./build/release/duckdb
D select <extension_name_you_chose>('Jane') as result;
┌─────────────────────────────────────┐
│                result               │
│               varchar               │
├─────────────────────────────────────┤
│ <extension_name_you_chose> Jane 🐥  │
└─────────────────────────────────────┘
```

For inspiration/examples on how to extend DuckDB in a more meaningful way, check out the in-tree 
[extensions](https://github.com/duckdb/duckdb/tree/master/extension) (or in your `duckdb` submodule) and the 
out-of-tree extensions in [duckdblabs](https://github.com/duckdblabs)! 

## Distributing your extension
Easy distribution of extensions built with this template is facilitated using a similar process used by DuckDB itself. 
Binaries are generated for various versions/platforms allowing duckdb to automatically install the correct binary.

This step requires that you pass the following 4 parameters to your GitHub repo as action secrets:

| secret name   | description                         |
| ------------- | ----------------------------------- |
| S3_REGION     | s3 region holding your bucket       |
| S3_BUCKET     | the name of the bucket to deploy to |
| S3_DEPLOY_ID  | the S3 key id                       |
| S3_DEPLOY_KEY | the S3 key secret                   |

After setting these variables, all pushes to master will trigger a new (dev) release. Note that your AWS token should
have full permissions to the bucket, and you will need to have ACLs enabled.

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the 
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. 

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension 
you want to install.
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/duckcon/v0.0.1';
    
```

After running these steps, you can install and load your extension using the regular INSTALL/LOAD command:
```sql
INSTALL <your extension name>
LOAD <your extension name>
```
Note: your DuckDB version should match up with the binaries you have distributed. Which versions this is will be 
depends on the commit you have pinned in your submodule and the extra versions you have specified in the GitHub actions 
workflows.

## Setting up CLion 
Configuring CLion with the extension template requires a little work. Firstly, make sure that the DuckDB submodule is available. 
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

Now to configure the build targets, copy the CMake variables specified in the Makefile and ensure
the build directory is set to `../build/<build_mode>`.