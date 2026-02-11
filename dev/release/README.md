<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Releasing SpatialBench

## Verifying a release candidate

### Testing locally (before creating a release candidate)

Before creating a release candidate, you should test your local checkout:

```shell
# git clone https://github.com/apache/sedona-spatialbench.git && cd sedona-spatialbench
# or
# cd existing/sedona-spatialbench && git fetch upstream && git switch main && git pull upstream main
dev/release/verify-release-candidate.sh
```

This will run all verification tests on your local checkout without requiring any release artifacts.

### Testing a local tarball

To create a local tarball for testing:

```shell
VERSION="0.1.0" && git archive HEAD --prefix=apache-sedona-spatialbench-${VERSION}/ | gzip > apache-sedona-spatialbench-${VERSION}-src.tar.gz
dev/release/verify-release-candidate.sh apache-sedona-spatialbench-${VERSION}-src.tar.gz
```

### Verifying an official release candidate

Once a release candidate has been uploaded to Apache dist, verify it using:

```shell
dev/release/verify-release-candidate.sh 0.1.0 1
```

This will download the release candidate from `https://dist.apache.org/repos/dist/dev/sedona/` and verify it.

Release verification requires:
- A recent Rust toolchain (can be installed from <https://rustup.rs/>)
- Java (for Apache RAT license checking)
- Python (for RAT report filtering)

The verification script will:
1. Run Apache RAT to check all files have proper license headers
2. Build and test all Rust crates in the workspace

When verifying via Docker or on a smaller machine it may be necessary to limit the
number of parallel jobs to avoid running out of memory:

```shell
export CARGO_BUILD_JOBS=4
```

## Creating a release

Create a release branch on the corresponding remote pointing to the official Apache
repository (i.e., <https://github.com/apache/sedona-spatialbench>). This step must be done by
a committer.

```shell
git pull upstream main
git branch -b branch-0.1.0
git push upstream -u branch-0.1.0:branch-0.1.0
```

When the state of the `branch-x.x.x` branch is clean and checks are complete,
the release candidate tag can be created:

```shell
git tag -a sedona-spatialbench-0.1.0-rc1 -m "Tag Apache Sedona SpatialBench 0.1.0-rc1"
git push upstream sedona-spatialbench-0.1.0-rc1
```

### Signing Commands

Now the assets need to be signed with signatures.

**GPG Signing:**

```shell
# Sign a file (creates .asc file automatically)
gpg -ab apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz

# Verify a signature
gpg --verify apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz.asc apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz
```

**SHA512 Checksum:**

```shell
# Generate SHA512 checksum
shasum -a 512 apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz > apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz.sha512

# Verify a checksum
shasum -a 512 --check apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz.sha512
```

**Upload to Apache SVN:**

After the assets are signed, they can be committed and uploaded to the
dev/sedona directory of the Apache distribution SVN:

```shell
# Set version and RC number variables
SEDONA_VERSION="0.1.0"
RC_NUMBER="1"

# Create the directory in SVN
svn mkdir -m "Adding folder" https://dist.apache.org/repos/dist/dev/sedona/sedona-spatialbench-${SEDONA_VERSION}-rc${RC_NUMBER}

# Checkout the directory
svn co https://dist.apache.org/repos/dist/dev/sedona/sedona-spatialbench-${SEDONA_VERSION}-rc${RC_NUMBER} tmp

# Copy files to the checked out directory
cp apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz tmp/
cp apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz.asc tmp/
cp apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz.sha512 tmp/

# Add and commit the files
cd tmp
svn add apache-sedona-spatialbench-${SEDONA_VERSION}-src.tar.gz*
svn ci -m "Apache SpatialBench ${SEDONA_VERSION} RC${RC_NUMBER}"
cd ..
rm -rf tmp
```

## Vote

An email must now be sent to `dev@sedona.apache.org` calling on developers to follow
the release verification instructions and vote appropriately on the source release.

## Bump versions

After a successful release, versions on the `main` branch need to be updated. These
are currently all derived from `Cargo.toml`, which can be updated to:

```
[workspace.package]
version = "0.2.0"
```

## Publishing to crates.io

After a successful Apache release, the Rust crates can be published to [crates.io](https://crates.io).

### Prerequisites

1. **crates.io account**: Create an account at <https://crates.io> if you don't have one
2. **API token**: Generate an API token from <https://crates.io/me>
3. **Login to cargo**: Authenticate cargo with your API token:
   ```shell
   cargo login <your-api-token>
   ```
4. **Verify ownership**: Ensure you have owner permissions for the crates on crates.io. If this is the first publish, you'll automatically become an owner.

### Pre-publish checks

Before publishing, verify that:

1. All tests pass:
   ```shell
   cargo test --workspace
   ```

2. The workspace builds successfully:
   ```shell
   cargo build --workspace --release
   ```

3. Check for any issues with `dry run`:
   ```shell
   cargo publish --dry-run
   ```

### Publishing order

The crates will be published in dependency order. The correct order is:

1. `spatialbench` (no workspace dependencies)
2. `spatialbench-arrow` (depends on `spatialbench`)
3. `spatialbench-cli` (depends on both `spatialbench` and `spatialbench-arrow`)

To publish, run this command:

```shell
cargo publish
```
