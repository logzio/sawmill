# Release process

Sawmill' release process is automated as a Travis deployment. This describes the basic steps for a project member to perform a release.

## Steps

1. Ensure that the master branch is building and that tests are passing.
1. Ensure that the [`CHANGELOG`](CHANGELOG.md) file is up to date and includes all merged features.
1. Create a new release on GitHub. **The tag name is used as the version**, so please keep the tag name plain (e.g. 1.2.3).
1. Check that the Travis build passed.
1. Release of published artifacts is fully automated. Once the Travis build completes, there are no further actions to perform on the repository.

## Internal details

* The signing and publishing steps are initiated as a script-type Travis CI deployment phase.
* mvn -Drevision=$TRAVIS_TAG is applied to use the tag name to the version number for the build.
* An encrypted GPG key within the release directory is used for signing. This key file is decrypted using Travis secrets.
* Travis secrets also hold GPG and Maven Central API username/passwords that are used for publishing.
* The publication process is automated using nexus-staging-maven-plugin.
* The process is done with adle and Bintray.
* Travis service will automatically promote the release to Maven Central.
