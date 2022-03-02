# Release process

Sawmill' release process is automated as a GitHub Actions deployment. This describes the basic steps for a project admin to perform a release.

## Steps

1. Ensure that the master branch is building and that tests are passing.
1. Create a new release on GitHub. **The tag name is used as the version**, so please keep the tag name plain (e.g. 1.2.3).
1. Check that the GitHub Actions build passed.
1. Release of published artifacts is fully automated. Once the GitHub Actions build completes, there are no further actions to perform on the repository.

## Internal details

* The signing and publishing to Maven Central steps are initiated as a script-type GitHub Actions "Deploy" step, upon a new git tag. The tag is created when a release is created via GitHub.
* mvn versions:set -DnewVersion=${GITHUB_REF#refs/tags/} is applied to use the tag name to the version number for the build.
* An encrypted GPG key within the release directory is used for signing. This key file is decrypted using GitHub secrets.
* GitHub secrets also hold GPG and Maven Central API username/passwords that are used for publishing.
* The publish/deploy to Maven Central is done via nexus-staging-maven-plugin.
* GitHub Actions service will automatically promote the release to Maven Central.
